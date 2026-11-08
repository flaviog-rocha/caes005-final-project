# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Gerenciador de filas de tarefas por entidade, usando apenas a biblioteca padrão.

  Características:

    * Erros de validação retornam `{:error, {:validation, motivos}}` e nunca são
      reprocessados. Falhas operacionais retornam `{:error, {:operational, razao}}`
      e são re-tentadas até `max_attempts`.
    * Cada entidade possui seu próprio processo (GenServer) supervisionado
      dinamicamente, então entidades diferentes são processadas de forma independente.
    * `status/1` e `list_operations/0` consultam o estado atual das operações.
    * Notificações são publicadas via `Registry` (pub/sub local) para os
      assinantes registrados com `subscribe/1`. Formato da mensagem recebida:
      `{:task_queue, entidade, evento}`.

  Eventos de notificação:

    * `{:queue_threshold_reached, limite}` - fila atingiu o limite configurado.
    * `{:task_retry_scheduled, id, tentativa}` - tarefa agendada para nova tentativa.
    * `{:task_completed, id}` - tarefa concluída.
    * `{:task_failed, id, tipo, motivo}` - tarefa falhou definitivamente (`tipo` é
      `:validation` ou `:operational`).
    * `{:persistence_failed, id, motivo}` - falha ao persistir no banco fictício.
    * `:queue_drained` - fila da entidade ficou vazia.

  Exemplo de uso:

      {:ok, _} = TaskQueue.start_link(failure_rate: 0.1)
      TaskQueue.subscribe(:all)

      handler = fn %{"type" => tipo} -> {:ok, {:processado, tipo}} end
      {:ok, _id} = TaskQueue.enqueue("pedidos", %{"type" => "envio"}, handler: handler)

      {:error, {:validation, _}} = TaskQueue.enqueue("pedidos", %{})

      {:ok, snapshot} = TaskQueue.status("pedidos")
      TaskQueue.list_operations()
  """

  alias TaskQueue.{Entity, Storage, Validation}

  @doc false
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end

  @doc """
  Inicia a árvore de supervisão: registries, banco fictício e supervisor de entidades.

  Opções:
    * `:failure_rate` - probabilidade (0.0 a 1.0) de o banco fictício falhar em cada escrita.
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(
      [
        {Registry, keys: :unique, name: TaskQueue.EntityRegistry},
        {Registry, keys: :duplicate, name: TaskQueue.PubSub},
        {Storage, opts},
        {DynamicSupervisor, strategy: :one_for_one, name: TaskQueue.EntitySupervisor}
      ],
      strategy: :one_for_one,
      name: TaskQueue.Root
    )
  end

  @doc """
  Enfileira uma tarefa para a entidade.

  Opções (aplicadas somente na primeira vez que a entidade é criada):
    * `:handler` - função `payload -> {:ok, resultado} | {:error, motivo}`.
      Retornar `{:error, {:validation, motivos}}` marca a tarefa como inválida (sem retry).
    * `:max_attempts` - número máximo de tentativas para falhas operacionais (padrão 3).
    * `:threshold` - tamanho de fila que dispara notificação (padrão 100).
    * `:retry_delay` - intervalo em ms entre tentativas (padrão 50).

  Retornos:
    * `{:ok, id}`
    * `{:error, {:validation, motivos}}`
    * `{:error, {:operational, razao}}`
  """
  def enqueue(entity, payload, opts \\ []) do
    with :ok <- Validation.validate(entity, payload),
         :ok <- ensure_entity(entity, opts) do
      call(Entity.via(entity), {:enqueue, payload})
    end
  end

  @doc "Consulta o estado atual de uma entidade."
  def status(entity) do
    case Registry.lookup(TaskQueue.EntityRegistry, entity) do
      [{pid, _}] -> snapshot(pid)
      [] -> {:error, :not_found}
    end
  end

  @doc "Lista o estado atual de todas as entidades ativas."
  def list_operations do
    TaskQueue.EntityRegistry
    |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
    |> Enum.flat_map(fn entity ->
      case status(entity) do
        {:ok, snap} -> [snap]
        _ -> []
      end
    end)
  end

  @doc "Retorna as tarefas gravadas no banco fictício para a entidade."
  def persisted_tasks(entity), do: Storage.list(entity)

  @doc "Assina notificações: `:all` para todas ou o nome de uma entidade."
  def subscribe(scope \\ :all) do
    Registry.register(TaskQueue.PubSub, scope_key(scope), nil)
  end

  @doc "Cancela a assinatura de notificações."
  def unsubscribe(scope \\ :all) do
    Registry.unregister(TaskQueue.PubSub, scope_key(scope))
  end

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  defp scope_key(:all), do: :all
  defp scope_key(entity), do: {:entity, entity}

  defp ensure_entity(entity, opts) do
    spec = {Entity, [{:entity, entity} | opts]}

    case DynamicSupervisor.start_child(TaskQueue.EntitySupervisor, spec) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  defp snapshot(pid) do
    case call(pid, :status) do
      {:error, _} = error -> error
      snap -> {:ok, snap}
    end
  end

  # Converte saídas de processo (entidade morta, timeout) em erro operacional.
  defp call(target, msg) do
    GenServer.call(target, msg)
  catch
    :exit, _ -> {:error, {:operational, :entity_unavailable}}
  end
end

defmodule TaskQueue.Validation do
  @moduledoc """
  Validações de entrada. Erros retornados aqui são sempre de validação (não são re-tentados).
  """

  @doc "Valida entidade e payload. Retorna `:ok` ou `{:error, {:validation, motivos}}`."
  def validate(entity, payload) do
    errors =
      []
      |> check(valid_entity?(entity), {:entity, "deve ser átomo ou string não vazia"})
      |> check(is_map(payload), {:payload, "deve ser um mapa"})
      |> check(not is_map(payload) or valid_type?(payload), {:type, "deve ser string não vazia"})

    case errors do
      [] -> :ok
      _ -> {:error, {:validation, errors}}
    end
  end

  defp check(errors, true, _error), do: errors
  defp check(errors, false, error), do: errors ++ [error]

  defp valid_entity?(e) when is_binary(e), do: e != ""
  defp valid_entity?(e) when is_atom(e), do: e not in [nil, true, false]
  defp valid_entity?(_), do: false

  defp valid_type?(payload) do
    case Map.get(payload, "type") do
      t when is_binary(t) -> String.trim(t) != ""
      _ -> false
    end
  end
end

defmodule TaskQueue.Storage do
  @moduledoc """
  Banco de dados fictício baseado em `Agent`.

  Simula indisponibilidade de escrita via `:failure_rate`, permitindo testar
  o caminho de falhas operacionais.

  Equivalente opcional em Ecto.Migration (não utilizado em tempo de execução):

      # create table(:task_queue_tasks, primary_key: false) do
      #   add :entity, :string, null: false, primary_key: true
      #   add :id, :bigint, null: false, primary_key: true
      #   add :payload, :map, null: false
      #   add :status, :string, null: false
      #   add :attempts, :integer, null: false, default: 0
      #   add :result, :map
      #   add :last_error, :text
      #   add :inserted_at, :utc_datetime_usec, null: false
      # end
      # create index(:task_queue_tasks, [:entity, :status])
  """

  use Agent

  def start_link(opts) do
    failure_rate = Keyword.get(opts, :failure_rate, 0.0)

    Agent.start_link(
      fn -> %{rows: %{}, failure_rate: failure_rate} end,
      name: __MODULE__
    )
  end

  @doc "Grava (ou atualiza) uma tarefa. Retorna `:ok` ou `{:error, :db_unavailable}`."
  def save(task) do
    Agent.get_and_update(__MODULE__, fn %{rows: rows, failure_rate: rate} = state ->
      if :rand.uniform() < rate do
        {{:error, :db_unavailable}, state}
      else
        {:ok, %{state | rows: Map.put(rows, {task.entity, task.id}, task)}}
      end
    end)
  end

  @doc "Lista as tarefas persistidas de uma entidade."
  def list(entity) do
    Agent.get(__MODULE__, fn %{rows: rows} ->
      for {{e, _id}, task} <- rows, e == entity, do: task
    end)
  end
end

defmodule TaskQueue.Entity do
  @moduledoc """
  Processo por entidade. Mantém a fila, processa uma tarefa por vez (em processo
  auxiliar monitorado), aplica retries e publica notificações.
  """

  use GenServer, restart: :transient

  alias TaskQueue.Storage

  @doc "Nome de registro via `Registry` para a entidade."
  def via(entity), do: {:via, Registry, {TaskQueue.EntityRegistry, entity}}

  def start_link(opts) do
    entity = Keyword.fetch!(opts, :entity)
    GenServer.start_link(__MODULE__, opts, name: via(entity))
  end

  @impl true
  def init(opts) do
    state = %{
      entity: Keyword.fetch!(opts, :entity),
      handler: Keyword.get(opts, :handler, fn _payload -> {:ok, :processed} end),
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      threshold: Keyword.get(opts, :threshold, 100),
      retry_delay: Keyword.get(opts, :retry_delay, 50),
      queue: :queue.new(),
      size: 0,
      current: nil,
      completed: 0,
      failed: 0,
      unsynced: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, payload}, _from, s) do
    task = new_task(s.entity, payload)

    case Storage.save(task) do
      :ok ->
        s = %{s | queue: :queue.in(task, s.queue), size: s.size + 1}

        s =
          if s.size == s.threshold,
            do: announce(s, {:queue_threshold_reached, s.threshold}),
            else: s

        send(self(), :process_next)
        {:reply, {:ok, task.id}, s}

      {:error, reason} ->
        {:reply, {:error, {:operational, reason}}, s}
    end
  end

  def handle_call(:status, _from, s) do
    in_progress =
      case s.current do
        nil ->
          nil

        %{task: task, started_at: started_at} ->
          %{
            id: task.id,
            type: task.payload["type"],
            attempts: task.attempts,
            running_for_ms: System.monotonic_time(:millisecond) - started_at
          }
      end

    snapshot = %{
      entity: s.entity,
      state: if(s.current, do: :processing, else: :idle),
      in_progress: in_progress,
      pending: s.size,
      completed: s.completed,
      failed: s.failed,
      unsynced: s.unsynced
    }

    {:reply, snapshot, s}
  end

  @impl true
  def handle_info(:process_next, %{current: nil} = s) do
    case :queue.out(s.queue) do
      {:empty, _} ->
        {:noreply, s}

      {{:value, task}, rest} ->
        parent = self()
        handler = s.handler
        payload = task.payload
        id = task.id

        # O trabalho roda em processo separado: a entidade continua respondendo consultas
        # e, se o worker crashar, recebemos :DOWN e tratamos como falha operacional.
        {pid, ref} =
          spawn_monitor(fn ->
            send(parent, {:work_result, id, run(handler, payload)})
          end)

        current = %{
          task: %{task | status: :processing},
          pid: pid,
          ref: ref,
          started_at: System.monotonic_time(:millisecond)
        }

        {:noreply, %{s | queue: rest, size: s.size - 1, current: current}}
    end
  end

  def handle_info(:process_next, s), do: {:noreply, s}

  def handle_info({:work_result, id, result}, %{current: %{task: %{id: id}, ref: ref}} = s) do
    Process.demonitor(ref, [:flush])
    {:noreply, complete(s, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{current: %{ref: ref}} = s) do
    {:noreply, complete(s, {:error, {:operational, {:worker_crashed, reason}}})}
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # ---------------------------------------------------------------------------
  # Execução e conclusão
  # ---------------------------------------------------------------------------

  defp run(handler, payload) do
    case handler.(payload) do
      {:ok, _} = ok -> ok
      {:error, {:validation, _}} = invalid -> invalid
      {:error, {:operational, _}} = op -> op
      {:error, reason} -> {:error, {:operational, reason}}
      other -> {:error, {:operational, {:invalid_handler_return, other}}}
    end
  rescue
    e -> {:error, {:operational, {:exception, Exception.message(e)}}}
  end

  defp complete(%{current: %{task: task}} = s, result) do
    s = %{s | current: nil}

    case result do
      {:ok, value} ->
        task = %{task | status: :completed, result: value}

        s
        |> persist(task)
        |> Map.update!(:completed, &(&1 + 1))
        |> announce({:task_completed, task.id})
        |> after_task()

      # Erro de validação: não há motivo para re-tentar.
      {:error, {:validation, reasons}} ->
        fail(s, %{task | status: :failed, last_error: reasons}, :validation, reasons)

      # Falha operacional: re-tenta até o limite.
      {:error, {:operational, reason}} ->
        attempts = task.attempts + 1
        task = %{task | attempts: attempts, last_error: reason}

        if attempts < s.max_attempts do
          retry(s, task, attempts)
        else
          fail(s, %{task | status: :failed}, :operational, reason)
        end
    end
  end

  defp retry(s, task, attempts) do
    task = %{task | status: :pending}

    s =
      s
      |> persist(task)
      |> then(fn st -> %{st | queue: :queue.in_r(task, st.queue), size: st.size + 1} end)
      |> announce({:task_retry_scheduled, task.id, attempts})

    Process.send_after(self(), :process_next, s.retry_delay)
    s
  end

  defp fail(s, task, kind, reason) do
    s
    |> persist(task)
    |> Map.update!(:failed, &(&1 + 1))
    |> announce({:task_failed, task.id, kind, reason})
    |> after_task()
  end

  defp after_task(s) do
    s = if s.size == 0, do: announce(s, :queue_drained), else: s
    send(self(), :process_next)
    s
  end

  # Persistência: falha no banco não interrompe o processamento, mas é contabilizada e notificada.
  defp persist(s, task) do
    case Storage.save(task) do
      :ok ->
        s

      {:error, reason} ->
        s = %{s | unsynced: s.unsynced + 1}
        announce(s, {:persistence_failed, task.id, reason})
    end
  end

  # ---------------------------------------------------------------------------
  # Notificações e helpers
  # ---------------------------------------------------------------------------

  defp announce(s, event) do
    notify(s.entity, event)
    s
  end

  defp notify(entity, event) do
    for key <- [:all, {:entity, entity}] do
      Registry.dispatch(TaskQueue.PubSub, key, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:task_queue, entity, event})
      end)
    end

    :ok
  end

  defp new_task(entity, payload) do
    %{
      id: System.unique_integer([:positive, :monotonic]),
      entity: entity,
      payload: payload,
      status: :pending,
      attempts: 0,
      result: nil,
      last_error: nil,
      inserted_at: DateTime.utc_now()
    }
  end
end
