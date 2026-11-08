# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir comunicação entre diferentes processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de fila de tarefas com OTP.
#
# Árvore de supervisão:
#   TaskQueue.Root (Supervisor, :one_for_all)
#   ├── TaskQueue.WorkerSupervisor (DynamicSupervisor, um worker por operação)
#   └── TaskQueue.Manager (GenServer: fila, histórico, idempotência, pub/sub)
#
# Exemplo de uso (em comentário, para referência):
#   {:ok, _} = TaskQueue.start_link(max_concurrency: 2, timeout: 2_000)
#   TaskQueue.subscribe()
#   {:ok, "op-1"} = TaskQueue.enqueue("op-1", %{command: :ok_job})
#   {:error, {:validation, :duplicate}} = TaskQueue.enqueue("op-1", %{command: :ok_job})
#   {:error, {:validation, :missing_command}} = TaskQueue.enqueue("op-2", %{})
#   TaskQueue.status("op-1")
#   TaskQueue.in_progress()
#   TaskQueue.history()
#   # mensagens recebidas: {:task_queue, %{at: ..., event: :succeeded, id: "op-1", detail: nil}}

defmodule TaskQueue do
  # Ponto de entrada da API pública e supervisor raiz.

  @root TaskQueue.Root
  @manager TaskQueue.Manager
  @worker_sup TaskQueue.WorkerSupervisor

  @doc """
  Inicia a árvore de supervisão da fila.

  Opções:
    * `:executor` - função de aridade 1 que executa a operação. Deve retornar
      `{:ok, valor}` ou `{:error, motivo}`.
    * `:validator` - função de aridade 1 que valida o payload. Deve retornar
      `:ok` ou `{:error, motivo}`.
    * `:max_concurrency` - número máximo de operações executando simultaneamente.
    * `:timeout` - tempo máximo (ms) de execução de cada operação.
    * `:max_history` - quantidade máxima de eventos mantidos no histórico.
  """
  def start_link(opts \\ []) do
    children = [
      # DynamicSupervisor: cria um processo worker dinamicamente para cada operação.
      {DynamicSupervisor, strategy: :one_for_one, name: @worker_sup},
      {TaskQueue.Manager, opts}
    ]

    # :one_for_all garante que, se o Manager cair, os workers em andamento
    # também sejam encerrados, evitando execuções órfãs não rastreadas.
    Supervisor.start_link(children, strategy: :one_for_all, name: @root)
  end

  @doc "Enfileira uma operação identificada por `id`. Operações com o mesmo `id` não são reexecutadas."
  def enqueue(id, payload) do
    GenServer.call(@manager, {:enqueue, id, payload})
  end

  @doc "Consulta o estado de uma operação pelo `id`."
  def status(id) do
    GenServer.call(@manager, {:status, id})
  end

  @doc "Lista as operações pendentes ou em execução, da mais antiga para a mais recente."
  def in_progress do
    GenServer.call(@manager, :in_progress)
  end

  @doc "Retorna o histórico de eventos em ordem cronológica. Aceita `:all` ou um limite inteiro (últimos N eventos)."
  def history(limit \\ :all) do
    GenServer.call(@manager, {:history, limit})
  end

  @doc """
  Inscreve o processo chamador para receber eventos em tempo real.
  Os eventos chegam como `{:task_queue, evento}` na caixa de mensagens.
  """
  def subscribe do
    GenServer.call(@manager, {:subscribe, self()})
  end
end

defmodule TaskQueue.Manager do
  # GenServer central: mantém a fila FIFO, o registro de operações (base da
  # idempotência), o histórico e a lista de assinantes.

  use GenServer

  @name TaskQueue.Manager
  @worker_sup TaskQueue.WorkerSupervisor
  @default_max_concurrency 4
  @default_timeout 5_000
  @default_max_history 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  # Validador padrão: o payload deve ser um mapa contendo a chave :command.
  def default_validator(payload) when is_map(payload) do
    if Map.has_key?(payload, :command), do: :ok, else: {:error, :missing_command}
  end

  def default_validator(_payload), do: {:error, :payload_must_be_map}

  # Executor padrão usado em demonstrações.
  def default_executor(%{command: :fail}), do: {:error, :simulated_failure}
  def default_executor(%{command: command}), do: {:ok, {:executed, command}}

  @impl true
  def init(opts) do
    state = %{
      # id => registro da operação. Como nunca é removido, serve de barreira de idempotência.
      ops: %{},
      # Fila FIFO de ids aguardando slot de execução.
      pending: :queue.new(),
      # id => referência de monitoramento do worker em execução.
      running: %{},
      # Eventos, do mais recente para o mais antigo.
      history: [],
      # monitor_ref => pid dos assinantes.
      subscribers: %{},
      executor: Keyword.get(opts, :executor, &TaskQueue.Manager.default_executor/1),
      validator: Keyword.get(opts, :validator, &TaskQueue.Manager.default_validator/1),
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      timeout: Keyword.get(opts, :timeout, @default_timeout),
      max_history: Keyword.get(opts, :max_history, @default_max_history)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, id, payload}, _from, state) do
    case validate(id, payload, state) do
      :ok ->
        op = %{
          id: id,
          payload: payload,
          status: :pending,
          result: nil,
          error: nil,
          enqueued_at: now(),
          started_at: nil,
          finished_at: nil
        }

        state =
          %{state | ops: Map.put(state.ops, id, op), pending: :queue.in(id, state.pending)}
          |> record(:enqueued, id, nil)
          |> dispatch()

        {:reply, {:ok, id}, state}

      {:error, reason} = error ->
        # Erro de validação: a operação nem entra no registro de operações.
        {:reply, error, record(state, :rejected, id, reason)}
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.ops, id) do
        {:ok, op} -> {:ok, op}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    ops =
      state.ops
      |> Map.values()
      |> Enum.filter(&(&1.status in [:pending, :running]))
      |> Enum.sort_by(& &1.enqueued_at, DateTime)

    {:reply, ops, state}
  end

  def handle_call({:history, limit}, _from, state) do
    events =
      if limit == :all do
        Enum.reverse(state.history)
      else
        state.history |> Enum.take(limit) |> Enum.reverse()
      end

    {:reply, events, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  @impl true
  def handle_cast({:finished, id, outcome}, state) do
    case Map.fetch(state.running, id) do
      {:ok, ref} ->
        # Remove o monitor antes de tratar o término para não contar como crash.
        Process.demonitor(ref, [:flush])
        {:noreply, finish(state, id, outcome)}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  # Assinante caiu: remove da lista de inscritos.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{subscribers: subs} = state)
      when is_map_key(subs, ref) do
    {:noreply, %{state | subscribers: Map.delete(subs, ref)}}
  end

  # Worker caiu antes de reportar resultado: falha operacional.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_id, r} -> r == ref end) do
      {id, _} ->
        outcome = {:error, {:operational, {:worker_crash, reason}}}
        {:noreply, finish(state, id, outcome)}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Validação: id válido, não duplicado e payload válido.
  # Todos os retornos de erro aqui são {:error, {:validation, _}}.
  defp validate(id, payload, state) do
    cond do
      not (is_binary(id) and id != "") ->
        {:error, {:validation, :invalid_id}}

      Map.has_key?(state.ops, id) ->
        {:error, {:validation, :duplicate}}

      true ->
        case state.validator.(payload) do
          :ok -> :ok
          {:error, reason} -> {:error, {:validation, reason}}
        end
    end
  end

  # Inicia workers enquanto houver capacidade e itens na fila.
  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency and not :queue.is_empty(state.pending) do
      {{:value, id}, pending} = :queue.out(state.pending)
      state = %{state | pending: pending}
      op = Map.fetch!(state.ops, id)
      args = {id, op.payload, state.executor, self(), state.timeout}

      case DynamicSupervisor.start_child(@worker_sup, {TaskQueue.Worker, args}) do
        {:ok, pid} ->
          ref = Process.monitor(pid)

          state =
            %{state | running: Map.put(state.running, id, ref)}
            |> update_op(id, %{status: :running, started_at: now()})
            |> record(:started, id, nil)

          dispatch(state)

        {:error, reason} ->
          # Falha ao criar o worker é falha operacional, não de validação.
          finish(state, id, {:error, {:operational, {:spawn_failed, reason}}})
      end
    else
      state
    end
  end

  # Finaliza uma operação, registra o resultado e libera o slot.
  defp finish(state, id, outcome) do
    {status, fields} =
      case outcome do
        {:ok, value} -> {:succeeded, %{result: value, error: nil}}
        {:error, reason} -> {:failed, %{result: nil, error: reason}}
      end

    state = update_op(state, id, Map.merge(fields, %{status: status, finished_at: now()}))
    state = %{state | running: Map.delete(state.running, id)}

    state
    |> record(status, id, fields.error)
    |> dispatch()
  end

  defp update_op(state, id, fields) do
    %{state | ops: Map.update!(state.ops, id, &Map.merge(&1, fields))}
  end

  # Registra o evento no histórico e notifica os assinantes (comunicação entre processos).
  defp record(state, event, id, detail) do
    entry = %{at: now(), event: event, id: id, detail: detail}
    history = Enum.take([entry | state.history], state.max_history)

    Enum.each(state.subscribers, fn {_ref, pid} ->
      send(pid, {:task_queue, entry})
    end)

    %{state | history: history}
  end

  defp now, do: DateTime.utc_now()
end

defmodule TaskQueue.Worker do
  # Processo temporário que executa uma única operação.
  # restart: :temporary impede que uma falha reexecute a operação,
  # preservando a garantia de execução única.

  use GenServer, restart: :temporary

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    # A execução começa fora do init/1 para não bloquear o supervisor.
    {:ok, args, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, {id, payload, executor, manager, timeout} = state) do
    outcome = run(executor, payload, timeout)
    GenServer.cast(manager, {:finished, id, outcome})
    {:stop, :normal, state}
  end

  # Executa a operação com timeout. Falhas aqui são operacionais.
  defp run(executor, payload, timeout) do
    task = Task.async(fn -> safe_execute(executor, payload) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, value}} -> {:ok, value}
      {:ok, {:error, reason}} -> {:error, {:operational, reason}}
      {:ok, other} -> {:error, {:operational, {:invalid_return, other}}}
      {:exit, reason} -> {:error, {:operational, {:exit, reason}}}
      nil -> {:error, {:operational, :timeout}}
    end
  end

  # Captura exceções e throws para que não derrubem o worker sem registro.
  defp safe_execute(executor, payload) do
    try do
      executor.(payload)
    rescue
      e -> {:error, {:exception, Exception.message(e)}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end
end
