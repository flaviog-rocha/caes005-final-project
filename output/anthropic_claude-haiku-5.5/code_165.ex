# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Fazer uso de macros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir comunicação entre diferentes processos.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# ============================================================================
# Gerenciador de fila de tarefas em Elixir (arquivo único, autocontido)
#
# Execução: elixir task_queue.exs
#
# Requisitos atendidos:
#   1. Consulta do estado das operações (TaskQueue.Server.status/0)
#   2. Uso de macros (TaskQueue.DSL: defjob/2 e defjob/3)
#   3. Execução periódica de jobs (Process.send_after/3 agendando jobs)
#   4. Comunicação entre processos (chamadas síncronas, assinantes de eventos
#      e mensagens enviadas a partir dos workers)
#   5. Banco de dados fictício (TaskQueue.FakeDB via Agent). Ecto é opcional e
#      não é necessário; a equivalência de migration está descrita em comentário.
# ============================================================================

# ----------------------------------------------------------------------------
# DSL baseada em macros para declarar jobs
# ----------------------------------------------------------------------------
defmodule TaskQueue.DSL do
  @moduledoc """
  Macros para declarar jobs de forma declarativa.

  Uso:

      defmodule MeusJobs do
        use TaskQueue.DSL

        defjob :enviar_email, every: 10_000 do
          # corpo do job; a variável `payload` está disponível
          {:ok, payload}
        end
      end

  Quando `every:` é informado, o job é registrado como periódico e
  `TaskQueue.Server` o enfileirará automaticamente nesse intervalo.
  """

  defmacro __using__(_opts) do
    quote do
      import TaskQueue.DSL
      # Acumula metadados dos jobs declarados neste módulo
      Module.register_attribute(__MODULE__, :jobs, accumulate: true)
      @before_compile TaskQueue.DSL
    end
  end

  # Gera a função `__jobs__/0`, que lista os jobs periódicos {nome, intervalo}
  defmacro __before_compile__(_env) do
    quote do
      def __jobs__, do: @jobs |> Enum.reverse()
    end
  end

  # Declaração sem opções
  defmacro defjob(name, do: block), do: build_job(name, [], block)

  # Declaração com opções (ex.: every: 2_000)
  defmacro defjob(name, opts, do: block), do: build_job(name, opts, block)

  defp build_job(name, opts, block) do
    every = Keyword.get(opts, :every)

    # Variável `payload` com contexto nil para que o bloco do chamador a enxergue
    # (evita problemas de higiene de variáveis em macros)
    arg = Macro.var(:payload, nil)

    registration =
      if every do
        quote do
          @jobs {unquote(name), unquote(every)}
        end
      end

    quote do
      unquote(registration)

      def unquote(name)(unquote(arg)) do
        # Evita warning de variável não utilizada quando o job ignora o payload
        _ = unquote(arg)
        unquote(block)
      end
    end
  end
end

# ----------------------------------------------------------------------------
# Banco de dados fictício (sem Ecto)
#
# Equivalente a uma migration Ecto (opcional):
#
#   create table(:tasks, primary_key: false) do
#     add :id, :bigint, primary_key: true
#     add :name, :string, null: false
#     add :payload, :map
#     add :status, :string, null: false
#     add :result, :map
#     add :enqueued_at, :bigint
#     add :started_at, :bigint
#     add :finished_at, :bigint
#   end
# ----------------------------------------------------------------------------
defmodule TaskQueue.FakeDB do
  @moduledoc "Persistência em memória simulando uma tabela `tasks`."
  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def save(task), do: Agent.update(__MODULE__, &Map.put(&1, task.id, task))

  def fetch(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  def all, do: Agent.get(__MODULE__, &Map.values/1)
end

# ----------------------------------------------------------------------------
# Definição dos jobs da aplicação
# ----------------------------------------------------------------------------
defmodule TaskQueue.Jobs do
  @moduledoc "Jobs disponíveis para execução pela fila."
  use TaskQueue.DSL

  # Job periódico: a cada 2 segundos
  defjob :heartbeat, every: 2_000 do
    %{alive: true, at: System.system_time(:millisecond)}
  end

  # Job periódico: a cada 5 segundos, consulta o banco fictício
  defjob :report, every: 5_000 do
    {:report, length(TaskQueue.FakeDB.all())}
  end

  # Job sob demanda: simula processamento com atraso configurável
  defjob :process_order do
    delay = if is_map(payload), do: Map.get(payload, :delay, 0), else: 0
    Process.sleep(delay)
    {:order_processed, payload}
  end
end

# ----------------------------------------------------------------------------
# Servidor da fila
# ----------------------------------------------------------------------------
defmodule TaskQueue.Server do
  @moduledoc """
  GenServer que controla a fila de tarefas.

  - Enfileira tarefas (manuais ou periódicas)
  - Executa no máximo `max_concurrency` tarefas simultaneamente
  - Notifica assinantes sobre eventos (`{:task_queue, evento}`)
  - Persiste cada mudança de estado no banco fictício
  """
  use GenServer

  @default_concurrency 3

  ## ------------------------------------------------------------------
  ## API pública
  ## ------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Enfileira uma tarefa. Pode ser chamado de qualquer processo."
  def enqueue(name, payload \\ nil), do: GenServer.call(__MODULE__, {:enqueue, name, payload})

  @doc "Retorna o estado atual das operações (pendentes, em execução e contadores)."
  def status, do: GenServer.call(__MODULE__, :status)

  @doc "Registra um processo para receber eventos da fila."
  def subscribe(pid \\ self()), do: GenServer.call(__MODULE__, {:subscribe, pid})

  ## ------------------------------------------------------------------
  ## Callbacks do GenServer
  ## ------------------------------------------------------------------

  @impl true
  def init(opts) do
    job_module = Keyword.get(opts, :job_module, TaskQueue.Jobs)

    state = %{
      job_module: job_module,
      queue: :queue.new(),
      # pid_do_worker => {tarefa, referência_do_monitor}
      running: %{},
      # pid_do_assinante => referência_do_monitor
      subscribers: %{},
      max: Keyword.get(opts, :max_concurrency, @default_concurrency),
      done: 0,
      failed: 0
    }

    # Agenda os jobs periódicos declarados via macro
    Enum.each(job_module.__jobs__(), fn {name, every} ->
      Process.send_after(self(), {:periodic, name, every}, every)
    end)

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, name, payload}, _from, state) do
    {id, state} = add_task(state, name, payload)
    {:reply, {:ok, id}, dispatch(state)}
  end

  def handle_call(:status, _from, state) do
    pending =
      for task <- :queue.to_list(state.queue),
          do: %{id: task.id, name: task.name, enqueued_at: task.enqueued_at}

    running =
      for {_pid, {task, _ref}} <- state.running,
          do: %{id: task.id, name: task.name, started_at: task.started_at}

    reply = %{
      pending: pending,
      running: running,
      done: state.done,
      failed: state.failed,
      concurrency: state.max
    }

    {:reply, reply, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      # Monitora o assinante para remoção automática se ele morrer
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, ref)}}
    end
  end

  @impl true
  def handle_info({:periodic, name, every}, state) do
    # Reagenda a próxima execução e enfileira a tarefa periódica
    Process.send_after(self(), {:periodic, name, every}, every)
    {_id, state} = add_task(state, name, %{scheduled_at: now()})
    {:noreply, dispatch(state)}
  end

  # Mensagem enviada pelo worker ao concluir a execução
  def handle_info({:task_result, pid, result}, state) do
    {:noreply, complete(state, pid, result)}
  end

  # Monitor disparado: assinante encerrou ou worker terminou de forma abrupta
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.subscribers, pid) do
      ^ref ->
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}

      _ ->
        {:noreply, complete(state, pid, {:error, {:exit, reason}})}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## ------------------------------------------------------------------
  ## Funções privadas
  ## ------------------------------------------------------------------

  defp add_task(state, name, payload) do
    task = %{
      id: System.unique_integer([:positive, :monotonic]),
      name: name,
      payload: payload,
      status: :pending,
      enqueued_at: now(),
      started_at: nil,
      finished_at: nil,
      result: nil
    }

    TaskQueue.FakeDB.save(task)
    state = %{state | queue: :queue.in(task, state.queue)}
    {task.id, broadcast(state, {:enqueued, task.id, name})}
  end

  # Inicia tarefas enquanto houver capacidade e itens na fila
  defp dispatch(state) do
    cond do
      map_size(state.running) >= state.max -> state
      :queue.is_empty(state.queue) -> state
      true -> start_next(state)
    end
  end

  defp start_next(state) do
    {{:value, task}, rest} = :queue.out(state.queue)
    task = %{task | status: :running, started_at: now()}
    server = self()
    job_module = state.job_module

    # Worker isolado: uma falha não derruba o servidor da fila
    {pid, ref} =
      spawn_monitor(fn ->
        send(server, {:task_result, self(), run(job_module, task)})
      end)

    TaskQueue.FakeDB.save(task)

    state =
      %{state | queue: rest, running: Map.put(state.running, pid, {task, ref})}
      |> broadcast({:started, task.id, task.name})

    dispatch(state)
  end

  # Executa o job capturando erros e exits
  defp run(job_module, task) do
    {:ok, apply(job_module, task.name, [task.payload])}
  rescue
    e -> {:error, Exception.message(e)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp complete(state, pid, outcome) do
    case Map.pop(state.running, pid) do
      {nil, _} ->
        state

      {{task, ref}, running} ->
        # Remove o monitor para não receber o DOWN posteriormente
        Process.demonitor(ref, [:flush])

        {status, result} =
          case outcome do
            {:ok, value} -> {:done, value}
            {:error, reason} -> {:failed, reason}
          end

        task = %{task | status: status, result: result, finished_at: now()}
        TaskQueue.FakeDB.save(task)

        state =
          %{state | running: running}
          |> bump(status)
          |> broadcast({status, task.id, task.name})

        dispatch(state)
    end
  end

  defp bump(state, :done), do: %{state | done: state.done + 1}
  defp bump(state, :failed), do: %{state | failed: state.failed + 1}

  defp broadcast(state, event) do
    Enum.each(Map.keys(state.subscribers), &send(&1, {:task_queue, event}))
    state
  end

  defp now, do: System.system_time(:millisecond)
end

# ----------------------------------------------------------------------------
# Supervisão da aplicação
# ----------------------------------------------------------------------------
defmodule TaskQueue.Root do
  @moduledoc "Supervisor raiz: banco fictício e servidor da fila."
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {TaskQueue.FakeDB, []},
      {TaskQueue.Server, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ----------------------------------------------------------------------------
# Demonstração de uso
# ----------------------------------------------------------------------------
defmodule TaskQueue.Demo do
  @moduledoc "Executa um cenário de exemplo com consultas de status e eventos."

  def run do
    {:ok, _} = TaskQueue.Root.start_link()

    # Processo independente que recebe os eventos da fila
    listener = spawn(fn -> listen() end)
    :ok = TaskQueue.Server.subscribe(listener)

    # Enfileira tarefas sob demanda
    Enum.each(1..5, fn i ->
      {:ok, _id} = TaskQueue.Server.enqueue(:process_order, %{id: i, delay: 300})
    end)

    Process.sleep(500)
    IO.inspect(TaskQueue.Server.status(), label: "status (em andamento)")

    # Aguarda execuções periódicas (heartbeat e report)
    Process.sleep(4_000)
    IO.inspect(TaskQueue.Server.status(), label: "status (final)")
    IO.inspect(length(TaskQueue.FakeDB.all()), label: "registros no FakeDB")
  end

  defp listen do
    receive do
      {:task_queue, event} -> IO.inspect(event, label: "evento")
    end

    listen()
  end
end

TaskQueue.Demo.run()
