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
# Execução: `elixir task_queue.exs`
# Requisitos atendidos:
#   1. Consulta do estado das operações (status/1, job/2)
#   2. Uso de macros (TaskQueue.Macros: deftask/2, timed/1)
#   3. Operações periódicas (tick agendado com Process.send_after/3)
#   4. Comunicação entre processos (mensagens, assinaturas e monitores)
#   5. Persistência em banco fictício (TaskQueue.FakeDB, baseado em Agent)
# ============================================================================


defmodule TaskQueue.Macros do
  @moduledoc """
  Macros utilitárias.

  * `deftask/2` define um handler de tarefa e registra seu nome em um
    atributo acumulado, gerando a função `__handlers__/0` no módulo que a usa.
  * `timed/1` mede o tempo de execução de um bloco e devolve `{resultado, ms}`.
  """

  defmacro __using__(_opts) do
    quote do
      import TaskQueue.Macros
      Module.register_attribute(__MODULE__, :task_handlers, accumulate: true)
      @before_compile TaskQueue.Macros
    end
  end

  # Gera `handle_<nome>/1` e registra `{nome, handle_<nome>}`.
  # `var!(payload)` permite que o bloco do usuário enxergue a variável `payload`.
  defmacro deftask(name, do: block) do
    fun = :"handle_#{name}"

    quote do
      @task_handlers {unquote(name), unquote(fun)}

      def unquote(fun)(var!(payload)) do
        _ = var!(payload)
        unquote(block)
      end
    end
  end

  # Injeta, ao final da compilação do módulo, a função `__handlers__/0`.
  defmacro __before_compile__(env) do
    handlers = Module.get_attribute(env.module, :task_handlers)

    quote do
      @doc false
      def __handlers__, do: unquote(Macro.escape(handlers))
    end
  end

  # Mede o tempo do bloco em milissegundos (monotônico, não sofre com ajuste de relógio).
  defmacro timed(do: block) do
    quote do
      start = System.monotonic_time(:millisecond)
      result = unquote(block)
      {result, System.monotonic_time(:millisecond) - start}
    end
  end
end


defmodule TaskQueue.Job do
  @moduledoc "Representa uma tarefa ao longo de seu ciclo de vida."

  defstruct [
    :id,
    :name,
    :payload,
    :result,
    :enqueued_at,
    :started_at,
    :finished_at,
    :duration_ms,
    status: :queued
  ]
end


defmodule TaskQueue.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.
  Simula operações de persistência (upsert, leitura e snapshots).
  Pode ser substituído por um repositório Ecto sem alterar o Manager.
  """

  @max_snapshots 10

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{tasks: %{}, snapshots: []} end, name: __MODULE__)
  end

  def upsert_task(%TaskQueue.Job{id: id} = job) do
    Agent.update(__MODULE__, fn db -> %{db | tasks: Map.put(db.tasks, id, job)} end)
  end

  def get_task(id) do
    Agent.get(__MODULE__, fn db -> Map.get(db.tasks, id) end)
  end

  def save_snapshot(snapshot) do
    Agent.update(__MODULE__, fn db ->
      %{db | snapshots: Enum.take([snapshot | db.snapshots], @max_snapshots)}
    end)
  end

  def list_snapshots do
    Agent.get(__MODULE__, & &1.snapshots)
  end
end


defmodule TaskQueue.Jobs do
  @moduledoc "Conjunto de tarefas executáveis, definidas com a macro `deftask`."

  use TaskQueue.Macros

  deftask :send_email do
    Process.sleep(300)
    "email enviado para #{Map.get(payload, :to)}"
  end

  deftask :resize_image do
    Process.sleep(500)
    "imagem #{Map.get(payload, :file)} redimensionada"
  end

  deftask :fail_me do
    Process.sleep(100)
    raise "falha simulada na tarefa"
  end
end


defmodule TaskQueue.Manager do
  @moduledoc """
  GenServer responsável por:

    * enfileirar tarefas e executá-las com limite de concorrência;
    * expor o estado atual das operações (`status/1`, `job/2`);
    * executar um ciclo periódico (`:tick`) que despacha tarefas e grava snapshots;
    * notificar processos assinantes sobre eventos do ciclo de vida das tarefas;
    * monitorar workers e assinantes, tratando falhas e desconexões.
  """

  use GenServer
  require TaskQueue.Macros

  @default_tick_ms 500
  @default_concurrency 3

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Enfileira uma tarefa pelo nome. Retorna `{:ok, id}` ou `{:error, :unknown_task}`."
  def enqueue(server \\ __MODULE__, name, payload) do
    GenServer.call(server, {:enqueue, name, payload})
  end

  @doc "Retorna um resumo do estado atual das operações."
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  @doc "Consulta uma tarefa específica pelo id (persistida no banco fictício)."
  def job(server \\ __MODULE__, id) do
    GenServer.call(server, {:job, id})
  end

  @doc "Assina eventos do gerenciador. O processo chamador passa a receber `{:task_queue, evento}`."
  def subscribe(server \\ __MODULE__) do
    GenServer.call(server, :subscribe)
  end

  @doc "Cancela a assinatura do processo chamador."
  def unsubscribe(server \\ __MODULE__) do
    GenServer.call(server, :unsubscribe)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    jobs = Keyword.get(opts, :jobs, TaskQueue.Jobs)

    state = %{
      jobs: jobs,
      handlers: Map.new(jobs.__handlers__()),
      queue: :queue.new(),
      running: %{},
      done: 0,
      failed: 0,
      subscribers: %{},
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_concurrency),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms)
    }

    # Agenda o primeiro ciclo periódico.
    Process.send_after(self(), :tick, state.tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, name, payload}, _from, state) do
    case Map.fetch(state.handlers, name) do
      :error ->
        {:reply, {:error, :unknown_task}, state}

      {:ok, _fun} ->
        job = %TaskQueue.Job{
          id: System.unique_integer([:positive, :monotonic]),
          name: name,
          payload: payload,
          status: :queued,
          enqueued_at: now()
        }

        state =
          %{state | queue: :queue.in(job, state.queue)}
          |> persist(job)
          |> broadcast({:enqueued, job})
          |> dispatch()

        {:reply, {:ok, job.id}, state}
    end
  end

  def handle_call(:status, _from, state) do
    reply = %{
      queued: :queue.len(state.queue),
      running:
        Enum.map(state.running, fn {_id, %{job: job}} ->
          %{id: job.id, name: job.name, started_at: job.started_at}
        end),
      done: state.done,
      failed: state.failed,
      subscribers: map_size(state.subscribers),
      max_concurrency: state.max_concurrency
    }

    {:reply, reply, state}
  end

  def handle_call({:job, id}, _from, state) do
    case TaskQueue.FakeDB.get_task(id) do
      nil -> {:reply, :not_found, state}
      job -> {:reply, {:ok, job}, state}
    end
  end

  def handle_call(:subscribe, {pid, _tag}, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      mref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, mref)}}
    end
  end

  def handle_call(:unsubscribe, {pid, _tag}, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _} ->
        {:reply, :ok, state}

      {mref, subscribers} ->
        Process.demonitor(mref, [:flush])
        {:reply, :ok, %{state | subscribers: subscribers}}
    end
  end

  # Ciclo periódico: despacha tarefas pendentes e grava um snapshot no banco.
  @impl true
  def handle_info(:tick, state) do
    state = dispatch(state)

    TaskQueue.FakeDB.save_snapshot(%{
      at: now(),
      queued: :queue.len(state.queue),
      running: map_size(state.running),
      done: state.done,
      failed: state.failed
    })

    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  # Resultado enviado por um worker.
  def handle_info({:task_result, id, outcome, ms}, state) do
    if Map.has_key?(state.running, id) do
      {status, result} =
        case outcome do
          {:ok, value} -> {:done, value}
          {:error, reason} -> {:failed, reason}
        end

      {:noreply, state |> finish(id, status, result, ms) |> dispatch()}
    else
      {:noreply, state}
    end
  end

  # Queda de um processo monitorado: assinante ou worker.
  def handle_info({:DOWN, mref, :process, pid, reason}, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
    else
      case Enum.find(state.running, fn {_id, r} -> r.mref == mref end) do
        nil ->
          {:noreply, state}

        {id, _} ->
          {:noreply, state |> finish(id, :failed, {:crash, reason}, 0) |> dispatch()}
      end
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Inicia tarefas enquanto houver vagas de concorrência e itens na fila.
  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, job}, rest} ->
          %{state | queue: rest}
          |> start_job(job)
          |> dispatch()

        {:empty, _} ->
          state
      end
    else
      state
    end
  end

  defp start_job(state, job) do
    manager = self()
    mod = state.jobs
    fun = Map.fetch!(state.handlers, job.name)

    # Worker isolado: um crash não derruba o gerenciador.
    {pid, mref} =
      spawn_monitor(fn ->
        {outcome, ms} = TaskQueue.Macros.timed(do: execute(mod, fun, job.payload))
        send(manager, {:task_result, job.id, outcome, ms})
      end)

    job = %{job | status: :running, started_at: now()}

    state = %{state | running: Map.put(state.running, job.id, %{job: job, pid: pid, mref: mref})}

    state
    |> persist(job)
    |> broadcast({:started, job})
  end

  # Executa o handler capturando exceções e throws.
  @doc false
  def execute(mod, fun, payload) do
    {:ok, apply(mod, fun, [payload])}
  rescue
    e -> {:error, Exception.message(e)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp finish(state, id, status, result, ms) do
    {%{job: job, mref: mref}, running} = Map.pop(state.running, id)
    Process.demonitor(mref, [:flush])

    job = %{job | status: status, result: result, finished_at: now(), duration_ms: ms}

    state =
      case status do
        :done -> %{state | done: state.done + 1}
        :failed -> %{state | failed: state.failed + 1}
      end

    %{state | running: running}
    |> persist(job)
    |> broadcast({:finished, job})
  end

  defp persist(state, job) do
    TaskQueue.FakeDB.upsert_task(job)
    state
  end

  defp broadcast(state, event) do
    Enum.each(Map.keys(state.subscribers), &send(&1, {:task_queue, event}))
    state
  end

  defp now, do: System.system_time(:millisecond)
end


defmodule TaskQueue.Demo do
  @moduledoc "Demonstração de uso: enfileira tarefas e acompanha os eventos."

  def run do
    {:ok, _} = TaskQueue.FakeDB.start_link()
    {:ok, _} = TaskQueue.Manager.start_link(max_concurrency: 2, tick_ms: 300)
    :ok = TaskQueue.Manager.subscribe()

    {:ok, _} = TaskQueue.Manager.enqueue(:send_email, %{to: "ana@exemplo.com"})
    {:ok, _} = TaskQueue.Manager.enqueue(:resize_image, %{file: "foto.png"})
    {:ok, _} = TaskQueue.Manager.enqueue(:fail_me, %{})
    {:ok, last_id} = TaskQueue.Manager.enqueue(:send_email, %{to: "bob@exemplo.com"})

    IO.inspect(TaskQueue.Manager.status(), label: "status inicial")

    listen(2_000)

    IO.inspect(TaskQueue.Manager.status(), label: "status final")
    IO.inspect(TaskQueue.Manager.job(last_id), label: "consulta da última tarefa")
    IO.inspect(TaskQueue.FakeDB.list_snapshots() |> length(), label: "snapshots gravados")
  end

  defp listen(timeout) do
    receive do
      {:task_queue, event} ->
        IO.inspect(event, label: "evento")
        listen(timeout)
    after
      timeout -> :ok
    end
  end
end

TaskQueue.Demo.run()
