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

defmodule TaskQueue.Operation do
  @moduledoc """
  Macro para adicionar um despachante de operações a um módulo de tarefas.

  O módulo que usa este macro deve definir funções públicas para cada operação,
  recebendo os argumentos da tarefa.
  """

  defmacro __using__(_opts) do
    quote do
      def run_operation(operation, args) do
        apply(__MODULE__, operation, [args])
      end
    end
  end
end

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas concorrente baseada em GenServer.

  Opções de inicialização:
    * `:name` — nome registrado do servidor; padrão: `TaskQueue`.
    * `:max_concurrency` — máximo de tarefas em execução; padrão: `4`.
    * `:periodic` — lista de mapas com `:module`, `:operation`, `:interval`
      e, opcionalmente, `:args`. O intervalo é dado em milissegundos.
  """

  use GenServer

  @type task :: %{
          id: pos_integer(),
          module: module(),
          operation: atom(),
          args: term(),
          enqueued_at: integer()
        }

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Adiciona uma tarefa à fila e retorna seu identificador."
  def enqueue(server \\ __MODULE__, module, operation, args) do
    GenServer.call(server, {:enqueue, module, operation, args})
  end

  @doc "Consulta tarefas na fila, tarefas em execução e resultados recentes."
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      periodic = Keyword.get(opts, :periodic, [])

      state = %{
        queue: :queue.new(),
        running: %{},
        max_concurrency: max_concurrency,
        periodic: periodic,
        recent_results: []
      }

      Enum.each(periodic, &schedule_periodic/1)
      {:ok, state}
    end
  end

  @impl true
  def handle_call({:enqueue, module, operation, args}, _from, state) do
    {id, state} = add_task(state, module, operation, args)
    {:reply, id, dispatch(state)}
  end

  def handle_call(:status, _from, state) do
    queued =
      state.queue
      |> :queue.to_list()
      |> Enum.map(&task_summary/1)

    running =
      state.running
      |> Map.values()
      |> Enum.map(fn info ->
        info.task
        |> task_summary()
        |> Map.put(:pid, info.pid)
        |> Map.put(:started_at, info.started_at)
      end)

    {:reply,
     %{
       queued: queued,
       running: running,
       running_count: map_size(state.running),
       queued_count: length(queued),
       max_concurrency: state.max_concurrency,
       recent_results: state.recent_results
     }, state}
  end

  @impl true
  def handle_info({:task_result, id, outcome}, state) do
    case take_running_by_id(state.running, id) do
      :not_found ->
        {:noreply, state}

      {ref, info, running} ->
        Process.demonitor(ref, [:flush])

        result = %{
          id: id,
          status: outcome_status(outcome),
          outcome: outcome,
          finished_at: System.monotonic_time(:millisecond)
        }

        state = %{state | running: running}
        {:noreply, dispatch(add_recent_result(state, result))}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.running, ref) do
      {nil, _running} ->
        {:noreply, state}

      {info, running} ->
        result = %{
          id: info.task.id,
          status: :error,
          outcome: {:error, {:exit, reason}},
          finished_at: System.monotonic_time(:millisecond)
        }

        state = %{state | running: running}
        {:noreply, dispatch(add_recent_result(state, result))}
    end
  end

  def handle_info({:periodic, spec}, state) do
    schedule_periodic(spec)
    {_, state} = add_task(state, spec.module, spec.operation, Map.get(spec, :args))
    {:noreply, dispatch(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp add_task(state, module, operation, args) do
    id = System.unique_integer([:positive, :monotonic])

    task = %{
      id: id,
      module: module,
      operation: operation,
      args: args,
      enqueued_at: System.monotonic_time(:millisecond)
    }

    {id, %{state | queue: :queue.in(task, state.queue)}}
  end

  defp dispatch(state) when map_size(state.running) >= state.max_concurrency,
    do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, task}, queue} ->
        parent = self()

        {pid, ref} =
          spawn_monitor(fn ->
            outcome =
              try do
                {:ok, apply(task.module, :run_operation, [task.operation, task.args])}
              rescue
                exception -> {:error, {:error, exception, __STACKTRACE__}}
              catch
                kind, reason -> {:error, {kind, reason, __STACKTRACE__}}
              end

            send(parent, {:task_result, task.id, outcome})
          end)

        info = %{
          task: task,
          pid: pid,
          started_at: System.monotonic_time(:millisecond)
        }

        state = %{state | queue: queue, running: Map.put(state.running, ref, info)}
        dispatch(state)
    end
  end

  defp take_running_by_id(running, id) do
    case Enum.find(running, fn {_ref, info} -> info.task.id == id end) do
      nil ->
        :not_found

      {ref, info} ->
        {ref, info, Map.delete(running, ref)}
    end
  end

  defp add_recent_result(state, result) do
    recent_results = [result | state.recent_results] |> Enum.take(50)
    %{state | recent_results: recent_results}
  end

  defp outcome_status({:ok, _result}), do: :ok
  defp outcome_status({:error, _reason}), do: :error

  defp task_summary(task) do
    %{
      id: task.id,
      module: task.module,
      operation: task.operation,
      args: task.args,
      enqueued_at: task.enqueued_at
    }
  end

  defp schedule_periodic(%{interval: interval} = spec)
       when is_integer(interval) and interval > 0 do
    Process.send_after(self(), {:periodic, spec}, interval)
  end

  defp schedule_periodic(_spec), do: :ok
end

defmodule TaskQueue.ExampleOperations do
  @moduledoc "Exemplo de módulo compatível com `TaskQueue`."

  use TaskQueue.Operation

  def echo(value), do: value

  def add(%{a: a, b: b}), do: a + b
end
