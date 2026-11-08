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

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas com histórico, consulta de estado e execução idempotente.

  As tarefas podem ser funções de aridade zero ou tuplas `{modulo, funcao, argumentos}`.
  """

  def start_link(opts \\ [])

  def start_link(opts) when is_list(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    cond do
      not is_integer(max_concurrency) or max_concurrency < 1 ->
        {:error, {:validation, :max_concurrency_must_be_a_positive_integer}}

      true ->
        manager_name = Keyword.get(opts, :name, __MODULE__)
        workers_name = Keyword.get(opts, :workers_name, TaskQueue.Workers)
        root_name = Keyword.get(opts, :supervisor_name, TaskQueue.Supervisor)

        worker_supervisor_opts =
          [strategy: :one_for_one] ++ maybe_name(workers_name)

        children = [
          %{
            id: :task_queue_workers,
            start: {DynamicSupervisor, :start_link, [worker_supervisor_opts]},
            type: :supervisor
          },
          %{
            id: :task_queue_server,
            start:
              {TaskQueue.Server, :start_link,
               [{workers_name, max_concurrency, manager_name}]},
            type: :worker
          }
        ]

        Supervisor.start_link(children, [strategy: :one_for_one] ++ maybe_name(root_name))
    end
  end

  def start_link(_opts), do: {:error, {:validation, :options_must_be_a_keyword_list}}

  def enqueue(operation_id, task, server \\ __MODULE__) do
    cond do
      is_nil(operation_id) ->
        {:error, {:validation, :operation_id_cannot_be_nil}}

      not valid_task?(task) ->
        {:error, {:validation, :invalid_task}}

      true ->
        safe_call(server, {:enqueue, operation_id, task})
    end
  end

  def status(operation_id, server \\ __MODULE__) do
    if is_nil(operation_id) do
      {:error, {:validation, :operation_id_cannot_be_nil}}
    else
      safe_call(server, {:status, operation_id})
    end
  end

  def in_progress(server \\ __MODULE__), do: safe_call(server, :in_progress)
  def history(server \\ __MODULE__), do: safe_call(server, :history)

  defp valid_task?(task), do: is_function(task, 0) or valid_mfa?(task)

  defp valid_mfa?({module, function, args}) do
    is_atom(module) and is_atom(function) and is_list(args)
  end

  defp valid_mfa?(_), do: false

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp maybe_name(nil), do: []
  defp maybe_name(name), do: [name: name]
end

defmodule TaskQueue.Server do
  use GenServer

  def start_link({workers_name, max_concurrency, manager_name}) do
    opts = if is_nil(manager_name), do: [], else: [name: manager_name]
    GenServer.start_link(__MODULE__, {workers_name, max_concurrency}, opts)
  end

  @impl true
  def init({workers_name, max_concurrency}) do
    {:ok,
     %{
       workers: workers_name,
       max_concurrency: max_concurrency,
       pending: :queue.new(),
       operations: %{},
       monitors: %{},
       history: []
     }}
  end

  @impl true
  def handle_call({:enqueue, id, task}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, _operation} ->
        state = record(state, id, :duplicate, nil)
        {:reply, {:ok, :already_exists}, state}

      :error ->
        now = timestamp()

        operation = %{
          id: id,
          task: task,
          status: :queued,
          inserted_at: now,
          started_at: nil,
          finished_at: nil,
          result: nil,
          error: nil,
          worker: nil,
          monitor: nil
        }

        state =
          state
          |> put_in([:operations, id], operation)
          |> put_in([:pending], :queue.in(id, state.pending))
          |> record(id, :queued, nil)
          |> dispatch()

        {:reply, {:ok, :accepted}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} -> {:reply, {:ok, public_operation(operation)}, state}
      :error -> {:reply, {:ok, nil}, state}
    end
  end

  def handle_call(:in_progress, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running]))
      |> Enum.map(&public_operation/1)
      |> Enum.sort_by(& &1.inserted_at)

    {:reply, {:ok, operations}, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, {:ok, Enum.reverse(state.history)}, state}
  end

  @impl true
  def handle_info({:task_finished, id, outcome}, state) do
    case Map.get(state.operations, id) do
      %{status: :running, monitor: monitor} = operation ->
        Process.demonitor(monitor, [:flush])

        state =
          state
          |> remove_monitor(monitor)
          |> finish_operation(operation, outcome)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.get(state.operations, id) do
          %{status: :running} = operation ->
            state
            |> finish_operation(operation, {:error, {:worker_exit, reason}})
            |> dispatch()
            |> then(&{:noreply, &1})

          _ ->
            {:noreply, state}
        end
    end
  end

  defp dispatch(state) do
    if map_size(state.monitors) >= state.max_concurrency do
      state
    else
      case :queue.out(state.pending) do
        {:empty, _pending} ->
          state

        {{:value, id}, pending} ->
          state = %{state | pending: pending}
          operation = Map.fetch!(state.operations, id)

          case DynamicSupervisor.start_child(
                 state.workers,
                 {TaskQueue.Worker, [self(), id, operation.task]}
               ) do
            {:ok, pid} ->
              monitor = Process.monitor(pid)
              now = timestamp()

              operation = %{
                operation
                | status: :running,
                  started_at: now,
                  worker: pid,
                  monitor: monitor
              }

              state =
                state
                |> put_in([:operations, id], operation)
                |> put_in([:monitors, monitor], id)
                |> record(id, :started, nil)

              dispatch(state)

            {:error, reason} ->
              state
              |> finish_operation(operation, {:error, {:worker_start, reason}})
              |> dispatch()
          end
      end
    end
  end

  defp finish_operation(state, operation, {:ok, result}) do
    now = timestamp()

    updated = %{
      operation
      | status: :completed,
        finished_at: now,
        result: result,
        error: nil,
        worker: nil,
        monitor: nil
    }

    state
    |> put_in([:operations, operation.id], updated)
    |> record(operation.id, :completed, result)
  end

  defp finish_operation(state, operation, {:error, reason}) do
    now = timestamp()
    operational_error = {:operational, reason}

    updated = %{
      operation
      | status: :failed,
        finished_at: now,
        result: nil,
        error: operational_error,
        worker: nil,
        monitor: nil
    }

    state
    |> put_in([:operations, operation.id], updated)
    |> record(operation.id, :failed, operational_error)
  end

  defp remove_monitor(state, monitor) do
    %{state | monitors: Map.delete(state.monitors, monitor)}
  end

  defp record(state, id, event, details) do
    entry = %{
      operation_id: id,
      event: event,
      details: details,
      timestamp: timestamp()
    }

    %{state | history: [entry | state.history]}
  end

  defp public_operation(operation) do
    Map.take(operation, [
      :id,
      :status,
      :inserted_at,
      :started_at,
      :finished_at,
      :result,
      :error
    ])
  end

  defp timestamp, do: System.system_time(:millisecond)
end

defmodule TaskQueue.Worker do
  use GenServer

  def child_spec(args) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init({manager, operation_id, task}) do
    {:ok, {manager, operation_id, task}, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, {manager, operation_id, task} = state) do
    send(manager, {:task_finished, operation_id, execute(task)})
    {:stop, :normal, state}
  end

  defp execute(task) do
    try do
      result =
        case task do
          function when is_function(function, 0) -> function.()
          {module, function, args} -> apply(module, function, args)
        end

      case result do
        {:error, reason} -> {:error, reason}
        value -> {:ok, value}
      end
    rescue
      exception -> {:error, {:exception, exception, __STACKTRACE__}}
    catch
      kind, reason -> {:error, {kind, reason, __STACKTRACE__}}
    end
  end
end
