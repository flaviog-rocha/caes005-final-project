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

defmodule TaskQueue.Worker do
  use GenServer

  def start_link(owner, id, task) do
    GenServer.start_link(__MODULE__, {owner, id, task})
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :run}}

  @impl true
  def handle_continue(:run, {owner, id, task}) do
    outcome =
      try do
        {:ok, task.()}
      catch
        kind, reason -> {:error, {:task_failure, kind, reason}}
      end

    send(owner, {:task_finished, id, self(), outcome})
    {:stop, :normal, {owner, id, task}}
  end
end

defmodule TaskQueue do
  use GenServer

  @type operation_id :: binary() | integer()

  def start_link(opts \\ []) when is_list(opts) do
    if Keyword.keyword?(opts) and
         Enum.all?(Keyword.keys(opts), &(&1 in [:max_concurrency, :name])) and
         length(Keyword.keys(opts)) == length(Enum.uniq(Keyword.keys(opts))) do
      max_concurrency = Keyword.get(opts, :max_concurrency, 1)
      name = Keyword.get(opts, :name)

      if is_integer(max_concurrency) and max_concurrency > 0 do
        start_opts = if is_nil(name), do: [], else: [name: name]

        case GenServer.start_link(__MODULE__, max_concurrency, start_opts) do
          {:error, reason} -> {:error, {:operational, reason}}
          result -> result
        end
      else
        {:error, {:validation, :invalid_max_concurrency}}
      end
    else
      {:error, {:validation, :invalid_options}}
    end
  end

  def start_link(_opts), do: {:error, {:validation, :invalid_options}}

  @spec submit(GenServer.server(), operation_id(), (() -> term())) :: term()
  def submit(server, id, task), do: call(server, {:submit, id, task})

  def status(server, id), do: call(server, {:status, id})

  def operations(server), do: call(server, :operations)

  def history(server), do: call(server, :history)

  @impl true
  def init(max_concurrency) do
    case DynamicSupervisor.start_link(strategy: :one_for_one) do
      {:ok, supervisor} ->
        {:ok,
         %{
           supervisor: supervisor,
           max_concurrency: max_concurrency,
           operations: %{},
           queue: :queue.new(),
           running: %{},
           monitors: %{},
           history: []
         }}

      {:error, reason} ->
        {:stop, {:operational, reason}}
    end
  end

  @impl true
  def handle_call({:submit, id, task}, _from, state) do
    cond do
      not valid_id?(id) ->
        {:reply, {:error, {:validation, :invalid_operation_id}}, state}

      not is_function(task, 0) ->
        {:reply, {:error, {:validation, :task_must_be_zero_arity_function}}, state}

      Map.has_key?(state.operations, id) ->
        existing = public_operation(Map.fetch!(state.operations, id))
        {:reply, {:ok, {:duplicate, existing}}, state}

      true ->
        now = timestamp()

        operation = %{
          id: id,
          status: :queued,
          submitted_at: now,
          started_at: nil,
          finished_at: nil,
          worker_pid: nil,
          fun: task
        }

        state =
          state
          |> put_operation(id, operation)
          |> enqueue(id)
          |> add_event(:submitted, id, %{})
          |> dispatch()

        {:reply, {:ok, public_operation(Map.fetch!(state.operations, id))}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    cond do
      not valid_id?(id) ->
        {:reply, {:error, {:validation, :invalid_operation_id}}, state}

      operation = Map.get(state.operations, id) ->
        {:reply, {:ok, public_operation(operation)}, state}

      true ->
        {:reply, {:error, {:validation, :unknown_operation}}, state}
    end
  end

  def handle_call(:operations, _from, state) do
    result =
      state.operations
      |> Map.values()
      |> Enum.map(&public_operation/1)

    {:reply, {:ok, result}, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, {:ok, Enum.reverse(state.history)}, state}
  end

  @impl true
  def handle_info({:task_finished, id, pid, outcome}, state) do
    case Map.get(state.operations, id) do
      %{status: :running, worker_pid: ^pid, monitor_ref: ref} = operation ->
        Process.demonitor(ref, [:flush])

        {status, fields, event, details} =
          case outcome do
            {:ok, result} ->
              {:succeeded, %{result: result}, :succeeded, %{result: result}}

            {:error, reason} ->
              {:failed, %{error: reason}, :failed, %{error: reason}}
          end

        updated =
          operation
          |> Map.merge(fields)
          |> Map.put(:status, status)
          |> Map.put(:finished_at, timestamp())
          |> Map.put(:worker_pid, nil)
          |> Map.delete(:monitor_ref)

        state =
          state
          |> put_operation(id, updated)
          |> Map.update!(:running, &Map.delete(&1, pid))
          |> Map.update!(:monitors, &Map.delete(&1, ref))
          |> add_event(event, id, details)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.monitors, ref) do
      nil ->
        {:noreply, state}

      id ->
        operation = Map.fetch!(state.operations, id)

        if operation.status == :running and operation.worker_pid == pid do
          error = {:worker_down, reason}

          updated =
            operation
            |> Map.put(:status, :failed)
            |> Map.put(:error, error)
            |> Map.put(:finished_at, timestamp())
            |> Map.put(:worker_pid, nil)
            |> Map.delete(:monitor_ref)

          state =
            state
            |> put_operation(id, updated)
            |> Map.update!(:running, &Map.delete(&1, pid))
            |> Map.update!(:monitors, &Map.delete(&1, ref))
            |> add_event(:failed, id, %{error: error})
            |> dispatch()

          {:noreply, state}
        else
          {:noreply, state}
        end
    end
  end

  @impl true
  def terminate(_reason, state) do
    if Process.alive?(state.supervisor) do
      DynamicSupervisor.stop(state.supervisor, :normal)
    end

    :ok
  catch
    :exit, _ -> :ok
  end

  defp call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp valid_id?(id), do: (is_binary(id) and byte_size(id) > 0) or is_integer(id)

  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, id}, queue} ->
          state = %{state | queue: queue}
          operation = Map.fetch!(state.operations, id)

          child_spec = %{
            id: make_ref(),
            start: {TaskQueue.Worker, :start_link, [self(), id, operation.fun]},
            restart: :temporary,
            shutdown: 5_000,
            type: :worker
          }

          case DynamicSupervisor.start_child(state.supervisor, child_spec) do
            {:ok, pid} ->
              ref = Process.monitor(pid)
              now = timestamp()

              updated =
                operation
                |> Map.put(:status, :running)
                |> Map.put(:started_at, now)
                |> Map.put(:worker_pid, pid)
                |> Map.put(:monitor_ref, ref)
                |> Map.delete(:fun)

              state =
                state
                |> put_operation(id, updated)
                |> Map.update!(:running, &Map.put(&1, pid, id))
                |> Map.update!(:monitors, &Map.put(&1, ref, id))
                |> add_event(:started, id, %{worker_pid: pid})

              dispatch(state)

            {:error, reason} ->
              error = {:worker_start_failed, reason}

              updated =
                operation
                |> Map.put(:status, :failed)
                |> Map.put(:error, error)
                |> Map.put(:finished_at, timestamp())
                |> Map.put(:worker_pid, nil)
                |> Map.delete(:fun)

              state
              |> put_operation(id, updated)
              |> add_event(:failed, id, %{error: error})
              |> dispatch()
          end

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp enqueue(state, id), do: %{state | queue: :queue.in(id, state.queue)}

  defp put_operation(state, id, operation) do
    %{state | operations: Map.put(state.operations, id, operation)}
  end

  defp add_event(state, type, id, details) do
    event = %{type: type, id: id, at: timestamp(), details: details}
    %{state | history: [event | state.history]}
  end

  defp public_operation(operation), do: Map.drop(operation, [:fun, :monitor_ref])

  defp timestamp, do: System.system_time(:millisecond)
end
