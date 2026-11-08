# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SessionManager do
  use GenServer

  @default_max_concurrency System.schedulers_online()

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def submit(server \\ __MODULE__, user_id, session_id, operation_id, fun)
      when is_function(fun, 0) do
    GenServer.call(server, {:submit, user_id, session_id, operation_id, fun})
  end

  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  def operation(server \\ __MODULE__, user_id, session_id, operation_id) do
    GenServer.call(server, {:operation, {user_id, session_id, operation_id}})
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      case DynamicSupervisor.start_link(strategy: :one_for_one) do
        {:ok, supervisor} ->
          {:ok,
           %{
             supervisor: supervisor,
             max_concurrency: max_concurrency,
             active: %{},
             monitors: %{},
             queue: :queue.new(),
             operations: %{},
             history: []
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    end
  end

  @impl true
  def handle_call({:submit, user_id, session_id, operation_id, fun}, _from, state) do
    key = {user_id, session_id, operation_id}

    if Map.has_key?(state.operations, key) do
      {:reply, {:error, :duplicate_operation}, state}
    else
      now = timestamp()

      record = %{
        user_id: user_id,
        session_id: session_id,
        operation_id: operation_id,
        status: :queued,
        submitted_at: now
      }

      state =
        state
        |> put_in([:operations, key], record)
        |> add_event(key, :queued, nil)
        |> Map.update!(:queue, &:queue.in({key, fun}, &1))
        |> start_queued_operations()

      {:reply, {:ok, :accepted}, state}
    end
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:operation, key}, _from, state) do
    reply =
      case Map.fetch(state.operations, key) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:operation_result, operation_ref, result}, state) do
    case Map.pop(state.active, operation_ref) do
      {nil, _active} ->
        {:noreply, state}

      {active, remaining_active} ->
        Process.demonitor(active.monitor, [:flush])

        state =
          %{state | active: remaining_active, monitors: Map.delete(state.monitors, active.monitor)}
          |> finish_operation(active.key, result)
          |> start_queued_operations()

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {operation_ref, remaining_monitors} ->
        case Map.pop(state.active, operation_ref) do
          {nil, remaining_active} ->
            {:noreply, %{state | active: remaining_active, monitors: remaining_monitors}}

          {active, remaining_active} ->
            state = %{
              state
              | active: remaining_active,
                monitors: remaining_monitors
            }

            state =
              state
              |> finish_operation(active.key, {:error, {:worker_down, reason}})
              |> start_queued_operations()

            {:noreply, state}
        end
    end
  end

  @impl true
  def terminate(_reason, state) do
    Supervisor.stop(state.supervisor, :normal, 5_000)
  catch
    :exit, _ -> :ok
  end

  defp start_queued_operations(state) do
    if map_size(state.active) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {key, fun}}, queue} ->
          state = %{state | queue: queue}
          operation_ref = make_ref()

          child_opts = [
            owner: self(),
            operation_ref: operation_ref,
            fun: fun
          ]

          case DynamicSupervisor.start_child(state.supervisor, {SessionManager.Worker, child_opts}) do
            {:ok, pid} ->
              monitor = Process.monitor(pid)

              record = Map.fetch!(state.operations, key)
              record = Map.merge(record, %{status: :running, started_at: timestamp()})

              state =
                state
                |> put_in([:operations, key], record)
                |> Map.update!(:active, &Map.put(&1, operation_ref, %{key: key, pid: pid, monitor: monitor}))
                |> Map.update!(:monitors, &Map.put(&1, monitor, operation_ref))
                |> add_event(key, :running, nil)

              start_queued_operations(state)

            {:ok, pid, _info} ->
              monitor = Process.monitor(pid)

              record = Map.fetch!(state.operations, key)
              record = Map.merge(record, %{status: :running, started_at: timestamp()})

              state =
                state
                |> put_in([:operations, key], record)
                |> Map.update!(:active, &Map.put(&1, operation_ref, %{key: key, pid: pid, monitor: monitor}))
                |> Map.update!(:monitors, &Map.put(&1, monitor, operation_ref))
                |> add_event(key, :running, nil)

              start_queued_operations(state)

            {:error, reason} ->
              state
              |> finish_operation(key, {:error, {:worker_start_failed, reason}})
              |> start_queued_operations()
          end
      end
    end
  end

  defp finish_operation(state, key, {:ok, result}) do
    record =
      state.operations
      |> Map.fetch!(key)
      |> Map.merge(%{status: :completed, completed_at: timestamp(), result: result})

    state
    |> put_in([:operations, key], record)
    |> add_event(key, :completed, %{result: result})
  end

  defp finish_operation(state, key, {:error, reason}) do
    record =
      state.operations
      |> Map.fetch!(key)
      |> Map.merge(%{status: :failed, completed_at: timestamp(), error: reason})

    state
    |> put_in([:operations, key], record)
    |> add_event(key, :failed, %{error: reason})
  end

  defp add_event(state, key, status, details) do
    event = %{key: key, status: status, at: timestamp(), details: details}
    Map.update!(state, :history, &[event | &1])
  end

  defp timestamp, do: System.system_time(:millisecond)
end

defmodule SessionManager.Worker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def child_spec(opts) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init(opts) do
    send(self(), :execute)

    {:ok,
     %{
       owner: Keyword.fetch!(opts, :owner),
       operation_ref: Keyword.fetch!(opts, :operation_ref),
       fun: Keyword.fetch!(opts, :fun)
     }}
  end

  @impl true
  def handle_info(:execute, state) do
    result =
      try do
        {:ok, state.fun.()}
      rescue
        error -> {:error, {:error, error}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    send(state.owner, {:operation_result, state.operation_ref, result})
    {:stop, :normal, state}
  end
end
