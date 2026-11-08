# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Cache do
  use Supervisor

  @moduledoc """
  Concorrência limitada, histórico serializado e recuperação supervisionada para um cache.
  """

  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if is_integer(max_concurrency) and max_concurrency > 0 do
      Supervisor.start_link(__MODULE__, max_concurrency, name: __MODULE__)
    else
      {:error, {:invalid_max_concurrency, max_concurrency}}
    end
  end

  @impl true
  def init(max_concurrency) do
    children = [
      {Cache.Store, []},
      {Task.Supervisor, name: Cache.TaskSupervisor},
      {Cache.Manager, max_concurrency}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def put(key, value), do: Cache.Manager.execute({:put, key, value})
  def get(key), do: Cache.Manager.execute({:get, key})
  def delete(key), do: Cache.Manager.execute({:delete, key})
  def clear, do: Cache.Manager.execute(:clear)
  def history, do: Cache.Store.history()
end

defmodule Cache.Store do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, %{}, Keyword.put(opts, :name, __MODULE__))

  @impl true
  def init(state), do: {:ok, state}

  def execute(operation), do: GenServer.call(__MODULE__, {:execute, operation}, :infinity)
  def history, do: GenServer.call(__MODULE__, :history, :infinity)

  def record_failure(operation, reason) do
    GenServer.call(__MODULE__, {:record_failure, operation, reason}, :infinity)
  end

  @impl true
  def handle_call({:execute, operation}, _from, state) do
    {result, state} = run(operation, state)
    {:reply, result, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:record_failure, operation, reason}, _from, state) do
    entry = history_entry(state, operation, {:error, reason})
    {:reply, :ok, %{state | history: [entry | state.history], sequence: entry.sequence}}
  end

  defp run({:put, key, value} = operation, state) do
    result = {:ok, value}
    {result, record(%{state | data: Map.put(state.data, key, value)}, operation, result)}
  end

  defp run({:get, key} = operation, state) do
    result =
      case Map.fetch(state.data, key) do
        {:ok, value} -> {:ok, value}
        :error -> :not_found
      end

    {result, record(state, operation, result)}
  end

  defp run({:delete, key} = operation, state) do
    result =
      case Map.fetch(state.data, key) do
        {:ok, value} -> {:ok, value}
        :error -> :not_found
      end

    {result, record(%{state | data: Map.delete(state.data, key)}, operation, result)}
  end

  defp run(:clear = operation, state) do
    result = :ok
    {result, record(%{state | data: %{}}, operation, result)}
  end

  defp record(state, operation, result) do
    entry = history_entry(state, operation, result)
    %{state | history: [entry | state.history], sequence: entry.sequence}
  end

  defp history_entry(state, operation, result) do
    %{
      sequence: Map.get(state, :sequence, 0) + 1,
      operation: operation,
      result: result,
      timestamp: System.system_time(:millisecond)
    }
  end
end

defmodule Cache.Manager do
  use GenServer

  def start_link(max_concurrency) do
    GenServer.start_link(__MODULE__, max_concurrency, name: __MODULE__)
  end

  @impl true
  def init(max_concurrency) do
    {:ok, %{max_concurrency: max_concurrency, active: %{}, queue: :queue.new()}}
  end

  def execute(operation) do
    GenServer.call(__MODULE__, {:execute, operation}, :infinity)
  end

  @impl true
  def handle_call({:execute, operation}, from, state) do
    state = %{state | queue: :queue.in({from, operation}, state.queue)}
    {:noreply, dispatch(state)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{from, operation}, active} ->
        Process.demonitor(ref, [:flush])

        case result do
          {:exit, reason} ->
            record_failure(operation, reason)
            GenServer.reply(from, {:error, reason})

          value ->
            GenServer.reply(from, value)
        end

        {:noreply, dispatch(%{state | active: active})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{from, operation}, active} ->
        record_failure(operation, reason)
        GenServer.reply(from, {:error, {:operation_failed, reason}})
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  defp dispatch(state) when map_size(state.active) >= state.max_concurrency, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, {from, operation}}, queue} ->
        state = %{state | queue: queue}

        try do
          task =
            Task.Supervisor.async_nolink(Cache.TaskSupervisor, fn ->
              Cache.Store.execute(operation)
            end)

          state = %{state | active: Map.put(state.active, task.ref, {from, operation})}
          dispatch(state)
        catch
          :exit, reason ->
            record_failure(operation, reason)
            GenServer.reply(from, {:error, {:operation_failed, reason}})
            dispatch(state)
        end
    end
  end

  defp record_failure(operation, reason) do
    Cache.Store.record_failure(operation, reason)
  catch
    :exit, _ -> :ok
  end
end
