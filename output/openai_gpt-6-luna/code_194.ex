# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DenseCache do
  use Supervisor

  @store DenseCache.Store
  @workers DenseCache.Workers
  @server DenseCache.Server

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    children = [
      {DenseCache.Store, []},
      {Task.Supervisor, name: @workers},
      {DenseCache.Server, max_concurrency: max_concurrency}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def get(key), do: operation({:get, key})
  def put(key, value), do: operation({:put, key, value})
  def delete(key), do: operation({:delete, key})
  def clear, do: operation(:clear)
  def history(limit \\ :all), do: operation({:history, limit})

  defp operation(operation) do
    GenServer.call(@server, {:operation, operation}, :infinity)
  end
end

defmodule DenseCache.Store do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{data: %{}, history: [], sequence: 0}, name: __MODULE__)
  end

  @impl true
  def init(state), do: {:ok, state}

  def execute(operation) do
    GenServer.call(__MODULE__, {:execute, operation}, :infinity)
  end

  @impl true
  def handle_call({:execute, {:get, key}}, _from, state) do
    result = Map.fetch(state.data, key)
    {:reply, result, record(state, {:get, key})}
  end

  def handle_call({:execute, {:put, key, value}}, _from, state) do
    state = %{state | data: Map.put(state.data, key, value)}
    {:reply, :ok, record(state, {:put, key, value})}
  end

  def handle_call({:execute, {:delete, key}}, _from, state) do
    state = %{state | data: Map.delete(state.data, key)}
    {:reply, :ok, record(state, {:delete, key})}
  end

  def handle_call({:execute, :clear}, _from, state) do
    state = %{state | data: %{}}
    {:reply, :ok, record(state, :clear)}
  end

  def handle_call({:execute, {:history, :all}}, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:execute, {:history, limit}}, _from, state)
      when is_integer(limit) and limit >= 0 do
    history =
      state.history
      |> Enum.take(limit)
      |> Enum.reverse()

    {:reply, history, state}
  end

  def handle_call({:execute, {:history, _limit}}, _from, state) do
    {:reply, {:error, :invalid_limit}, state}
  end

  defp record(state, operation) do
    entry = %{
      sequence: state.sequence + 1,
      operation: operation,
      at: System.system_time(:millisecond)
    }

    %{
      state
      | history: [entry | state.history],
        sequence: state.sequence + 1
    }
  end
end

defmodule DenseCache.Server do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       max_concurrency: Keyword.fetch!(opts, :max_concurrency),
       active: %{},
       queue: :queue.new()
     }}
  end

  @impl true
  def handle_call({:operation, operation}, from, state) do
    state = %{state | queue: :queue.in({operation, from}, state.queue)}
    {:noreply, dispatch(state)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{_from, _operation}, active} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(elem(Map.fetch!(state.active, ref), 0), result)
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{from, _operation}, active} ->
        GenServer.reply(from, {:error, {:worker_exit, reason}})
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    if map_size(state.active) < state.max_concurrency do
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {operation, from}}, queue} ->
          state = %{state | queue: queue}

          try do
            task =
              Task.Supervisor.async_nolink(DenseCache.Workers, fn ->
                DenseCache.Store.execute(operation)
              end)

            active = Map.put(state.active, task.ref, {from, operation})
            dispatch(%{state | active: active})
          catch
            kind, reason ->
              GenServer.reply(from, {:error, {:worker_start_failed, kind, reason}})
              dispatch(state)
          end
      end
    else
      state
    end
  end
end
