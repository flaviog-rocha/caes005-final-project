# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheManager do
  use GenServer

  @default_ttl 60_000
  @default_sweep_interval 30_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def put(entity, key, value, opts \\ []) do
    with {:ok, worker} <- worker_for(entity) do
      GenServer.call(worker, {:put, key, value, Keyword.get(opts, :ttl, :default)})
    end
  end

  def put_stream(entity, enumerable, opts \\ []) do
    with {:ok, worker} <- worker_for(entity) do
      ttl = Keyword.get(opts, :ttl, :default)

      Enum.reduce_while(enumerable, {:ok, 0}, fn {key, value}, {:ok, count} ->
        case GenServer.call(worker, {:put, key, value, ttl}) do
          :ok -> {:cont, {:ok, count + 1}}
          error -> {:halt, error}
        end
      end)
    end
  end

  def get(entity, key) do
    with {:ok, worker} <- worker_for(entity) do
      GenServer.call(worker, {:get, key})
    end
  end

  def delete(entity, key) do
    with {:ok, worker} <- worker_for(entity) do
      GenServer.call(worker, {:delete, key})
    end
  end

  def clear(entity) do
    with {:ok, worker} <- worker_for(entity) do
      GenServer.call(worker, :clear)
    end
  end

  def size(entity) do
    with {:ok, worker} <- worker_for(entity) do
      GenServer.call(worker, :size)
    end
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    default_ttl = Keyword.get(opts, :ttl, @default_ttl)
    sweep_interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)

    unless positive_integer?(default_ttl) and positive_integer?(sweep_interval) do
      raise ArgumentError, "ttl e sweep_interval devem ser inteiros positivos"
    end

    {:ok,
     %{
       workers: %{},
       refs: %{},
       default_ttl: default_ttl,
       sweep_interval: sweep_interval
     }}
  end

  @impl true
  def handle_call({:worker, entity}, _from, state) do
    case ensure_worker(entity, state) do
      {:ok, worker, next_state} -> {:reply, {:ok, worker}, next_state}
      {{:error, reason}, next_state} -> {:reply, {:error, reason}, next_state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, refs} ->
        {:noreply, %{state | refs: refs}}

      {entity, refs} ->
        workers =
          if Map.get(state.workers, entity) == pid do
            Map.delete(state.workers, entity)
          else
            state.workers
          end

        {:noreply, %{state | workers: workers, refs: refs}}
    end
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  defp worker_for(entity) do
    GenServer.call(__MODULE__, {:worker, entity})
  end

  defp ensure_worker(entity, state) do
    case Map.get(state.workers, entity) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {:ok, pid, state}
        else
          start_worker(entity, %{state | workers: Map.delete(state.workers, entity)})
        end

      nil ->
        start_worker(entity, state)
    end
  end

  defp start_worker(entity, state) do
    opts = [
      ttl: state.default_ttl,
      sweep_interval: state.sweep_interval
    ]

    case CacheManager.Entity.start_link(opts) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        next_state = %{
          state
          | workers: Map.put(state.workers, entity, pid),
            refs: Map.put(state.refs, ref, entity)
        }

        {:ok, pid, next_state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0
end

defmodule CacheManager.Entity do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    interval = Keyword.fetch!(opts, :sweep_interval)
    Process.send_after(self(), :expire_entries, interval)

    {:ok,
     %{
       table: :ets.new(:entity_cache, [:set, :protected, read_concurrency: true]),
       ttl: Keyword.fetch!(opts, :ttl),
       sweep_interval: interval
     }}
  end

  @impl true
  def handle_call({:put, key, value, requested_ttl}, _from, state) do
    ttl = if requested_ttl == :default, do: state.ttl, else: requested_ttl

    if is_integer(ttl) and ttl > 0 do
      expires_at = System.monotonic_time(:millisecond) + ttl
      true = :ets.insert(state.table, {key, value, expires_at})
      {:reply, :ok, state}
    else
      {:reply, {:error, :invalid_ttl}, state}
    end
  end

  def handle_call({:get, key}, _from, state) do
    reply =
      case :ets.lookup(state.table, key) do
        [{^key, value, expires_at}] ->
          if expires_at > System.monotonic_time(:millisecond) do
            {:ok, value}
          else
            :ets.delete(state.table, key)
            :error
          end

        [] ->
          :error
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, :ets.delete(state.table, key), state}
  end

  def handle_call(:clear, _from, state) do
    :ets.delete_all_objects(state.table)
    {:reply, :ok, state}
  end

  def handle_call(:size, _from, state) do
    {:reply, :ets.info(state.table, :size), state}
  end

  @impl true
  def handle_info(:expire_entries, state) do
    now = System.monotonic_time(:millisecond)

    :ets.select_delete(state.table, [
      {{:"$1", :"$2", :"$3"}, [{:"=<", :"$3", now}], [true]}
    ])

    Process.send_after(self(), :expire_entries, state.sweep_interval)
    {:noreply, state}
  end
end
