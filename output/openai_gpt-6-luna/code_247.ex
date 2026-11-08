# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule StreamingCache do
  use Supervisor

  @registry StreamingCache.Registry
  @entity_supervisor StreamingCache.EntitySupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @entity_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Consome o enumerable em lotes limitados; os dados não são materializados integralmente.
  def put_stream(entity, enumerable, opts \\ []) do
    with {:ok, pid} <- ensure_entity(entity, opts) do
      batch_size = positive_integer(Keyword.get(opts, :batch_size, 100), 100)

      enumerable
      |> Stream.chunk_every(batch_size)
      |> Enum.reduce_while({:ok, 0}, fn batch, {:ok, total} ->
        case GenServer.call(pid, {:put_many, batch}, :infinity) do
          {:ok, count} -> {:cont, {:ok, total + count}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  def fetch(entity, key, opts \\ []) do
    with {:ok, pid} <- ensure_entity(entity, opts) do
      GenServer.call(pid, {:fetch, key})
    end
  end

  def delete(entity, key, opts \\ []) do
    with {:ok, pid} <- ensure_entity(entity, opts) do
      GenServer.call(pid, {:delete, key})
    end
  end

  def stats(entity, opts \\ []) do
    with {:ok, pid} <- ensure_entity(entity, opts) do
      GenServer.call(pid, :stats)
    end
  end

  def stop_entity(entity) do
    case Registry.lookup(@registry, entity) do
      [{pid, _value}] -> DynamicSupervisor.terminate_child(@entity_supervisor, pid)
      [] -> :ok
    end
  end

  defp ensure_entity(entity, opts) do
    case Registry.lookup(@registry, entity) do
      [{pid, _value}] ->
        {:ok, pid}

      [] ->
        result =
          DynamicSupervisor.start_child(
            @entity_supervisor,
            {StreamingCache.Entity, {entity, opts}}
          )

        case result do
          {:ok, pid} ->
            {:ok, pid}

          {:error, reason} ->
            case Registry.lookup(@registry, entity) do
              [{pid, _value}] -> {:ok, pid}
              [] -> {:error, reason}
            end
        end
    end
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default
end

defmodule StreamingCache.Entity do
  use GenServer

  @registry StreamingCache.Registry

  def start_link({entity, opts}) do
    GenServer.start_link(__MODULE__, opts, name: via(entity))
  end

  @impl true
  def init(opts) do
    capacity = positive_integer(Keyword.get(opts, :capacity, 1_000), 1_000)
    ttl = normalize_ttl(Keyword.get(opts, :ttl, 60_000))
    sweep_interval = positive_integer(Keyword.get(opts, :sweep_interval, 10_000), 10_000)

    schedule_sweep(sweep_interval)

    {:ok,
     %{
       entries: %{},
       capacity: capacity,
       ttl: ttl,
       sweep_interval: sweep_interval,
       clock: 0
     }}
  end

  @impl true
  def handle_call({:put_many, batch}, _from, state) do
    if Enum.all?(batch, &valid_entry?/1) do
      now = monotonic_time()

      updated_state =
        Enum.reduce(batch, state, fn {key, value}, acc ->
          put_entry(acc, key, value, now)
        end)

      {:reply, {:ok, length(batch)}, updated_state}
    else
      {:reply, {:error, :entries_must_be_key_value_pairs}, state}
    end
  end

  def handle_call({:fetch, key}, _from, state) do
    now = monotonic_time()

    case Map.fetch(state.entries, key) do
      :error ->
        {:reply, :miss, state}

      {:ok, %{expires_at: expires_at}} when is_integer(expires_at) and expires_at <= now ->
        {:reply, :miss, %{state | entries: Map.delete(state.entries, key)}}

      {:ok, entry} ->
        clock = state.clock + 1
        updated_entry = %{entry | touched: clock}
        entries = Map.put(state.entries, key, updated_entry)

        {:reply, {:ok, entry.value}, %{state | entries: entries, clock: clock}}
    end
  end

  def handle_call({:delete, key}, _from, state) do
    {entry, entries} = Map.pop(state.entries, key)
    reply = if is_nil(entry), do: :miss, else: :ok
    {:reply, reply, %{state | entries: entries}}
  end

  def handle_call(:stats, _from, state) do
    {:reply, %{size: map_size(state.entries), capacity: state.capacity}, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = monotonic_time()

    entries =
      Map.reject(state.entries, fn {_key, entry} ->
        is_integer(entry.expires_at) and entry.expires_at <= now
      end)

    schedule_sweep(state.sweep_interval)
    {:noreply, %{state | entries: entries}}
  end

  defp put_entry(state, key, value, now) do
    state =
      if not Map.has_key?(state.entries, key) and map_size(state.entries) >= state.capacity do
        evict_least_recently_used(state)
      else
        state
      end

    clock = state.clock + 1
    expires_at = if state.ttl == :infinity, do: :infinity, else: now + state.ttl

    entry = %{value: value, expires_at: expires_at, touched: clock}

    %{
      state
      | entries: Map.put(state.entries, key, entry),
        clock: clock
    }
  end

  defp evict_least_recently_used(state) when map_size(state.entries) == 0, do: state

  defp evict_least_recently_used(state) do
    {key, _entry} = Enum.min_by(state.entries, fn {_key, entry} -> entry.touched end)
    %{state | entries: Map.delete(state.entries, key)}
  end

  defp valid_entry?({_, _}), do: true
  defp valid_entry?(_), do: false

  defp via(entity), do: {:via, Registry, {@registry, entity}}

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)
  defp monotonic_time, do: System.monotonic_time(:millisecond)

  defp normalize_ttl(:infinity), do: :infinity
  defp normalize_ttl(ttl) when is_integer(ttl) and ttl > 0, do: ttl
  defp normalize_ttl(_ttl), do: 60_000

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default
end
