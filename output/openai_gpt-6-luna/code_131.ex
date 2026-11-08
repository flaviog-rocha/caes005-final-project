# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Store.Inventory.MemoryDB do
  @moduledoc false
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def fail_next(pid, count) when is_integer(count) and count >= 0 do
    GenServer.call(pid, {:fail_next, count})
  end

  def sync(pid, inventory) do
    GenServer.call(pid, {:sync, inventory})
  end

  def last_snapshot(pid) do
    GenServer.call(pid, :last_snapshot)
  end

  @impl true
  def init(opts) do
    {:ok, %{failures_left: Keyword.get(opts, :failures, 0), snapshot: %{}}}
  end

  @impl true
  def handle_call({:fail_next, count}, _from, state) do
    {:reply, :ok, %{state | failures_left: count}}
  end

  def handle_call({:sync, _inventory}, _from, %{failures_left: count} = state)
      when count > 0 do
    {:reply, {:error, {:temporary, :simulated_failure}},
     %{state | failures_left: count - 1}}
  end

  def handle_call({:sync, inventory}, _from, state) do
    {:reply, :ok, %{state | snapshot: inventory}}
  end

  def handle_call(:last_snapshot, _from, state) do
    {:reply, state.snapshot, state}
  end
end

defmodule Store.Inventory do
  @moduledoc """
  Gerenciamento de estoque em memória, com sincronização periódica para um banco
  de dados fictício e novas tentativas após falhas temporárias.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def add_stock(server, sku, name, quantity) do
    GenServer.call(server, {:add_stock, sku, name, quantity})
  end

  def sell(server, sku, quantity) do
    GenServer.call(server, {:sell, sku, quantity})
  end

  def inventory(server) do
    GenServer.call(server, :inventory)
  end

  def database(server) do
    GenServer.call(server, :database)
  end

  @impl true
  def init(opts) do
    {database, owns_database?} =
      case Keyword.get(opts, :db) do
        nil ->
          {:ok, database} =
            Store.Inventory.MemoryDB.start_link(failures: Keyword.get(opts, :db_failures, 0))

          Process.unlink(database)
          {database, true}

        database ->
          {database, false}
      end

    state = %{
      items: Keyword.get(opts, :items, %{}),
      database: database,
      owns_database?: owns_database?,
      interval: Keyword.get(opts, :interval, 60_000),
      retry_delay: Keyword.get(opts, :retry_delay, 1_000),
      max_retries: Keyword.get(opts, :max_retries, 3),
      cycle: nil,
      next_cycle_id: 0,
      timer: nil
    }

    {:ok, schedule_periodic(state)}
  end

  @impl true
  def handle_call({:add_stock, sku, name, quantity}, _from, state)
      when is_binary(sku) and is_binary(name) and is_integer(quantity) and quantity > 0 do
    case Map.fetch(state.items, sku) do
      :error ->
        item = %{name: name, quantity: quantity}
        {:reply, :ok, %{state | items: Map.put(state.items, sku, item)}}

      {:ok, %{name: ^name, quantity: current} = item} ->
        updated = %{item | quantity: current + quantity}
        {:reply, :ok, %{state | items: Map.put(state.items, sku, updated)}}

      {:ok, _item} ->
        {:reply, {:error, :name_mismatch}, state}
    end
  end

  def handle_call({:add_stock, _sku, _name, _quantity}, _from, state) do
    {:reply, {:error, :invalid_arguments}, state}
  end

  def handle_call({:sell, sku, quantity}, _from, state)
      when is_binary(sku) and is_integer(quantity) and quantity > 0 do
    case Map.fetch(state.items, sku) do
      :error ->
        {:reply, {:error, :unknown_sku}, state}

      {:ok, %{quantity: current} = item} when current < quantity ->
        {:reply, {:error, :insufficient_stock}, state}

      {:ok, %{quantity: current} = item} ->
        remaining = current - quantity
        updated = %{item | quantity: remaining}

        {:reply, {:ok, remaining},
         %{state | items: Map.put(state.items, sku, updated)}}
    end
  end

  def handle_call({:sell, _sku, _quantity}, _from, state) do
    {:reply, {:error, :invalid_arguments}, state}
  end

  def handle_call(:inventory, _from, state) do
    {:reply, state.items, state}
  end

  def handle_call(:database, _from, state) do
    {:reply, state.database, state}
  end

  @impl true
  def handle_info(:periodic, state) do
    state = schedule_periodic(state)

    if is_nil(state.cycle) do
      cycle_id = state.next_cycle_id + 1
      state = %{state | next_cycle_id: cycle_id, cycle: cycle_id}
      {:noreply, execute_cycle(state, cycle_id, 0)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:retry, cycle_id, retry_number}, %{cycle: cycle_id} = state) do
    {:noreply, execute_cycle(state, cycle_id, retry_number)}
  end

  def handle_info({:retry, _cycle_id, _retry_number}, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    if state.timer, do: Process.cancel_timer(state.timer)

    if state.owns_database? do
      try do
        GenServer.stop(state.database)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  defp execute_cycle(state, cycle_id, retry_number) do
    case safe_sync(state.database, state.items) do
      :ok ->
        %{state | cycle: nil}

      {:error, reason} = error ->
        if temporary_failure?(error) and retry_number < state.max_retries do
          Process.send_after(
            self(),
            {:retry, cycle_id, retry_number + 1},
            state.retry_delay
          )

          state
        else
          _ = reason
          %{state | cycle: nil}
        end
    end
  end

  defp safe_sync(database, items) do
    try do
      Store.Inventory.MemoryDB.sync(database, items)
    rescue
      _ -> {:error, :temporary}
    catch
      :exit, _ -> {:error, :temporary}
    end
  end

  defp temporary_failure?({:error, :temporary}), do: true
  defp temporary_failure?({:error, {:temporary, _reason}}), do: true
  defp temporary_failure?(_), do: false

  defp schedule_periodic(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    timer = Process.send_after(self(), :periodic, state.interval)
    %{state | timer: timer}
  end
end
