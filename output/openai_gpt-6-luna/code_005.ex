# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Store do
  def start_link(opts \\ []) do
    Store.Supervisor.start_link(opts)
  end

  def add_stock(sku, quantity), do: Store.Inventory.add_stock(sku, quantity)
  def remove_stock(sku, quantity), do: Store.Inventory.remove_stock(sku, quantity)
  def stock(sku), do: Store.Inventory.stock(sku)
  def all_stock, do: Store.Inventory.all_stock()
  def history(type \\ :all), do: Store.History.events(type)
end

defmodule Store.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Store.History,
      Store.Inventory,
      Store.ProcessMonitor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Store.History do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  def record(event), do: GenServer.call(__MODULE__, {:record, event})

  def events(type \\ :all) do
    GenServer.call(__MODULE__, {:events, type})
  end

  @impl true
  def init(:ok), do: {:ok, []}

  @impl true
  def handle_call({:record, event}, _from, events) do
    {:reply, :ok, [event | events]}
  end

  def handle_call({:events, type}, _from, events) do
    result =
      events
      |> Enum.reverse()
      |> Stream.filter(fn event -> type == :all or event.type == type end)
      |> Enum.to_list()

    {:reply, result, events}
  end
end

defmodule Store.Inventory do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  def add_stock(sku, quantity), do: GenServer.call(__MODULE__, {:add, sku, quantity})
  def remove_stock(sku, quantity), do: GenServer.call(__MODULE__, {:remove, sku, quantity})
  def stock(sku), do: GenServer.call(__MODULE__, {:stock, sku})
  def all_stock, do: GenServer.call(__MODULE__, :all_stock)

  @impl true
  def init(:ok) do
    stock =
      Store.History.events()
      |> Stream.reduce(%{}, &replay_event/2)

    {:ok, stock}
  end

  @impl true
  def handle_call({:add, sku, quantity}, _from, stock)
      when not is_nil(sku) and is_integer(quantity) and quantity > 0 do
    event = operation_event(:stock_added, sku, quantity)
    :ok = Store.History.record(event)
    {:reply, :ok, Map.update(stock, sku, quantity, &(&1 + quantity))}
  end

  def handle_call({:remove, sku, quantity}, _from, stock)
      when not is_nil(sku) and is_integer(quantity) and quantity > 0 do
    current = Map.get(stock, sku, 0)

    if current >= quantity do
      event = operation_event(:stock_removed, sku, quantity)
      :ok = Store.History.record(event)

      updated =
        if current == quantity do
          Map.delete(stock, sku)
        else
          Map.put(stock, sku, current - quantity)
        end

      {:reply, :ok, updated}
    else
      {:reply, {:error, :insufficient_stock}, stock}
    end
  end

  def handle_call({action, _sku, _quantity}, _from, stock)
      when action in [:add, :remove] do
    {:reply, {:error, :invalid_arguments}, stock}
  end

  def handle_call({:stock, sku}, _from, stock) do
    {:reply, Map.get(stock, sku, 0), stock}
  end

  def handle_call(:all_stock, _from, stock) do
    {:reply, stock, stock}
  end

  defp operation_event(type, sku, quantity) do
    %{
      type: type,
      sku: sku,
      quantity: quantity,
      at: System.system_time(:millisecond)
    }
  end

  defp replay_event(%{type: :stock_added, sku: sku, quantity: quantity}, stock) do
    Map.update(stock, sku, quantity, &(&1 + quantity))
  end

  defp replay_event(%{type: :stock_removed, sku: sku, quantity: quantity}, stock) do
    current = Map.get(stock, sku, 0)

    if current <= quantity do
      Map.delete(stock, sku)
    else
      Map.put(stock, sku, current - quantity)
    end
  end

  defp replay_event(_event, stock), do: stock
end

defmodule Store.ProcessMonitor do
  use GenServer

  @watched_processes [Store.History, Store.Inventory]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    send(self(), :check_processes)
    {:ok, %{refs: %{}, watched: %{}}}
  end

  @impl true
  def handle_info(:check_processes, state) do
    watched =
      Enum.reduce(@watched_processes, state.watched, fn module, acc ->
        case {Map.has_key?(acc, module), Process.whereis(module)} do
          {false, pid} when is_pid(pid) ->
            ref = Process.monitor(pid)
            Map.put(acc, module, {pid, ref})

          _ ->
            acc
        end
      end)

    {:noreply, %{state | watched: watched, refs: refs_from_watched(watched)}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.refs, ref) do
      {module, ^pid} ->
        watched = Map.delete(state.watched, module)
        refs = Map.delete(state.refs, ref)

        unless reason in [:normal, :shutdown] do
          send(self(), {:record_down, module, reason})
        end

        Process.send_after(self(), :check_processes, 25)
        {:noreply, %{state | watched: watched, refs: refs}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:record_down, module, reason}, state) do
    event = %{
      type: :process_down,
      process: module,
      reason: inspect(reason),
      at: System.system_time(:millisecond)
    }

    try do
      Store.History.record(event)
    catch
      :exit, _ -> Process.send_after(self(), {:record_down, module, reason}, 100)
    end

    {:noreply, state}
  end

  defp refs_from_watched(watched) do
    Map.new(watched, fn {module, {pid, ref}} -> {ref, {module, pid}} end)
  end
end
