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

defmodule Shop do
  @moduledoc false

  def start_link do
    Shop.Supervisor.start_link()
  end

  def receive_stock(sku, quantity) do
    Shop.Inventory.adjust(sku, quantity)
  end

  def sell_stock(sku, quantity) do
    Shop.Inventory.adjust(sku, -quantity)
  end

  def stock(sku) do
    Shop.Inventory.stock(sku)
  end

  def history do
    history_stream() |> Enum.to_list()
  end

  def history_stream(sku \\ nil) do
    Shop.Data.history()
    |> Stream.filter(fn event -> is_nil(sku) or event.sku == sku end)
  end
end

defmodule Shop.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {Shop.Data, []},
      {Shop.Inventory, []}
    ]

    # O supervisor detecta a saída de processos filhos e reinicia os permanentes.
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Shop.Data do
  @moduledoc false
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def adjust(sku, quantity) do
    GenServer.call(__MODULE__, {:adjust, sku, quantity})
  end

  def stock(sku) do
    GenServer.call(__MODULE__, {:stock, sku})
  end

  def history do
    GenServer.call(__MODULE__, :history)
  end

  @impl true
  def init(:ok) do
    {:ok, %{inventory: %{}, history: [], sequence: 0}}
  end

  @impl true
  def handle_call({:adjust, _sku, quantity}, _from, state)
      when not is_integer(quantity) or quantity == 0 do
    {:reply, {:error, :invalid_quantity}, state}
  end

  def handle_call({:adjust, sku, change}, _from, state) do
    current = Map.get(state.inventory, sku, 0)
    updated = current + change

    if updated < 0 do
      {:reply, {:error, :insufficient_stock}, state}
    else
      sequence = state.sequence + 1

      event = %{
        id: sequence,
        sku: sku,
        change: change,
        quantity: updated,
        operation: if(change > 0, do: :received, else: :sold),
        at: DateTime.utc_now()
      }

      next_state = %{
        state
        | inventory: Map.put(state.inventory, sku, updated),
          history: [event | state.history],
          sequence: sequence
      }

      {:reply, {:ok, updated}, next_state}
    end
  end

  def handle_call({:stock, sku}, _from, state) do
    {:reply, Map.get(state.inventory, sku, 0), state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end
end

defmodule Shop.Inventory do
  @moduledoc false
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def adjust(sku, quantity) do
    GenServer.call(__MODULE__, {:adjust, sku, quantity})
  end

  def stock(sku) do
    GenServer.call(__MODULE__, {:stock, sku})
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call({:adjust, sku, quantity}, _from, state) do
    {:reply, Shop.Data.adjust(sku, quantity), state}
  end

  def handle_call({:stock, sku}, _from, state) do
    {:reply, Shop.Data.stock(sku), state}
  end
end
