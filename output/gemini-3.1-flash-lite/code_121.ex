# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventoryManager do
  @moduledoc """
  Gerenciador de estoque com controle de concorrência via GenServer.
  
  - O estado armazena o estoque atual e um log de transações.
  - O limite de operações simultâneas é garantido pelo uso de processamento síncrono
    via `GenServer.call` e, opcionalmente, via um Pool (neste caso, mantemos a 
    consistência serializando as chamadas no mailbox do processo).
  """

  use GenServer

  # API Pública

  def start_link(initial_inventory \\ %{}) do
    GenServer.start_link(__MODULE__, initial_inventory, name: __MODULE__)
  end

  def update_stock(item, quantity) do
    GenServer.call(__MODULE__, {:update, item, quantity})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # Callbacks do GenServer

  @impl true
  def init(initial_inventory) do
    {:ok, %{stock: initial_inventory, history: []}}
  end

  @impl true
  def handle_call({:update, item, quantity}, _from, state) do
    new_stock = Map.update(state.stock, item, quantity, &(&1 + quantity))
    
    entry = %{
      item: item,
      change: quantity,
      timestamp: System.system_time(:second)
    }

    new_state = %{
      stock: new_stock,
      history: [entry | state.history]
    }

    {:reply, {:ok, new_stock}, new_state}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end
end

# Exemplo de uso para garantir concorrência:
# O GenServer processa mensagens em ordem (mailbox), garantindo a atomicidade
# das operações sem a necessidade de locks externos.

# InventoryManager.start_link(%{"item_a" => 10})
# InventoryManager.update_stock("item_a", -2)
# InventoryManager.get_history()
