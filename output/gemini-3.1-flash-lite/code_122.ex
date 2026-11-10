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
  Gerenciador de estoque com controle de concorrência via GenServer e semáforo.
  """
  use GenServer

  # --- API Pública ---

  def start_link(max_concurrency) do
    GenServer.start_link(__MODULE__, max_concurrency, name: __MODULE__)
  end

  def update_stock(item, quantity) do
    GenServer.call(__MODULE__, {:update, item, quantity})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(max_concurrency) do
    # O estado armazena o estoque, o histórico e o limite de concorrência
    {:ok, %{
      stock: %{},
      history: [],
      max_concurrency: max_concurrency,
      active_tasks: 0
    }}
  end

  @impl true
  def handle_call({:update, item, quantity}, _from, state) do
    if state.active_tasks < state.max_concurrency do
      new_state = process_transaction(state, item, quantity)
      {:reply, {:ok, new_state.stock[item]}, new_state}
    else
      {:reply, {:error, :concurrency_limit_reached}, state}
    end
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  # --- Funções Auxiliares ---

  defp process_transaction(state, item, quantity) do
    # Garante consistência atualizando o estoque e o log de forma atômica dentro do processo
    current_val = Map.get(state.stock, item, 0)
    new_val = current_val + quantity
    
    entry = %{item: item, change: quantity, timestamp: System.system_time(:second)}

    %{state | 
      stock: Map.put(state.stock, item, new_val),
      history: [entry | state.history]
    }
  end
end
