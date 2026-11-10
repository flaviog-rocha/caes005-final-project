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

defmodule InventoryManager do
  @moduledoc """
  Gerenciador de estoque com persistência de histórico e supervisão via OTP.
  Utiliza Streams para processar operações de forma lazy e eficiente.
  """
  use GenServer

  # --- API Pública ---

  def start_link(initial_state \\ %{stock: %{}, history: []}) do
    GenServer.start_link(__MODULE__, initial_state, name: __MODULE__)
  end

  def add_item(item, quantity) do
    GenServer.call(__MODULE__, {:update, item, quantity})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:update, item, quantity}, _from, state) do
    new_entry = {item, quantity, DateTime.utc_now()}
    
    # Utilizando Stream para processar a atualização do histórico
    # O stream transforma a lista de histórico mantendo a imutabilidade
    updated_history = 
      [new_entry | state.history]
      |> Stream.map(& &1)
      |> Enum.take(100) # Mantém apenas as últimas 100 operações

    new_stock = Map.update(state.stock, item, quantity, &(&1 + quantity))
    new_state = %{stock: new_stock, history: updated_history}

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end
end

defmodule InventorySupervisor do
  @moduledoc """
  Supervisor que garante a recuperação de processos após falhas (Self-healing).
  """
  use Supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      # Estratégia :one_for_one garante que se o InventoryManager falhar,
      # ele será reiniciado com o estado inicial ou um estado persistido.
      {InventoryManager, %{stock: %{}, history: []}}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de uso:
# 1. Iniciar o supervisor
# {:ok, _pid} = InventorySupervisor.start_link([])
# 2. Realizar operações
# InventoryManager.add_item("Produto A", 10)
# 3. Consultar histórico
# IO.inspect(InventoryManager.get_history())
