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
  Gerenciador de estoque resiliente utilizando GenServer e Supervisor.
  O histórico é mantido em uma lista processada via Stream para consumo eficiente.
  """
  use GenServer

  # API Pública
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def add_item(item, qty), do: GenServer.cast(__MODULE__, {:add, item, qty})

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # Callbacks do GenServer
  @impl true
  def init(_) do
    # Estado inicial: {estoque_map, lista_historico}
    {:ok, {%{}, []}}
  end

  @impl true
  def handle_cast({:add, item, qty}, {stock, history}) do
    new_stock = Map.update(stock, item, qty, &(&1 + qty))
    new_history = ["Added #{qty} of #{item}" | history]
    {:noreply, {new_stock, new_history}}
  end

  @impl true
  def handle_call(:get_history, _from, {stock, history}) do
    # Uso de Stream para processar o histórico
    processed_history = 
      history
      |> Stream.map(&String.upcase/1)
      |> Enum.to_list()

    {:reply, processed_history, {stock, history}}
  end
end

defmodule InventorySupervisor do
  @moduledoc """
  Supervisor que garante a recuperação do processo após falhas.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {InventoryManager, []}
    ]

    # Estratégia :one_for_one garante que se o InventoryManager falhar,
    # ele será reiniciado automaticamente.
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de uso:
# 1. Iniciar o supervisor
# InventorySupervisor.start_link([])

# 2. Executar operações
# InventoryManager.add_item("Café", 10)
# InventoryManager.add_item("Açúcar", 5)

# 3. Recuperar histórico via Stream
# IO.inspect(InventoryManager.get_history())

# 4. Simular falha inesperada
# Process.exit(Process.whereis(InventoryManager), :kill)
# O Supervisor reiniciará o processo automaticamente.
