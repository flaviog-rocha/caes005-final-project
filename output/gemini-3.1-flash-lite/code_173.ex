# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventoryManager do
  @moduledoc """
  Gerenciador de estoque usando Stream para processamento lazy (memória eficiente),
  Agent para estado em tempo real e GenServer para persistência de histórico.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{history: []}, name: __MODULE__)
  end

  @doc "Processa um stream de operações sem carregar tudo na memória"
  def process_stream(stream) do
    stream
    |> Stream.map(&execute_operation/1)
    |> Stream.run()
  end

  @doc "Consulta o estado atual de um produto"
  def get_stock(product_id) do
    Agent.get(:stock_state, &Map.get(&1, product_id, 0))
  end

  @doc "Consulta o histórico de operações"
  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks e Lógica Interna ---

  def init(state) do
    Agent.start_link(fn -> %{} end, name: :stock_state)
    {:ok, state}
  end

  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  def handle_cast({:log_op, op}, state) do
    {:noreply, %{state | history: [op | state.history]}}
  end

  defp execute_operation({product_id, amount} = op) do
    Agent.update(:stock_state, fn state ->
      Map.update(state, product_id, amount, &(&1 + amount))
    end)

    GenServer.cast(__MODULE__, {:log_op, op})
  end
end

# --- Exemplo de Uso ---

# Inicializa o processo
InventoryManager.start_link(nil)

# Simulação de processamento de um volume gigante de dados
# O Stream.map garante que o processamento ocorra item a item
large_data =
  Stream.iterate(1, &(&1 + 1))
  |> Stream.take(1_000_000)
  |> Stream.map(fn i -> {:product_a, 1} end)

InventoryManager.process_stream(large_data)

# Consultas
IO.inspect(InventoryManager.get_stock(:product_a), label: "Estoque Final")
IO.inspect(Enum.take(InventoryManager.get_history(), 5), label: "Últimas 5 operações")
