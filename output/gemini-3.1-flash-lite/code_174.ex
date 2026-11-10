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
  Agent para estado global e GenServer para log de transações.
  """

  use Agent

  # --- Interface Pública ---

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
    TransactionLogger.start_link([])
  end

  @doc "Processa um stream de operações sem carregar tudo em memória"
  def process_operations(stream) do
    stream
    |> Stream.each(fn op ->
      update_inventory(op)
      TransactionLogger.log(op)
    end)
    |> Stream.run()
  end

  def get_stock, do: Agent.get(__MODULE__, & &1)

  def get_history, do: TransactionLogger.get_history()

  # --- Privado ---

  defp update_inventory({sku, delta}) do
    Agent.update(__MODULE__, fn state ->
      Map.update(state, sku, delta, &(&1 + delta))
    end)
  end

  # --- Logger de Histórico ---

  defmodule TransactionLogger do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
    def log(op), do: GenServer.cast(__MODULE__, {:log, op})
    def get_history, do: GenServer.call(__MODULE__, :get)

    @impl true
    def init(_), do: {:ok, []}

    @impl true
    def handle_cast({:log, op}, state), do: {:noreply, [op | state]}

    @impl true
    def handle_call(:get, _from, state), do: {:reply, Enum.reverse(state), state}
  end
end

# --- Exemplo de Uso ---
# {:ok, _} = InventoryManager.start_link([])
#
# # Simulando grande volume de dados via Stream (sem carregar lista na memória)
# large_stream = Stream.map(1..1_000_000, fn i -> 
#   sku = "item_#{rem(i, 10)}"
#   {sku, if(rem(i, 2) == 0, do: 1, else: -1)}
# end)
#
# InventoryManager.process_operations(large_stream)
#
# IO.inspect(InventoryManager.get_stock())
# IO.inspect(InventoryManager.get_history())
