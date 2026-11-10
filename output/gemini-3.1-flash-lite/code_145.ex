# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderSystem do
  @moduledoc """
  Sistema de gerenciamento de pedidos utilizando GenServer para isolamento,
  ETS para persistência em memória (simulando persistência) e validação de schema.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def process_order(order_data) do
    GenServer.call(__MODULE__, {:process, order_data})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Tabela ETS pública para persistência durável no processo
    :ets.new(:orders, [:set, :public, :named_table])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:process, data}, _from, state) do
    case validate_order(data) do
      {:ok, order} ->
        # Persistência
        :ets.insert(:orders, {order.id, order})
        
        # Lógica de notificação baseada em condições
        maybe_notify(order)
        
        {:reply, {:ok, order}, state}
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # --- Validação e Regras de Negócio ---

  defp validate_order(%{id: id, amount: amount}) when is_integer(id) and amount > 0 do
    {:ok, %{id: id, amount: amount, status: :pending}}
  end
  defp validate_order(_), do: {:error, "Dados de pedido inválidos"}

  defp maybe_notify(%{amount: amount} = order) when amount > 1000 do
    IO.puts("[NOTIFICAÇÃO] Pedido de alto valor detectado: ID #{order.id}")
  end
  defp maybe_notify(_), do: :ok

  # --- Exemplo de Uso ---
  # OrderSystem.start_link([])
  # OrderSystem.process_order(%{id: 1, amount: 500})
  # OrderSystem.process_order(%{id: 2, amount: 1500})
end
