# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderManager do
  @moduledoc """
  Gerenciador de pedidos utilizando DETS para persistência e GenServer para concorrência.
  """
  use GenServer

  @table_name :orders_table

  # --- Definição de Erros ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc "Cria um novo pedido. Valida campos básicos."
  def create_order(id, items) when is_list(items) and length(items) > 0 do
    GenServer.call(__MODULE__, {:create, id, items})
  end
  def create_order(_, _), do: {:error, %ValidationError{message: "Pedido inválido: itens vazios"}}

  @doc "Recupera um pedido do DETS."
  def get_order(id) do
    case :dets.lookup(@table_name, id) do
      [{^id, order}] -> {:ok, order}
      [] -> {:error, %OperationalError{message: "Pedido não encontrado"}}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    case :dets.open_file(@table_name, [type: :set, file: 'orders.dets']) do
      {:ok, _} -> {:ok, nil}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, id, items}, _from, state) do
    case :dets.lookup(@table_name, id) do
      [{^id, _}] ->
        {:reply, {:error, %ValidationError{message: "ID de pedido já existe"}}, state}

      [] ->
        order = %{id: id, items: items, status: :pending, timestamp: System.system_time(:second)}
        
        case :dets.insert(@table_name, {id, order}) do
          :ok -> {:reply, {:ok, order}, state}
          {:error, reason} -> {:reply, {:error, %OperationalError{message: "Falha na persistência: #{inspect(reason)}"}}, state}
        end
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table_name)
    :ok
  end
end

# --- Exemplo de Uso ---
# {:ok, _} = OrderManager.start_link(nil)
# OrderManager.create_order("ORD-001", ["Item A", "Item B"])
# OrderManager.get_order("ORD-001")
