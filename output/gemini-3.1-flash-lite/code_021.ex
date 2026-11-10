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
  Gerenciador de pedidos utilizando DETS para persistência.
  - Concorrência: Acesso via GenServer.
  - Erros: Distinção entre :validation_error e :system_error.
  - Persistência: DETS (Disk Based Term Storage).
  """

  use GenServer

  @table_name :orders_table

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def create_order(id, items) when is_list(items) and items != [] do
    GenServer.call(__MODULE__, {:create, id, items})
  end

  def create_order(_, _), do: {:error, :validation_error, "Items cannot be empty"}

  def get_order(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(:ok) do
    # Abre ou cria o arquivo de persistência
    {:ok, table} = :dets.open_file(@table_name, [type: :set, file: ~c"orders.dets"])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:create, id, items}, _from, state) do
    case :dets.insert_new(state.table, {id, items, DateTime.utc_now()}) do
      true ->
        :dets.sync(state.table)
        {:reply, {:ok, id}, state}

      false ->
        # Falha operacional: ID já existe
        {:reply, {:error, :system_error, "Order ID already exists"}, state}

      {:error, reason} ->
        # Falha operacional: Problema de I/O ou corrupção
        {:reply, {:error, :system_error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    case :dets.lookup(state.table, id) do
      [{^id, items, timestamp}] ->
        {:reply, {:ok, %{id: id, items: items, created_at: timestamp}}, state}

      [] ->
        {:reply, {:error, :validation_error, "Order not found"}, state}

      {:error, reason} ->
        {:reply, {:error, :system_error, reason}, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end
end
