# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventoryManager do
  @moduledoc """
  Sistema de gerenciamento de estoque com processamento periódico e tratamento de erros.
  """

  # Definição de tipos para clareza no domínio
  @type item :: %{id: String.t(), quantity: integer(), name: String.t()}
  @type error_type :: :validation_error | :operational_failure

  defmodule StockError do
    defexception [:message, :type]
  end

  # --- Simulação de Banco de Dados ---
  defp fetch_items_from_db do
    # Simula busca de itens no banco de dados
    [%{id: "1", quantity: 10, name: "Teclado"}, %{id: "2", quantity: 0, name: "Mouse"}]
  end

  # --- Lógica de Domínio ---

  @doc "Validação de negócio: Verifica se o item possui estoque"
  def validate_stock(%{quantity: q} = item) when q > 0, do: {:ok, item}
  def validate_stock(item), do: {:error, %StockError{message: "Sem estoque para #{item.name}", type: :validation_error}}

  @doc "Operação de escrita simulada"
  def sync_to_database(item) do
    # Simula falha operacional aleatória (ex: queda de conexão)
    if :rand.uniform(10) > 8 do
      {:error, %StockError{message: "Falha na conexão com DB", type: :operational_failure}}
    else
      IO.puts("Item #{item.name} sincronizado com sucesso.")
      :ok
    end
  end

  # --- Processamento ---

  def process_item(item) do
    # Processa entidades independentemente usando o pipe operator
    with {:ok, valid_item} <- validate_stock(item),
         :ok <- sync_to_database(valid_item) do
      {:ok, valid_item.id}
    else
      {:error, %StockError{type: :validation_error} = err} ->
        IO.puts("Erro de Validação: #{err.message}")
        {:error, :validation}

      {:error, %StockError{type: :operational_failure} = err} ->
        IO.puts("Falha Operacional: #{err.message}. Reagendando...")
        {:error, :operational}
    end
  end

  # --- Execução Periódica (GenServer simplificado) ---

  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state) do
    schedule_work()
    {:ok, state}
  end

  @impl true
  def handle_info(:work, state) do
    IO.puts("--- Iniciando ciclo de inventário ---")
    
    fetch_items_from_db()
    |> Enum.map(&Task.async(fn -> process_item(&1) end))
    |> Enum.map(&Task.await/1)

    schedule_work()
    {:noreply, state}
  end

  defp schedule_work do
    # Executa a cada 5 segundos
    Process.send_after(self(), :work, 5_000)
  end
end

# Para rodar:
# InventoryManager.start_link([])
