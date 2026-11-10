# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventorySystem do
  @moduledoc """
  Sistema de gerenciamento de estoque com processamento periódico.
  """

  # --- Tipos e Definições ---

  defmodule Item do
    defstruct id: nil, name: "", quantity: 0, min_threshold: 10
  end

  # Representação de erros conforme requisitos
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- Simulação de Banco de Dados ---

  defmodule Repository do
    @doc "Simula busca de registros no banco"
    def list_low_stock_items do
      # Em um cenário real com Ecto, seria: Repo.all(from i in Item, where: i.quantity < i.min_threshold)
      [
        %Item{id: 1, name: "Notebook", quantity: 2, min_threshold: 5},
        %Item{id: 5, name: "Mouse", quantity: 0, min_threshold: 10}
      ]
    end
  end

  # --- Lógica de Domínio ---

  defmodule InventoryManager do
    @doc "Processa o reabastecimento de um item"
    def restock(item, amount) when amount <= 0 do
      raise ValidationError, message: "A quantidade de reabastecimento deve ser positiva."
    end

    def restock(%Item{} = item, amount) do
      # Simula falha operacional (ex: erro de conexão com API de fornecedor)
      if :rand.uniform() > 0.9 do
        raise OperationalError, message: "Falha na conexão com o fornecedor para o item #{item.id}"
      end

      {:ok, %{item | quantity: item.quantity + amount}}
    end
  end

  # --- Agendamento e Execução Periódica ---

  defmodule Scheduler do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

    @impl true
    def init(state) do
      schedule_work()
      {:ok, state}
    end

    @impl true
    def handle_info(:work, state) do
      perform_inventory_check()
      schedule_work()
      {:noreply, state}
    end

    defp schedule_work, do: Process.send_after(self(), :work, :timer.seconds(10))

    defp perform_inventory_check do
      IO.puts("[#{DateTime.utc_now()}] Iniciando checagem de estoque...")
      
      Repository.list_low_stock_items()
      |> Enum.each(fn item ->
        # Processamento independente por entidade
        Task.start(fn -> 
          try do
            InventoryManager.restock(item, 20)
            IO.puts("Sucesso: Item #{item.name} reabastecido.")
          rescue
            e in ValidationError -> IO.puts("Erro de Validação: #{e.message}")
            e in OperationalError -> IO.puts("Falha Operacional: #{e.message}. Tentaremos novamente mais tarde.")
          end
        end)
      end)
    end
  end
end

# Inicialização do supervisor simulada
InventorySystem.Scheduler.start_link([])
