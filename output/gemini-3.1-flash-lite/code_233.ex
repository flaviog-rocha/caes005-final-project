# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Implementar expiração automática de registros.
# 3. Manter um histórico das operações realizadas.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de transações financeiras utilizando GenServer para gerenciar o estado,
  implementando idempotência, histórico e expiração de registros.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{transactions: %{}, history: []}, name: __MODULE__)
  end

  @doc "Executa uma transação garantindo idempotência e validade temporal."
  def process_transaction(id, amount, ttl_seconds \\ 3600) do
    GenServer.call(__MODULE__, {:execute, id, amount, ttl_seconds})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:execute, id, amount, ttl}, _from, state) do
    now = System.system_time(:second)
    
    # 1. Verifica se a operação já foi executada e se não expirou
    case Map.get(state.transactions, id) do
      nil ->
        # Nova transação
        expires_at = now + ttl
        new_tx = %{amount: amount, expires_at: expires_at}
        
        new_state = %{
          state |
          transactions: Map.put(state.transactions, id, new_tx),
          history: [%{id: id, amount: amount, timestamp: now} | state.history]
        }
        
        {:reply, {:ok, :processed}, new_state}

      %{expires_at: expires_at} when expires_at < now ->
        # Transação expirada: permite reprocessar
        new_tx = %{amount: amount, expires_at: now + ttl}
        new_state = %{
          state |
          transactions: Map.put(state.transactions, id, new_tx),
          history: [%{id: id, amount: amount, status: :reprocessed, timestamp: now} | state.history]
        }
        {:reply, {:ok, :reprocessed}, new_state}

      _ ->
        # Tentativa de duplicidade dentro do prazo de validade
        {:reply, {:error, :already_processed}, state}
    end
  end

  # --- Explicação de Integração (Conceitual) ---
  
  @doc """
  Exemplo de como seria a migração usando Ecto para persistência:
  
  defmodule MyApp.Repo.Migrations.CreateTransactions do
    use Ecto.Migration
    def change do
      create table(:transactions, primary_key: false) do
        add :id, :uuid, primary_key: true
        add :amount, :decimal
        add :expires_at, :utc_datetime
        timestamps()
      end
    end
  end
  """
end

# Exemplo de uso:
# {:ok, _pid} = FinancialSystem.start_link([])
# FinancialSystem.process_transaction("tx_123", 100.0) # {:ok, :processed}
# FinancialSystem.process_transaction("tx_123", 100.0) # {:error, :already_processed}
