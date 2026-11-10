# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir comunicação entre diferentes processos.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem.TransactionManager do
  @moduledoc """
  Gerenciador de transações com persistência, supervisão e mensageria.
  """
  use GenServer
  require Logger

  @db_file "transactions.ets"

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def process_transaction(account_id, amount) do
    GenServer.call(__MODULE__, {:process, account_id, amount})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # 1. Persistência usando ETS com salvamento em disco
    table = :ets.file2tab(String.to_charlist(@db_file))
    case table do
      {:ok, tid} -> {:ok, %{table: tid}}
      _ -> {:ok, %{table: :ets.new(:transactions, [:set, :public, :named_table])}}
    end
  end

  @impl true
  def handle_call({:process, account_id, amount}, _from, state) do
    # 4. Persistência
    :ets.insert(state.table, {{account_id, System.system_time()}, amount})
    :ets.tab2file(state.table, String.to_charlist(@db_file))

    # 3. Notificação baseada em condição
    if amount > 10000 do
      notify_compliance(account_id, amount)
    end

    {:reply, :ok, state}
  end

  # 2. Comunicação entre processos (exemplo de broadcast)
  defp notify_compliance(account_id, amount) do
    Logger.warn("Alerta de transação alta: Conta #{account_id} - Valor: #{amount}")
    # Simulação de envio de mensagem para um processo de auditoria (se registrado)
    if Process.whereis(:audit_service) do
      send(:audit_service, {:high_value_transaction, account_id, amount})
    end
  end
end

defmodule FinancialSystem.Supervisor do
  @moduledoc """
  Garante a recuperação após falhas inesperadas (Requisito 1).
  """
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {FinancialSystem.TransactionManager, []}
    ]

    # Estratégia :one_for_one garante que o processo seja reiniciado em caso de crash
    Supervisor.init(children, strategy: :one_for_one)
  end
end
