# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir a execução de operações simultâneas.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações financeiras com controle de concorrência usando DynamicSupervisor.
  """
  use DynamicSupervisor

  @doc "Inicia o supervisor principal"
  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Executa uma transação com limite de concorrência.
  Utilizamos um Pool (via Task.Supervisor ou Worker) para limitar.
  Aqui, limitamos a concorrência através de um Registry ou semáforo simples.
  """
  def execute_transaction(account_id, amount) do
    # Verifica limite de concorrência global (exemplo simples com contador no Registry)
    case Registry.count(:transaction_registry) do
      n when n < 10 ->
        DynamicSupervisor.start_child(__MODULE__, {TransactionWorker, {account_id, amount}})
      _ ->
        {:error, :too_many_concurrent_operations}
    end
  end
end

defmodule TransactionWorker do
  use Task, restart: :transient

  @doc "Simula a lógica de negócio e persistência"
  def start_link({account_id, amount}) do
    Task.start_link(fn ->
      Registry.register(:transaction_registry, account_id, make_ref())
      
      # Simulação de latência de banco de dados
      :timer.sleep(1000) 
      
      # Lógica fictícia de persistência (Ecto-like)
      IO.puts("Processando transação de #{amount} para conta #{account_id}")
      
      Registry.unregister(:transaction_registry, account_id)
      :ok
    end)
  end
end

# Inicialização do ambiente
defmodule App do
  def run do
    # Necessário para rastrear concorrência
    Registry.start_link(keys: :duplicate, name: :transaction_registry)
    TransactionManager.start_link([])

    # Exemplo de execução simultânea
    Enum.each(1..5, fn i ->
      TransactionManager.execute_transaction("ACC_#{i}", 100 * i)
    end)
  end
end
