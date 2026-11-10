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
  Sistema de transações financeiras utilizando agentes para persistência em memória,
  garantindo idempotência, histórico e expiração.
  """

  use Agent

  @type transaction_id :: String.t()
  @type amount :: decimal()
  @type t :: %{
          id: transaction_id(),
          amount: amount(),
          status: :processed | :expired,
          inserted_at: DateTime.t()
        }

  # Configuração: transações expiram em 3600 segundos (1 hora)
  @expiration_seconds 3600

  def start_link(_), do: Agent.start_link(fn -> %{log: %{}, history: []} end, name: __MODULE__)

  @doc """
  Executa uma transação garantindo idempotência.
  """
  def process_transaction(id, amount) when is_binary(id) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state.log, id) do
        nil ->
          transaction = %{
            id: id,
            amount: amount,
            status: :processed,
            inserted_at: DateTime.utc_now()
          }

          new_state = %{
            state |
            log: Map.put(state.log, id, transaction),
            history: [transaction | state.history]
          }

          {:ok, new_state}

        _existing ->
          {:error, :already_processed}
      end
    end)
  end

  @doc """
  Remove transações expiradas do log.
  """
  def expire_transactions do
    Agent.update(__MODULE__, fn state ->
      now = DateTime.utc_now()

      expired_log =
        Enum.reject(state.log, fn {_id, tx} ->
          DateTime.diff(now, tx.inserted_at) > @expiration_seconds
        end)
        |> Map.new()

      %{state | log: expired_log}
    end)
  end

  @doc """
  Retorna o histórico completo das operações.
  """
  def get_history, do: Agent.get(__MODULE__, & &1.history)

  @doc """
  Consulta o status de uma operação específica.
  """
  def get_transaction(id), do: Agent.get(__MODULE__, &Map.get(&1.log, id))
end

# Exemplo de uso:
# {:ok, _} = FinancialSystem.start_link([])
# FinancialSystem.process_transaction("tx_001", 100.50)
# FinancialSystem.process_transaction("tx_001", 100.50) # Retorna {:error, :already_processed}
# FinancialSystem.get_history()
