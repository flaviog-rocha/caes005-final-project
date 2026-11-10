# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager do
  @moduledoc """
  Gerenciador de entregas com foco em idempotência, atomicidade e resiliência.
  """

  # Simulação de um banco de dados para persistência
  # Em um cenário real, usaríamos Ecto.Multi para garantir atomicidade no banco.
  @db_agent :delivery_db

  def start_link do
    Agent.start_link(fn -> %{} end, name: @db_agent)
  end

  @doc """
  Atualiza o status de uma entrega garantindo:
  1. Idempotência: Verifica se o status já não foi atingido.
  2. Atomicidade: Utiliza transação fictícia (garantindo consistência).
  3. Resiliência: Implementa mecanismo de retentativa (Retry).
  """
  def update_status(delivery_id, new_status, idempotency_key) do
    retry_with_backoff(fn ->
      perform_transaction(fn ->
        delivery = Agent.get(@db_agent, &Map.get(&1, delivery_id, %{}))

        # 1. Check de Idempotência
        if MapSet.member?(Map.get(delivery, :processed_keys, MapSet.new()), idempotency_key) do
          {:ok, :already_processed}
        else
          # 2. Atomicidade: Atualização do estado e registro da chave
          new_delivery = %{
            status: new_status,
            processed_keys: MapSet.put(Map.get(delivery, :processed_keys, MapSet.new()), idempotency_key)
          }

          Agent.update(@db_agent, &Map.put(&1, delivery_id, new_delivery))
          {:ok, :updated}
        end
      end)
    end)
  end

  # 4. Implementação de novas tentativas (Exponential Backoff)
  defp retry_with_backoff(fun, attempt \\ 0) do
    max_attempts = 3

    try do
      case fun.() do
        {:ok, result} -> result
        {:error, :transient} when attempt < max_attempts ->
          :timer.sleep(:math.pow(2, attempt) |> round() * 100)
          retry_with_backoff(fun, attempt + 1)
      end
    rescue
      _ -> if attempt < max_attempts, do: retry_with_backoff(fun, attempt + 1), else: :error
    end
  end

  # Simulação de transação de banco de dados
  defp perform_transaction(fun) do
    # Em um cenário Ecto, aqui usaríamos Ecto.Multi.new() |> ... |> Repo.transaction()
    fun.()
  end
end

# --- Exemplo de uso ---
# DeliveryManager.start_link()
# DeliveryManager.update_status("pedido_123", "enviado", "req_abc_001")
