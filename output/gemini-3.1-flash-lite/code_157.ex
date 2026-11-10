# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento de entregas garantindo idempotência, 
  atomicidade e resiliência.
  """

  # Requisito 1: Idempotência via ID de operação
  # Requisito 2: Atomicidade via simulação de transações
  # Requisito 4: Retry via recursão com estratégia de backoff simples

  @doc """
  Processa uma atualização de status de entrega de forma segura.
  """
  def update_delivery(delivery_id, status, operation_id) do
    execute_with_retry(fn ->
      ensure_idempotency(operation_id, fn ->
        run_in_transaction(fn ->
          # Simula acesso a banco de dados
          IO.puts("Executando: Status #{status} para entrega #{delivery_id}")
          {:ok, :updated}
        end)
      end)
    end)
  end

  # --- Helpers Internos ---

  defp execute_with_retry(func, attempts \\ 3) do
    try do
      func.()
    rescue
      e in [RuntimeError] ->
        if attempts > 1 do
          Process.sleep(100) # Backoff simples
          execute_with_retry(func, attempts - 1)
        else
          {:error, :max_retries_exceeded}
        end
    end
  end

  defp ensure_idempotency(operation_id, func) do
    # Simula verificação em cache/banco de registros processados
    case :ets.lookup(:processed_operations, operation_id) do
      [{^operation_id, _result}] -> {:error, :already_processed}
      [] ->
        result = func.()
        :ets.insert(:processed_operations, {operation_id, result})
        result
    end
  end

  defp run_in_transaction(func) do
    # Simula atomicidade: Se o bloco falhar, nada é persistido
    try do
      result = func.()
      # Simula commit
      result
    rescue
      e ->
        # Simula rollback
        IO.puts("Rollback acionado: #{inspect(e)}")
        reraise e, __STACKTRACE__
    end
  end

  # Inicialização de estrutura de controle para o exemplo rodar
  def start_system do
    :ets.new(:processed_operations, [:set, :public, :named_table])
  end
end

# --- Exemplo de Execução ---

DeliverySystem.start_system()

# Sucesso
IO.inspect DeliverySystem.update_delivery("DEL-001", "TRANSIT", "OP-100")

# Requisito 1: Impedir reexecução (Idempotência)
IO.inspect DeliverySystem.update_delivery("DEL-001", "TRANSIT", "OP-100")

# Requisito 4: Retry em falhas (simulação)
# Se a lógica dentro da transação falhar, o sistema tentará novamente
