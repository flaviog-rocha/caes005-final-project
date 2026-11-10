# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderManager do
  @moduledoc """
  Gerenciador de pedidos com retries, expiração (TTL), tarefas periódicas,
  rastreamento de estado e idempotência (via ETS).
  """
  use GenServer

  @table_name :order_operations
  @ttl_ms 60_000

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def init(_) do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    :timer.send_interval(10_000, :cleanup)
    {:ok, %{}}
  end

  # Requisito 5: Impedir execução dupla (Idempotência)
  def process_order(order_id, action) do
    if :ets.insert_new(@table_name, {order_id, :running, :os.system_time(:millisecond)}) do
      Task.start(fn -> execute_with_retry(order_id, action, 3) end)
      {:ok, :started}
    else
      {:error, :already_in_progress}
    end
  end

  # Requisito 1: Retry após falhas
  defp execute_with_retry(order_id, action, retries) do
    try do
      # Simulação de lógica de negócio
      result = perform_action(action)
      :ets.insert(@table_name, {order_id, :completed, :os.system_time(:millisecond)})
      result
    rescue
      _ ->
        if retries > 1 do
          execute_with_retry(order_id, action, retries - 1)
        else
          :ets.insert(@table_name, {order_id, :failed, :os.system_time(:millisecond)})
        end
    end
  end

  # Requisito 4: Consultar estado
  def get_status(order_id) do
    case :ets.lookup(@table_name, order_id) do
      [{^order_id, state, _}] -> {:ok, state}
      [] -> {:error, :not_found}
    end
  end

  # Requisito 3: Operações periódicas (Cleanup)
  def handle_info(:cleanup, state) do
    now = :os.system_time(:millisecond)
    :ets.match_delete(@table_name, {:_, :_, :"$1"})
    
    # Requisito 2: Expiração automática (TTL > 60s)
    :ets.foldl(fn {id, status, timestamp}, _ ->
      if now - timestamp > @ttl_ms, do: :ets.delete(@table_name, id)
    end, nil, @table_name)
    
    {:noreply, state}
  end

  defp perform_action(action) do
    # Lógica simulada de falha aleatória para testar retry
    if :rand.uniform(10) > 7, do: raise("Transient Error"), else: :ok
  end
end
