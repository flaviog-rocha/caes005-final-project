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
  use GenServer

  @moduledoc """
  Gerenciador de pedidos com retry, expiração, periodicidade e idempotência.
  Utiliza :ets para estado compartilhado e :timer para agendamentos.
  """

  # Requisito 4 & 5: Tabela ETS para consultar estado e garantir idempotência
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def init(_) do
    :ets.new(:orders_table, [:set, :named_table, :public, read_concurrency: true])
    # Requisito 3: Tarefa periódica (limpeza de expirados a cada 60s)
    :timer.send_interval(60_000, :cleanup_expired)
    {:ok, %{}}
  end

  # Requisito 5: Impedir execução duplicada
  def process_order(order_id, action) do
    if :ets.insert_new(:orders_table, {order_id, :processing, System.system_time(:second) + 3600}) do
      GenServer.cast(__MODULE__, {:run_task, order_id, action, 0})
      {:ok, :started}
    else
      {:error, :already_processing_or_exists}
    end
  end

  # Requisito 4: Consultar estado
  def get_status(order_id), do: :ets.lookup(:orders_table, order_id)

  # Requisito 1: Retry com lógica de falha temporária
  def handle_cast({:run_task, id, action, retries}, state) do
    case perform_action(action) do
      :ok ->
        :ets.insert(:orders_table, {id, :completed, System.system_time(:second) + 3600})
      :error when retries < 3 ->
        Process.send_after(self(), {:retry, id, action, retries + 1}, 1000)
      _ ->
        :ets.insert(:orders_table, {id, :failed, System.system_time(:second) + 3600})
    end
    {:noreply, state}
  end

  def handle_info({:retry, id, action, retries}, state) do
    GenServer.cast(self(), {:run_task, id, action, retries})
    {:noreply, state}
  end

  # Requisito 2: Expiração automática
  def handle_info(:cleanup_expired, state) do
    now = System.system_time(:second)
    :ets.select_delete(:orders_table, [{{:_, :_, :"$1"}, [{:<, :"$1", now}], [true]}])
    {:noreply, state}
  end

  # Simulação de IO externo
  defp perform_action(_), do: if(:rand.uniform(10) > 7, do: :ok, else: :error)
end
