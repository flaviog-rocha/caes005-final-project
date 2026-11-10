# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Fazer uso de macros.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Implementar expiração automática de registros.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task.Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderManager do
  @moduledoc """
  Gerenciador de pedidos usando ETS para estado, Task.Supervisor para concorrência
  e macros para DSL de transações atômicas.
  """

  use Supervisor
  require Logger

  @table :orders_table
  @expiration_ms 60_000

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    children = [{Task.Supervisor, name: OrderTaskSupervisor}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Macros ---

  defmacro transactional_order(order_id, do: block) do
    quote do
      order_id = unquote(order_id)
      # Impedir atualizações parciais: bloqueio lógico simples via status
      if :ets.insert_new(@table, {order_id, :pending, System.system_time(:millisecond)}) do
        Task.Supervisor.start_child(OrderTaskSupervisor, fn ->
          try do
            unquote(block)
            :ets.insert(@table, {order_id, :completed, System.system_time(:millisecond)})
            check_notifications(order_id, :completed)
          rescue
            e ->
              :ets.delete(@table, order_id)
              Logger.error("Falha na transação #{order_id}: #{inspect(e)}")
          end
        end)
      else
        {:error, :already_processing}
      end
    end
  end

  # --- Funcionalidades ---

  def get_status(order_id), do: :ets.lookup(@table, order_id)

  defp check_notifications(order_id, status) do
    if status == :completed do
      Logger.info("Notificação: Pedido #{order_id} finalizado com sucesso.")
    end
  end

  def cleanup_expired do
    now = System.system_time(:millisecond)
    :ets.foldl(fn {id, status, ts}, _acc ->
      if now - ts > @expiration_ms do
        :ets.delete(@table, id)
        Logger.info("Expiração: Registro #{id} removido.")
      end
    end, nil, @table)
  end

  # Exemplo de uso:
  # OrderManager.transactional_order("pedido_123") do
  #   :timer.sleep(1000)
  #   IO.puts("Processando...")
  # end
end
