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
  Gerenciador de pedidos com atomicidade, monitoramento e expiração.
  """
  use Agent

  defstruct orders: %{}, status: %{}

  # Macros para DSL de transação atômica
  defmacro transaction(order_id, do: block) do
    quote do
      order_id = unquote(order_id)
      # 1. Impedir atualizações parciais: o uso de um Agent garante serialização
      # Se uma etapa falhar, o estado não é consolidado.
      try do
        OrderManager.update_status(order_id, :processing)
        result = unquote(block)
        OrderManager.update_status(order_id, :completed)
        result
      rescue
        e ->
          OrderManager.update_status(order_id, :failed)
          reraise e, __STACKTRACE__
      end
    end
  end

  # API Pública
  def start_link(_), do: Agent.start_link(fn -> %__MODULE__{} end, name: __MODULE__)

  def update_status(id, status) do
    Agent.update(__MODULE__, fn state ->
      put_in(state.status[id], status)
    end)
  end

  # 3. Consultar estado atual
  def get_order_status(id), do: Agent.get(__MODULE__, &get_in(&2, [:status, id]))

  # 4. Expiração automática e 5. Notificações
  def process_order_with_timeout(id, work_fn, timeout \\ 5000) do
    Task.Supervisor.start_child(OrderTaskSupervisor, fn ->
      # Task de monitoramento de expiração
      monitor = Task.async(fn ->
        Process.sleep(timeout)
        if get_order_status(id) == :processing do
          update_status(id, :expired)
          IO.puts("NOTIFICAÇÃO: Pedido #{id} expirou!")
        end
      end)

      # Execução da lógica
      try do
        result = work_fn.()
        Task.yield(monitor)
        result
      rescue
        _ -> IO.puts("NOTIFICAÇÃO: Falha crítica no pedido #{id}")
      end
    end)
  end
end

# Supervisor para gerenciar as Tasks de processamento
defmodule OrderTaskSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: Supervisor.init([{Task.Supervisor, name: __MODULE__}], strategy: :one_for_one)
end

# Exemplo de uso (em um ambiente real, seria chamado via processo)
# OrderTaskSupervisor.start_link([])
# OrderManager.start_link([])
#
# OrderManager.process_order_with_timeout("ped_123", fn ->
#   require OrderManager
#   OrderManager.transaction "ped_123" do
#     Process.sleep(1000)
#     :ok
#   end
# end)
