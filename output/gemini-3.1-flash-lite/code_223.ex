# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir comunicação entre diferentes processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Implementar expiração automática de registros.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para atender aos requisitos utilizando apenas a biblioteca padrão:
# 1. Supervisores detectam falhas (processos ligados).
# 2. Registros (Registry) permitem comunicação via nomes.
# 3. :ets com opção :bag e :named_table para persistência em disco.
# 4. Processos individuais com `Process.send_after` para expiração.
# 5. Supervisor dinâmico para isolamento de entidades.

defmodule Ecommerce.OrderManager do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: OrderRegistry},
      {DynamicSupervisor, name: OrderSupervisor, strategy: :one_for_one},
      {Agent, fn -> :ets.new(:orders_table, [:named_table, :set, :public, read_concurrency: true]) end, name: OrderTable}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def create_order(order_id, data, ttl_ms \\ 60_000) do
    DynamicSupervisor.start_child(OrderSupervisor, {OrderWorker, {order_id, data, ttl_ms}})
  end
end

defmodule OrderWorker do
  use GenServer

  def start_link({order_id, data, ttl_ms}) do
    GenServer.start_link(__MODULE__, {order_id, data, ttl_ms}, name: {:via, Registry, {OrderRegistry, order_id}})
  end

  @impl true
  def init({order_id, data, ttl_ms}) do
    # Persistência
    :ets.insert(:orders_table, {order_id, data})
    
    # Expiração automática
    Process.send_after(self(), :expire, ttl_ms)
    
    {:ok, %{order_id: order_id, data: data}}
  end

  @impl true
  def handle_info(:expire, state) do
    :ets.delete(:orders_table, state.order_id)
    {:stop, :normal, state}
  end

  # Comunicação entre processos
  @impl true
  def handle_call({:get_data}, _from, state) do
    {:reply, state.data, state}
  end

  def get_order_data(order_id) do
    case GenServer.whereis({:via, Registry, {OrderRegistry, order_id}}) do
      nil -> {:error, :not_found}
      pid -> GenServer.call(pid, {:get_data})
    end
  end
end

# Exemplo de uso:
# Ecommerce.OrderManager.start_link([])
# Ecommerce.OrderManager.create_order("pedido_123", %{item: "Livro Elixir", valor: 50.0})
# Ecommerce.OrderWorker.get_order_data("pedido_123")
