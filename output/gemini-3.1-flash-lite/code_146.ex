# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de Pedidos - Arquitetura baseada em GenServer e Registry
# Este módulo gerencia o ciclo de vida de pedidos de forma isolada e persistente.

defmodule Ecommerce.OrderManager do
  use GenServer
  require Logger

  @persistence_file "orders.dets"

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def create_order(order_id, data) do
    with :ok <- validate_order(data) do
      Registry.start_link(keys: :unique, name: OrderRegistry)
      spec = %{
        id: {:via, Registry, {OrderRegistry, order_id}},
        start: {OrderWorker, :start_link, [data]}
      }
      DynamicSupervisor.start_child(OrderSupervisor, spec)
    end
  end

  # --- Callbacks ---

  def init(_) do
    {:ok, _} = Registry.start_link(keys: :unique, name: OrderRegistry)
    {:ok, _} = DynamicSupervisor.start_link(strategy: :one_for_one, name: OrderSupervisor)
    {:ok, nil}
  end

  defp validate_order(%{total: total, items: items}) when total > 0 and is_list(items), do: :ok
  defp validate_order(_), do: {:error, :invalid_order_data}
end

defmodule OrderWorker do
  use GenServer, restart: :transient

  def start_link(data), do: GenServer.start_link(__MODULE__, data)

  def init(data) do
    # Persistência básica usando ETS/DETS para recuperação
    {:ok, table} = :dets.open_file(:orders, [file: 'orders.dets', type: :set])
    :dets.insert(table, {self(), data})
    
    # Notificação condicional
    if data.total > 1000, do: Logger.info("Pedido de alto valor criado: #{data.total}")
    
    {:ok, data}
  end

  def handle_cast({:update_status, status}, state) do
    new_state = Map.put(state, :status, status)
    {:noreply, new_state}
  end
end

# --- Inicialização do Supervisionamento ---

defmodule Ecommerce.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: OrderRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: OrderSupervisor},
      Ecommerce.OrderManager
    ]
    Supervisor.start_link(children, strategy: :one_for_all)
  end
end
