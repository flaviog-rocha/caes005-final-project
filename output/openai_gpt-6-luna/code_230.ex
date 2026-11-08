# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir a execução de operações simultâneas.
# 4. Impedir atualizações parcialmente concluídas.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.OrderDSL do
  defmacro command(name, arguments, payload) do
    quote do
      def unquote(name)(id, unquote_splicing(arguments)) do
        Ecommerce.Orders.execute(id, unquote(payload))
      end
    end
  end
end

defmodule Ecommerce.Order do
  defstruct [:id, status: :pending, items: %{}, version: 0]

  def apply_command(%__MODULE__{status: :pending} = order, {:add_item, sku, quantity, unit_price})
      when is_binary(sku) and byte_size(sku) > 0 and is_integer(quantity) and quantity > 0 and
             is_integer(unit_price) and unit_price >= 0 do
    case Map.get(order.items, sku) do
      nil ->
        item = %{quantity: quantity, unit_price: unit_price}
        {:ok, bump(%{order | items: Map.put(order.items, sku, item)})}

      %{unit_price: ^unit_price, quantity: existing_quantity} = item ->
        updated_item = %{item | quantity: existing_quantity + quantity}
        {:ok, bump(%{order | items: Map.put(order.items, sku, updated_item)})}

      _item ->
        {:error, :price_mismatch}
    end
  end

  def apply_command(%__MODULE__{status: :pending} = order, {:remove_item, sku})
      when is_binary(sku) do
    if Map.has_key?(order.items, sku) do
      {:ok, bump(%{order | items: Map.delete(order.items, sku)})}
    else
      {:error, :item_not_found}
    end
  end

  def apply_command(%__MODULE__{status: :pending} = order, :submit) do
    if map_size(order.items) > 0 do
      {:ok, bump(%{order | status: :submitted})}
    else
      {:error, :empty_order}
    end
  end

  def apply_command(%__MODULE__{status: status} = order, :cancel)
      when status in [:pending, :submitted] do
    {:ok, bump(%{order | status: :cancelled})}
  end

  def apply_command(%__MODULE__{}, {:add_item, _, _, _}), do: {:error, :invalid_item}
  def apply_command(%__MODULE__{status: status}, _command) when status != :pending,
    do: {:error, :order_not_editable}

  def apply_command(%__MODULE__{}, _command), do: {:error, :unsupported_command}

  def total(%__MODULE__{items: items}) do
    Enum.reduce(items, 0, fn {_sku, item}, sum ->
      sum + item.quantity * item.unit_price
    end)
  end

  defp bump(order), do: %{order | version: order.version + 1}
end

defmodule Ecommerce.FakeRepo do
  use GenServer

  @database_key {__MODULE__, :database}

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def create_order(id), do: GenServer.call(__MODULE__, {:create_order, id})
  def get_order(id), do: GenServer.call(__MODULE__, {:get_order, id})

  def transact(id, operation_id, command) do
    GenServer.call(__MODULE__, {:transact, id, operation_id, command})
  end

  @impl true
  def init(:ok) do
    database = :persistent_term.get(@database_key, %{orders: %{}, operations: %{}})
    {:ok, database}
  end

  @impl true
  def handle_call({:create_order, nil}, _from, database) do
    {:reply, {:error, :invalid_order_id}, database}
  end

  def handle_call({:create_order, id}, _from, database) do
    case Map.fetch(database.orders, id) do
      {:ok, _order} ->
        {:reply, {:error, :already_exists}, database}

      :error ->
        order = %Ecommerce.Order{id: id}
        updated = %{database | orders: Map.put(database.orders, id, order)}
        persist(updated)
        {:reply, {:ok, order}, updated}
    end
  end

  def handle_call({:get_order, id}, _from, database) do
    reply =
      case Map.fetch(database.orders, id) do
        {:ok, order} -> {:ok, order}
        :error -> {:error, :not_found}
      end

    {:reply, reply, database}
  end

  def handle_call({:transact, id, operation_id, command}, _from, database) do
    case Map.fetch(database.operations, operation_id) do
      {:ok, previous_result} ->
        {:reply, previous_result, database}

      :error ->
        {result, updated} = apply_transaction(database, id, operation_id, command)
        persist(updated)
        {:reply, result, updated}
    end
  end

  defp apply_transaction(database, id, operation_id, command) do
    result =
      case Map.fetch(database.orders, id) do
        {:ok, order} ->
          case Ecommerce.Order.apply_command(order, command) do
            {:ok, updated_order} -> {:ok, updated_order}
            {:error, reason} -> {:error, reason}
          end

        :error ->
          {:error, :not_found}
      end

    orders =
      case result do
        {:ok, updated_order} -> Map.put(database.orders, id, updated_order)
        {:error, _reason} -> database.orders
      end

    updated = %{
      database
      | orders: orders,
        operations: Map.put(database.operations, operation_id, result)
    }

    {result, updated}
  end

  defp persist(database), do: :persistent_term.put(@database_key, database)
end

defmodule Ecommerce.OrderServer do
  use GenServer

  def start_link(order_id) do
    GenServer.start_link(
      __MODULE__,
      order_id,
      name: {:via, Registry, {Ecommerce.OrderRegistry, order_id}}
    )
  end

  @impl true
  def init(order_id), do: {:ok, order_id}

  @impl true
  def handle_call({:execute, operation_id, command}, _from, order_id) do
    {:reply, Ecommerce.FakeRepo.transact(order_id, operation_id, command), order_id}
  end
end

defmodule Ecommerce.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      Ecommerce.FakeRepo,
      {Registry, keys: :unique, name: Ecommerce.OrderRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Ecommerce.OrderWorkers}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Ecommerce do
  def start_link(opts \\ []), do: Ecommerce.Supervisor.start_link(opts)
end

defmodule Ecommerce.Orders do
  import Ecommerce.OrderDSL

  def create(id), do: Ecommerce.FakeRepo.create_order(id)
  def get(id), do: Ecommerce.FakeRepo.get_order(id)

  command :add_item, [sku, quantity, unit_price], {:add_item, sku, quantity, unit_price}
  command :remove_item, [sku], {:remove_item, sku}
  command :submit, [], :submit
  command :cancel, [], :cancel

  def execute(id, command) do
    operation_id = make_ref()
    execute_with_recovery(id, operation_id, command, 3)
  end

  defp execute_with_recovery(_id, _operation_id, _command, 0),
    do: {:error, :service_unavailable}

  defp execute_with_recovery(id, operation_id, command, attempts) do
    with {:ok, pid} <- ensure_worker(id) do
      try do
        GenServer.call(pid, {:execute, operation_id, command}, 5_000)
      catch
        :exit, _reason ->
          execute_with_recovery(id, operation_id, command, attempts - 1)
      end
    else
      {:error, _reason} ->
        Process.sleep(10)
        execute_with_recovery(id, operation_id, command, attempts - 1)
    end
  end

  defp ensure_worker(id) do
    case Registry.lookup(Ecommerce.OrderRegistry, id) do
      [{pid, _value}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Ecommerce.OrderWorkers,
               {Ecommerce.OrderServer, id}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, _reason} ->
            case Registry.lookup(Ecommerce.OrderRegistry, id) do
              [{pid, _value}] -> {:ok, pid}
              [] -> {:error, :worker_unavailable}
            end
        end
    end
  end
end
