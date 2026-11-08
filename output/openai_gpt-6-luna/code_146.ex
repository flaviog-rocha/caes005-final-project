# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderManager do
  @moduledoc """
  Gerencia pedidos isolados por entidade, com persistência local e notificações.
  """

  @statuses [:pending, :paid, :shipped, :cancelled]

  def start_link(opts \\ []) do
    with {:ok, config} <- validate_config(opts),
         {:ok, _} <- Application.ensure_all_started(:crypto) do
      OrderManager.Supervisor.start_link(config)
    end
  end

  def create_order(entity_id, attrs) do
    with :ok <- validate_entity_id(entity_id),
         {:ok, order} <- validate_order(attrs),
         {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, {:create_order, order})
    end
  end

  def update_status(entity_id, order_id, status) do
    with :ok <- validate_entity_id(entity_id),
         :ok <- validate_order_id(order_id),
         :ok <- validate_status(status),
         {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, {:update_status, order_id, status})
    end
  end

  def get_orders(entity_id) do
    with :ok <- validate_entity_id(entity_id),
         {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, :get_orders)
    end
  end

  def subscribe(entity_id, subscriber \\ self()) do
    with :ok <- validate_entity_id(entity_id),
         true <- is_pid(subscriber) || {:error, :invalid_subscriber},
         {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, {:subscribe, subscriber})
    end
  end

  defp ensure_entity(entity_id) do
    case Registry.lookup(OrderManager.Registry, entity_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        config = :persistent_term.get({__MODULE__, :config})

        case DynamicSupervisor.start_child(
               OrderManager.EntitySupervisor,
               {OrderManager.Entity, {entity_id, config}}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, _reason} = error ->
            case Registry.lookup(OrderManager.Registry, entity_id) do
              [{pid, _}] -> {:ok, pid}
              [] -> error
            end
        end
    end
  catch
    :error, {:badkey, _key} -> {:error, :not_started}
    :exit, _reason -> {:error, :not_started}
  end

  defp validate_config(opts) when is_list(opts) do
    path = Keyword.get(opts, :data_dir, "data/orders")
    threshold = Keyword.get(opts, :notification_threshold, 1_000)

    cond do
      not is_binary(path) or path == "" ->
        {:error, :invalid_data_dir}

      not valid_amount?(threshold) ->
        {:error, :invalid_notification_threshold}

      true ->
        {:ok, %{data_dir: path, notification_threshold: threshold}}
    end
  end

  defp validate_config(_), do: {:error, :invalid_options}

  defp validate_entity_id(id) when is_binary(id) and byte_size(id) in 1..200, do: :ok
  defp validate_entity_id(id) when is_integer(id) and id >= 0, do: :ok
  defp validate_entity_id(_), do: {:error, :invalid_entity_id}

  defp validate_order(attrs) when is_map(attrs) do
    with {:ok, id} <- Map.fetch(attrs, :id),
         :ok <- validate_order_id(id),
         {:ok, items} <- Map.fetch(attrs, :items),
         {:ok, valid_items, total} <- validate_items(items) do
      {:ok, %{id: id, items: valid_items, total: total, status: :pending}}
    else
      :error -> {:error, :missing_order_field}
      error -> error
    end
  end

  defp validate_order(_), do: {:error, :invalid_order}

  defp validate_order_id(id) when is_binary(id) and byte_size(id) in 1..200, do: :ok
  defp validate_order_id(id) when is_integer(id) and id >= 0, do: :ok
  defp validate_order_id(_), do: {:error, :invalid_order_id}

  defp validate_items(items) when is_list(items) and items != [] do
    Enum.reduce_while(items, {:ok, [], 0}, fn item, {:ok, acc, total} ->
      case validate_item(item) do
        {:ok, valid_item} ->
          item_total = valid_item.quantity * valid_item.unit_price
          {:cont, {:ok, [valid_item | acc], total + item_total}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, valid_items, total} ->
        {:ok, Enum.reverse(valid_items), total}

      error ->
        error
    end
  end

  defp validate_items(_), do: {:error, :invalid_items}

  defp validate_item(item) when is_map(item) do
    with {:ok, sku} <- Map.fetch(item, :sku),
         true <- is_binary(sku) and byte_size(sku) in 1..200,
         {:ok, quantity} <- Map.fetch(item, :quantity),
         true <- is_integer(quantity) and quantity > 0,
         {:ok, unit_price} <- Map.fetch(item, :unit_price),
         true <- valid_amount?(unit_price) do
      {:ok, %{sku: sku, quantity: quantity, unit_price: unit_price}}
    else
      :error -> {:error, :invalid_item}
      false -> {:error, :invalid_item}
      error -> error
    end
  end

  defp validate_item(_), do: {:error, :invalid_item}

  defp valid_amount?(value) when is_integer(value), do: value >= 0

  defp valid_amount?(value) when is_float(value) do
    value >= 0 and value < 1.0e308
  end

  defp valid_amount?(_), do: false

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(_), do: {:error, :invalid_status}
end

defmodule OrderManager.Supervisor do
  use Supervisor

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    :persistent_term.put({OrderManager, :config}, config)

    children = [
      {Registry, keys: :unique, name: OrderManager.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: OrderManager.EntitySupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule OrderManager.Entity do
  use GenServer

  def start_link({entity_id, config}) do
    GenServer.start_link(__MODULE__, {entity_id, config},
      name: {:via, Registry, {OrderManager.Registry, entity_id}}
    )
  end

  @impl true
  def init({entity_id, config}) do
    path = entity_path(config.data_dir, entity_id)

    with :ok <- File.mkdir_p(config.data_dir),
         {:ok, orders} <- load_orders(path) do
      {:ok,
       %{
         entity_id: entity_id,
         path: path,
         threshold: config.notification_threshold,
         orders: orders,
         subscribers: %{}
       }}
    else
      {:error, reason} -> {:stop, {:persistence_error, reason}}
    end
  end

  @impl true
  def handle_call({:create_order, order}, _from, state) do
    if Map.has_key?(state.orders, order.id) do
      {:reply, {:error, :order_already_exists}, state}
    else
      orders = Map.put(state.orders, order.id, order)

      case persist(state.path, orders) do
        :ok ->
          notify(state, order, nil)
          {:reply, {:ok, order}, %{state | orders: orders}}

        {:error, reason} ->
          {:reply, {:error, {:persistence_error, reason}}, state}
      end
    end
  end

  def handle_call({:update_status, order_id, status}, _from, state) do
    case Map.fetch(state.orders, order_id) do
      :error ->
        {:reply, {:error, :order_not_found}, state}

      {:ok, order} ->
        updated_order = %{order | status: status}
        orders = Map.put(state.orders, order_id, updated_order)

        case persist(state.path, orders) do
          :ok ->
            notify(state, updated_order, order.status)
            {:reply, {:ok, updated_order}, %{state | orders: orders}}

          {:error, reason} ->
            {:reply, {:error, {:persistence_error, reason}}, state}
        end
    end
  end

  def handle_call(:get_orders, _from, state) do
    {:reply, {:ok, Map.values(state.orders)}, state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    subscribers =
      if Map.has_key?(state.subscribers, subscriber) do
        state.subscribers
      else
        Map.put(state.subscribers, subscriber, Process.monitor(subscriber))
      end

    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
  end

  defp notify(state, order, previous_status) do
    events =
      []
      |> maybe_event(
        order.total >= state.threshold and previous_status == nil,
        :high_value
      )
      |> maybe_event(order.status == :paid and previous_status != :paid, :paid)

    Enum.each(events, fn reason ->
      message = {:order_notification, state.entity_id, order, reason}
      Enum.each(Map.keys(state.subscribers), &send(&1, message))
    end)
  end

  defp maybe_event(events, true, event), do: [event | events]
  defp maybe_event(events, false, _event), do: events

  defp entity_path(data_dir, entity_id) do
    hash =
      entity_id
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    Path.join(data_dir, hash <> ".term")
  end

  defp load_orders(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            orders when is_map(orders) -> {:ok, orders}
            _ -> {:error, :invalid_persisted_data}
          end
        rescue
          ArgumentError -> {:error, :invalid_persisted_data}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist(path, orders) do
    temporary_path = path <> ".tmp"
    binary = :erlang.term_to_binary(orders)

    with :ok <- File.write(temporary_path, binary, [:binary]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    end
  end
end
