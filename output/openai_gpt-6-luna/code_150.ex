# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.OrderManager do
  use GenServer

  @statuses [:pending, :paid, :processing, :shipped, :delivered, :cancelled]

  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:processing, :cancelled],
    processing: [:shipped, :cancelled],
    shipped: [:delivered],
    delivered: [],
    cancelled: []
  }

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def subscribe(server \\ __MODULE__) do
    GenServer.call(server, {:subscribe, self()})
  end

  def create_order(attrs), do: create_order(__MODULE__, attrs)

  def create_order(server, attrs) do
    GenServer.call(server, {:create_order, attrs})
  end

  def update_status(order_id, status), do: update_status(__MODULE__, order_id, status)

  def update_status(server, order_id, status) do
    GenServer.call(server, {:update_status, order_id, status})
  end

  def get_order(order_id), do: get_order(__MODULE__, order_id)

  def get_order(server, order_id) do
    GenServer.call(server, {:get_order, order_id})
  end

  def list_orders(server \\ __MODULE__) do
    GenServer.call(server, :list_orders)
  end

  def watch_process(pid, context \\ %{}), do: watch_process(__MODULE__, pid, context)

  def watch_process(server, pid, context) do
    GenServer.call(server, {:watch_process, pid, context})
  end

  def unwatch_process(reference), do: unwatch_process(__MODULE__, reference)

  def unwatch_process(server, reference) do
    GenServer.call(server, {:unwatch_process, reference})
  end

  @impl true
  def init(opts) do
    if not Keyword.keyword?(opts) do
      {:stop, :invalid_options}
    else
      path = Keyword.get(opts, :path, "orders.term")
      threshold = Keyword.get(opts, :high_value_threshold, 100_000)

      if valid_path?(path) and is_integer(threshold) and threshold >= 0 do
        case load_orders(path) do
          {:ok, next_id, orders} ->
            {:ok,
             %{
               path: path,
               high_value_threshold: threshold,
               next_id: next_id,
               orders: orders,
               subscribers: MapSet.new(),
               monitors: %{}
             }}

          {:error, reason} ->
            {:stop, {:persistence_error, reason}}
        end
      else
        {:stop, :invalid_options}
      end
    end
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) when is_pid(pid) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:create_order, attrs}, _from, state) do
    case validate_order(attrs) do
      {:ok, customer_id, items, total} ->
        id = state.next_id

        order = %{
          id: id,
          customer_id: customer_id,
          items: items,
          total: total,
          status: :pending,
          created_at: System.system_time(:second)
        }

        candidate = %{
          state
          | next_id: id + 1,
            orders: Map.put(state.orders, id, order)
        }

        case persist(candidate) do
          :ok ->
            notify(candidate, %{type: :order_created, order: order})

            if total >= candidate.high_value_threshold do
              notify(candidate, %{
                type: :high_value_order,
                order_id: id,
                total: total,
                threshold: candidate.high_value_threshold
              })
            end

            {:reply, {:ok, order}, candidate}

          {:error, reason} ->
            {:reply, {:error, {:persistence_error, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:update_status, order_id, status}, _from, state) do
    with :ok <- validate_order_id(order_id),
         true <- status in @statuses or {:error, :invalid_status},
         {:ok, order} <- fetch_order(state.orders, order_id),
         true <- status in Map.fetch!(@transitions, order.status) or {:error, :invalid_transition} do
      updated_order = %{order | status: status}
      candidate = %{state | orders: Map.put(state.orders, order_id, updated_order)}

      case persist(candidate) do
        :ok ->
          notify(candidate, %{
            type: :status_changed,
            order_id: order_id,
            old_status: order.status,
            new_status: status
          })

          if status == :paid do
            notify(candidate, %{type: :order_paid, order_id: order_id})
          end

          {:reply, {:ok, updated_order}, candidate}

        {:error, reason} ->
          {:reply, {:error, {:persistence_error, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      false -> {:reply, {:error, :invalid_status}, state}
    end
  end

  def handle_call({:get_order, order_id}, _from, state) do
    reply =
      with :ok <- validate_order_id(order_id),
           {:ok, order} <- fetch_order(state.orders, order_id) do
        {:ok, order}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_orders, _from, state) do
    orders = state.orders |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, orders, state}
  end

  def handle_call({:watch_process, pid, context}, _from, state) do
    if is_pid(pid) and is_map(context) do
      reference = Process.monitor(pid)
      monitors = Map.put(state.monitors, reference, %{pid: pid, context: context})
      {:reply, {:ok, reference}, %{state | monitors: monitors}}
    else
      {:reply, {:error, :invalid_process_or_context}, state}
    end
  end

  def handle_call({:unwatch_process, reference}, _from, state) do
    case Map.pop(state.monitors, reference) do
      {nil, _monitors} ->
        {:reply, {:error, :unknown_monitor}, state}

      {_entry, monitors} ->
        Process.demonitor(reference, [:flush])
        {:reply, :ok, %{state | monitors: monitors}}
    end
  end

  @impl true
  def handle_info({:DOWN, reference, :process, pid, reason}, state) do
    case Map.pop(state.monitors, reference) do
      {nil, _monitors} ->
        {:noreply, state}

      {%{pid: ^pid, context: context}, monitors} ->
        updated_state = %{state | monitors: monitors}

        unless expected_exit?(reason) do
          notify(updated_state, %{
            type: :unexpected_process_exit,
            pid: pid,
            reason: reason,
            context: context
          })
        end

        {:noreply, updated_state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp validate_order(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    cond do
      not (is_binary(customer_id) and byte_size(String.trim(customer_id)) > 0) ->
        {:error, :invalid_customer_id}

      not is_list(items) or items == [] ->
        {:error, :invalid_items}

      true ->
        validate_items(items)
        |> case do
          {:ok, normalized_items, total} -> {:ok, customer_id, normalized_items, total}
          error -> error
        end
    end
  end

  defp validate_order(_), do: {:error, :invalid_order}

  defp validate_items(items) do
    Enum.reduce_while(items, {:ok, [], 0}, fn item, {:ok, acc, total} ->
      case validate_item(item) do
        {:ok, normalized_item, item_total} ->
          {:cont, {:ok, [normalized_item | acc], total + item_total}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized_items, total} ->
        {:ok, Enum.reverse(normalized_items), total}

      error ->
        error
    end
  end

  defp validate_item(item) when is_map(item) do
    sku = Map.get(item, :sku)
    quantity = Map.get(item, :quantity)
    unit_price = Map.get(item, :unit_price)

    if is_binary(sku) and byte_size(String.trim(sku)) > 0 and
         is_integer(quantity) and quantity > 0 and
         is_integer(unit_price) and unit_price >= 0 do
      {:ok,
       %{sku: sku, quantity: quantity, unit_price: unit_price},
       quantity * unit_price}
    else
      {:error, :invalid_item}
    end
  end

  defp validate_item(_), do: {:error, :invalid_item}

  defp validate_order_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_order_id(_), do: {:error, :invalid_order_id}

  defp fetch_order(orders, id) do
    case Map.fetch(orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :order_not_found}
    end
  end

  defp notify(state, event) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:ecommerce_notification, event})
    end)
  end

  defp expected_exit?(:normal), do: true
  defp expected_exit?(:shutdown), do: true
  defp expected_exit?({:shutdown, _reason}), do: true
  defp expected_exit?(_reason), do: false

  defp valid_path?(path), do: is_binary(path) and byte_size(String.trim(path)) > 0

  defp load_orders(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            {:ecommerce_orders_v1, next_id, orders}
            when is_integer(next_id) and next_id > 0 and is_map(orders) ->
              if valid_loaded_orders?(next_id, orders) do
                {:ok, next_id, orders}
              else
                {:error, :invalid_persisted_data}
              end

            _ ->
              {:error, :invalid_persisted_data}
          end
        rescue
          _ -> {:error, :invalid_persisted_data}
        end

      {:error, :enoent} ->
        {:ok, 1, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp valid_loaded_orders?(next_id, orders) do
    Enum.all?(orders, fn
      {id, order} when is_integer(id) and id > 0 ->
        id < next_id and valid_loaded_order?(id, order)

      _ ->
        false
    end)
  end

  defp valid_loaded_order?(id, order) when is_map(order) do
    with true <- Map.get(order, :id) == id,
         customer_id when is_binary(customer_id) <- Map.get(order, :customer_id),
         items when is_list(items) and items != [] <- Map.get(order, :items),
         status when status in @statuses <- Map.get(order, :status),
         created_at when is_integer(created_at) <- Map.get(order, :created_at),
         {:ok, normalized_items, total} <- validate_items(items),
         true <- normalized_items == items,
         true <- Map.get(order, :total) == total do
      byte_size(String.trim(customer_id)) > 0
    else
      _ -> false
    end
  end

  defp valid_loaded_order?(_id, _order), do: false

  defp persist(state) do
    data =
      :erlang.term_to_binary(
        {:ecommerce_orders_v1, state.next_id, state.orders},
        [:compressed]
      )

    temp_path = state.path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(state.path)),
         :ok <- File.write(temp_path, data, [:binary]),
         :ok <- File.rename(temp_path, state.path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temp_path)
        {:error, reason}
    end
  end
end
