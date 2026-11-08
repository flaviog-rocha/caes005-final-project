# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule EcommerceOrders do
  use GenServer

  @statuses [:pending, :paid, :shipped, :delivered, :cancelled]
  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [:delivered],
    delivered: [],
    cancelled: []
  }

  # Os valores monetários são expressos em centavos. Por padrão, pedidos de
  # pelo menos 100.000 centavos geram uma notificação adicional.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def create_order(server, attrs), do: GenServer.call(server, {:create_order, attrs})
  def get_order(server, id), do: GenServer.call(server, {:get_order, id})
  def list_orders(server), do: GenServer.call(server, :list_orders)
  def list_notifications(server), do: GenServer.call(server, :list_notifications)
  def update_status(server, id, status), do: GenServer.call(server, {:update_status, id, status})
  def subscribe(server), do: GenServer.call(server, {:subscribe, self()})
  def start_worker(server, order_id), do: GenServer.call(server, {:start_worker, order_id})
  def stop_worker(server, order_id), do: GenServer.call(server, {:stop_worker, order_id})
  def crash_worker(server, order_id), do: GenServer.call(server, {:crash_worker, order_id})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "orders.db")
    threshold = Keyword.get(opts, :high_value_threshold_cents, 100_000)

    if is_binary(path) and is_integer(threshold) and threshold > 0 do
      case load_store(path) do
        {:ok, stored} ->
          {:ok,
           %{
             path: path,
             threshold: threshold,
             orders: stored.orders,
             notifications: stored.notifications,
             subscribers: MapSet.new(),
             subscriber_refs: %{},
             workers: %{},
             worker_refs: %{}
           }}

        {:error, reason} ->
          {:stop, {:store_load_failed, reason}}
      end
    else
      {:stop, :invalid_configuration}
    end
  end

  @impl true
  def handle_call({:create_order, attrs}, _from, state) do
    with {:ok, order} <- validate_order(attrs),
         false <- Map.has_key?(state.orders, order.id) do
      order =
        Map.merge(order, %{
          status: :pending,
          inserted_at: System.system_time(:millisecond)
        })

      notifications =
        [
          notification(:order_created, order.id, %{status: :pending})
        ] ++
          if(order.total_cents >= state.threshold,
            do: [notification(:high_value_order, order.id, %{total_cents: order.total_cents})],
            else: []
          )

      case persist_and_notify(state, Map.put(state.orders, order.id, order), notifications) do
        {:ok, new_state} -> {:reply, {:ok, order}, new_state}
        {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
      end
    else
      true -> {:reply, {:error, :order_already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_order, id}, _from, state) do
    case Map.fetch(state.orders, id) do
      {:ok, order} -> {:reply, {:ok, order}, state}
      :error -> {:reply, {:error, :order_not_found}, state}
    end
  end

  def handle_call(:list_orders, _from, state) do
    {:reply, Map.values(state.orders), state}
  end

  def handle_call(:list_notifications, _from, state) do
    {:reply, state.notifications, state}
  end

  def handle_call({:update_status, id, status}, _from, state) do
    with {:ok, order} <- fetch_order(state.orders, id),
         :ok <- validate_transition(order.status, status) do
      updated_order = Map.put(order, :status, status)
      event = notification(:order_status_changed, id, %{status: status})

      case persist_and_notify(
             state,
             Map.put(state.orders, id, updated_order),
             [event]
           ) do
        {:ok, new_state} -> {:reply, {:ok, updated_order}, new_state}
        {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if MapSet.member?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)

      new_state = %{
        state
        | subscribers: MapSet.put(state.subscribers, pid),
          subscriber_refs: Map.put(state.subscriber_refs, ref, pid)
      }

      {:reply, :ok, new_state}
    end
  end

  def handle_call({:start_worker, order_id}, _from, state) do
    cond do
      not Map.has_key?(state.orders, order_id) ->
        {:reply, {:error, :order_not_found}, state}

      Map.has_key?(state.workers, order_id) ->
        {:reply, {:error, :worker_already_running}, state}

      true ->
        {pid, ref} = spawn_monitor(fn -> worker_loop() end)

        new_state = %{
          state
          | workers: Map.put(state.workers, order_id, {pid, ref}),
            worker_refs: Map.put(state.worker_refs, ref, order_id)
        }

        {:reply, :ok, new_state}
    end
  end

  def handle_call({:stop_worker, order_id}, _from, state) do
    case Map.fetch(state.workers, order_id) do
      {:ok, {pid, _ref}} ->
        send(pid, :stop)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :worker_not_running}, state}
    end
  end

  def handle_call({:crash_worker, order_id}, _from, state) do
    case Map.fetch(state.workers, order_id) do
      {:ok, {pid, _ref}} ->
        send(pid, :crash)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :worker_not_running}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cond do
      Map.has_key?(state.subscriber_refs, ref) ->
        subscriber = Map.fetch!(state.subscriber_refs, ref)

        {:noreply,
         %{
           state
           | subscribers: MapSet.delete(state.subscribers, subscriber),
             subscriber_refs: Map.delete(state.subscriber_refs, ref)
         }}

      Map.has_key?(state.worker_refs, ref) ->
        order_id = Map.fetch!(state.worker_refs, ref)

        state = %{
          state
          | workers: Map.delete(state.workers, order_id),
            worker_refs: Map.delete(state.worker_refs, ref)
        }

        if reason == :normal do
          {:noreply, state}
        else
          event =
            notification(:worker_terminated_unexpectedly, order_id, %{
              reason: inspect(reason)
            })

          notifications = state.notifications ++ [event]
          new_state = %{state | notifications: notifications}

          # A falha de persistência não deve impedir a notificação em tempo real.
          _ = persist(new_state)
          broadcast(new_state, event)
          {:noreply, new_state}
        end

      true ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp worker_loop do
    receive do
      :stop -> :ok
      :crash -> exit(:simulated_failure)
      _message -> worker_loop()
    end
  end

  defp validate_order(attrs) when is_map(attrs) do
    id = Map.get(attrs, :id)
    customer = Map.get(attrs, :customer)
    items = Map.get(attrs, :items)
    total_cents = Map.get(attrs, :total_cents)

    with :ok <- validate_id(id),
         :ok <- validate_non_empty_string(customer, :invalid_customer),
         {:ok, items_total} <- validate_items(items),
         true <- is_integer(total_cents) and total_cents > 0,
         true <- items_total == total_cents do
      {:ok,
       %{
         id: id,
         customer: customer,
         items: items,
         total_cents: total_cents
       }}
    else
      false -> {:error, :invalid_total}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_order(_), do: {:error, :invalid_order}

  defp validate_id(id) when is_integer(id) and id > 0, do: :ok

  defp validate_id(id) when is_binary(id) do
    if String.trim(id) == "", do: {:error, :invalid_id}, else: :ok
  end

  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_non_empty_string(value, _error)
       when is_binary(value) and byte_size(value) > 0 do
    if String.trim(value) == "", do: {:error, :invalid_customer}, else: :ok
  end

  defp validate_non_empty_string(_, error), do: {:error, error}

  defp validate_items(items) when is_list(items) and items != [] do
    Enum.reduce_while(items, {:ok, 0}, fn item, {:ok, total} ->
      case validate_item(item) do
        {:ok, item_total} -> {:cont, {:ok, total + item_total}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_items(_), do: {:error, :invalid_items}

  defp validate_item(item) when is_map(item) do
    name = Map.get(item, :name)
    quantity = Map.get(item, :quantity)
    unit_price_cents = Map.get(item, :unit_price_cents)

    with :ok <- validate_non_empty_string(name, :invalid_item_name),
         true <- is_integer(quantity) and quantity > 0,
         true <- is_integer(unit_price_cents) and unit_price_cents >= 0 do
      {:ok, quantity * unit_price_cents}
    else
      false -> {:error, :invalid_item}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_item(_), do: {:error, :invalid_item}

  defp validate_transition(current, next) do
    cond do
      next not in @statuses ->
        {:error, :invalid_status}

      next not in Map.get(@transitions, current, []) ->
        {:error, :invalid_status_transition}

      true ->
        :ok
    end
  end

  defp fetch_order(orders, id) do
    case Map.fetch(orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :order_not_found}
    end
  end

  defp notification(type, order_id, details) do
    %{
      type: type,
      order_id: order_id,
      details: details,
      at: System.system_time(:millisecond)
    }
  end

  defp persist_and_notify(state, orders, additions) do
    new_state = %{
      state
      | orders: orders,
        notifications: state.notifications ++ additions
    }

    case persist(new_state) do
      :ok ->
        Enum.each(additions, &broadcast(new_state, &1))
        {:ok, new_state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp broadcast(state, event) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:order_notification, event})
    end)
  end

  defp persist(state) do
    data = %{
      version: 1,
      orders: state.orders,
      notifications: state.notifications
    }

    path = state.path
    temp_path = path <> ".tmp." <> Integer.to_string(:erlang.unique_integer([:positive]))

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temp_path, :erlang.term_to_binary(data), [:binary]),
         :ok <- File.rename(temp_path, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temp_path)
        {:error, reason}
    end
  end

  defp load_store(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            %{version: 1, orders: orders, notifications: notifications}
            when is_map(orders) and is_list(notifications) ->
              {:ok, %{orders: orders, notifications: notifications}}

            _ ->
              {:error, :invalid_store_format}
          end
        rescue
          _ -> {:error, :invalid_store_format}
        end

      {:error, :enoent} ->
        {:ok, %{orders: %{}, notifications: []}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
