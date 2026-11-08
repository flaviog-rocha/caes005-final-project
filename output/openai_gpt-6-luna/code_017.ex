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

defmodule Ecommerce.OrderManager do
  use GenServer
  use Supervisor

  @statuses [:pending, :paid, :processing, :shipped, :cancelled]
  @order_fields [:id, :status, :items, :total, :metadata]
  @high_value_threshold 1_000

  defmacro notify_if(condition, event) do
    quote do
      if unquote(condition), do: [unquote(event)], else: []
    end
  end

  def start_link(opts \\ []) do
    supervisor_opts =
      case Keyword.get(opts, :supervisor_name) do
        nil -> []
        name -> [name: name]
      end

    Supervisor.start_link(__MODULE__, opts, supervisor_opts)
  end

  @impl Supervisor
  def init(opts) do
    task_supervisor_name =
      Keyword.get(opts, :task_supervisor_name, Module.concat(__MODULE__, TaskSupervisor))

    server_name = Keyword.get(opts, :name, __MODULE__)

    children = [
      {Task.Supervisor, name: task_supervisor_name},
      %{
        id: __MODULE__,
        start:
          {GenServer, :start_link,
           [__MODULE__, {opts, task_supervisor_name}, server_start_opts(server_name)]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_order(server, attrs, opts \\ []) when is_map(attrs) do
    GenServer.call(server, {:create_order, attrs, opts})
  end

  def get_order(server, id) do
    GenServer.call(server, {:get_order, id})
  end

  def update_order(server, id, changes) when is_map(changes) do
    GenServer.call(server, {:update_order, id, changes})
  end

  def update_order_async(server, id, changes, opts \\ []) when is_map(changes) do
    GenServer.call(server, {:update_order_async, id, changes, opts})
  end

  def get_operation(server, operation_id) do
    GenServer.call(server, {:get_operation, operation_id})
  end

  def list_operations(server) do
    GenServer.call(server, :list_operations)
  end

  def subscribe(server, subscriber \\ self()) when is_pid(subscriber) do
    GenServer.call(server, {:subscribe, subscriber})
  end

  def unsubscribe(server, subscriber \\ self()) when is_pid(subscriber) do
    GenServer.call(server, {:unsubscribe, subscriber})
  end

  @impl GenServer
  def init({opts, task_supervisor}) do
    sweep_interval = Keyword.get(opts, :sweep_interval, 1_000)

    state = %{
      orders: %{},
      operations: %{},
      subscribers: MapSet.new(),
      task_supervisor: task_supervisor,
      default_ttl: Keyword.get(opts, :order_ttl, 86_400_000),
      operation_ttl: Keyword.get(opts, :operation_ttl, 300_000),
      sweep_interval: sweep_interval
    }

    schedule_sweep(sweep_interval)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:create_order, attrs, opts}, _from, state) do
    id = Map.get(attrs, :id)
    ttl = Keyword.get(opts, :ttl, state.default_ttl)

    with true <- is_integer(ttl) and ttl > 0,
         false <- Map.has_key?(state.orders, id),
         order <- build_order(attrs, id, ttl),
         :ok <- validate_order(order) do
      orders = Map.put(state.orders, id, order)
      events = creation_notifications(order)
      notify_subscribers(state.subscribers, events)

      {:reply, {:ok, order}, %{state | orders: orders}}
    else
      true -> {:reply, {:error, :invalid_ttl}, state}
      false -> {:reply, {:error, :order_already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_order, id}, _from, state) do
    case Map.fetch(state.orders, id) do
      {:ok, order} -> {:reply, {:ok, order}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:update_order, id, changes}, _from, state) do
    apply_order_update(id, changes, nil, state)
  end

  def handle_call({:update_order_async, id, changes, opts}, _from, state) do
    operation_id = make_ref()
    ttl = Keyword.get(opts, :ttl, state.operation_ttl)

    if is_integer(ttl) and ttl > 0 do
      operation = %{
        id: operation_id,
        order_id: id,
        status: :pending,
        started_at: now_ms(),
        expires_at: now_ms() + ttl,
        result: nil
      }

      server = self()

      case Task.Supervisor.start_child(state.task_supervisor, fn ->
             result =
               try do
                 GenServer.call(server, {:apply_async_update, id, changes, operation_id}, :infinity)
               catch
                 :exit, reason -> {:error, {:task_exit, reason}}
               end

             GenServer.cast(server, {:finish_operation, operation_id, result})
           end) do
        {:ok, _pid} ->
          operations = Map.put(state.operations, operation_id, operation)
          {:reply, {:ok, operation_id}, %{state | operations: operations}}

        {:error, reason} ->
          {:reply, {:error, {:task_start_failed, reason}}, state}
      end
    else
      {:reply, {:error, :invalid_ttl}, state}
    end
  end

  def handle_call({:apply_async_update, id, changes, operation_id}, _from, state) do
    case Map.get(state.operations, operation_id) do
      %{status: :pending, expires_at: expires_at} when expires_at > now_ms() ->
        apply_order_update(id, changes, operation_id, state)

      _ ->
        {:reply, {:error, :operation_expired}, state}
    end
  end

  def handle_call({:get_operation, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_operations, _from, state) do
    {:reply, Map.values(state.operations), state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, subscriber)}}
  end

  def handle_call({:unsubscribe, subscriber}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.delete(state.subscribers, subscriber)}}
  end

  @impl GenServer
  def handle_cast({:finish_operation, operation_id, result}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        status =
          cond do
            operation.expires_at <= now_ms() -> :expired
            match?({:ok, _}, result) -> :completed
            true -> :failed
          end

        updated_operation = %{operation | status: status, result: result}
        operations = Map.put(state.operations, operation_id, updated_operation)
        {:noreply, %{state | operations: operations}}

      :error ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    now = now_ms()

    expired_orders =
      state.orders
      |> Enum.filter(fn {_id, order} -> order.expires_at <= now end)

    orders =
      Enum.reduce(expired_orders, state.orders, fn {id, _order}, acc ->
        Map.delete(acc, id)
      end)

    expired_operations =
      state.operations
      |> Enum.filter(fn {_id, operation} -> operation.expires_at <= now end)

    operations =
      Enum.reduce(expired_operations, state.operations, fn {id, _operation}, acc ->
        Map.delete(acc, id)
      end)

    expiry_events =
      Enum.map(expired_orders, fn {id, _order} -> {:order_expired, id} end)

    notify_subscribers(state.subscribers, expiry_events)
    schedule_sweep(state.sweep_interval)

    {:noreply, %{state | orders: orders, operations: operations}}
  end

  defp apply_order_update(id, changes, _operation_id, state) do
    with {:ok, current_order} <- fetch_order(state.orders, id),
         :ok <- validate_changes(changes),
         updated_order <- Map.merge(current_order, changes),
         :ok <- validate_order(updated_order) do
      orders = Map.put(state.orders, id, updated_order)
      events = update_notifications(current_order, updated_order)
      notify_subscribers(state.subscribers, events)

      {:reply, {:ok, updated_order}, %{state | orders: orders}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp fetch_order(orders, id) do
    case Map.fetch(orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :not_found}
    end
  end

  defp build_order(attrs, id, ttl) do
    order =
      Map.take(attrs, @order_fields)
      |> Map.merge(%{status: :pending, items: [], total: 0, metadata: %{}})

    Map.merge(order, %{
      id: id,
      expires_at: now_ms() + ttl
    })
  end

  defp validate_changes(changes) do
    if map_size(changes) > 0 and Enum.all?(Map.keys(changes), &(&1 in @order_fields and &1 != :id)) do
      :ok
    else
      {:error, :invalid_changes}
    end
  end

  defp validate_order(order) do
    cond do
      is_nil(order.id) ->
        {:error, :missing_id}

      order.status not in @statuses ->
        {:error, :invalid_status}

      not is_list(order.items) ->
        {:error, :invalid_items}

      not (is_number(order.total) and order.total >= 0) ->
        {:error, :invalid_total}

      not is_map(order.metadata) ->
        {:error, :invalid_metadata}

      true ->
        :ok
    end
  end

  defp creation_notifications(order) do
    notify_if(true, {:order_created, order.id}) ++
      notify_if(order.total >= @high_value_threshold, {:high_value_order, order.id, order.total})
  end

  defp update_notifications(previous, order) do
    notify_if(previous.status != :paid and order.status == :paid, {:order_paid, order.id}) ++
      notify_if(previous.status != :shipped and order.status == :shipped, {:order_shipped, order.id}) ++
      notify_if(
        previous.total < @high_value_threshold and order.total >= @high_value_threshold,
        {:high_value_order, order.id, order.total}
      )
  end

  defp notify_subscribers(subscribers, events) do
    Enum.each(events, fn event ->
      Enum.each(subscribers, &send(&1, {:ecommerce_notification, event}))
    end)
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :sweep, interval)
  end

  defp now_ms do
    System.monotonic_time(:millisecond)
  end

  defp server_start_opts(nil), do: []
  defp server_start_opts(name), do: [name: name]
end
