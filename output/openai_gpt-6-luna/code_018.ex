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

defmodule Ecommerce.OrderNotifications do
  defmacro status_notifiable?(status) do
    quote do
      unquote(status) in [:paid, :shipped, :delivered]
    end
  end
end

defmodule Ecommerce.OrderManager do
  use GenServer
  require Ecommerce.OrderNotifications

  @create_fields [:customer, :items, :total]
  @update_fields [:customer, :items, :total, :status]
  @statuses [:pending, :paid, :shipped, :delivered, :cancelled]
  @default_ttl 86_400_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def create_order(server, attrs, ttl \\ @default_ttl) do
    GenServer.call(server, {:create_order, attrs, ttl})
  end

  def update_order(server, order_id, attrs) do
    GenServer.call(server, {:update_order, order_id, attrs})
  end

  def get_order(server, order_id) do
    GenServer.call(server, {:get_order, order_id})
  end

  def list_orders(server) do
    GenServer.call(server, :list_orders)
  end

  def get_operation(server, operation_id) do
    GenServer.call(server, {:get_operation, operation_id})
  end

  def list_operations(server) do
    GenServer.call(server, :list_operations)
  end

  def subscribe(server, subscriber \\ self()) do
    GenServer.call(server, {:subscribe, subscriber})
  end

  def notifications(server) do
    GenServer.call(server, :notifications)
  end

  @impl true
  def init(_opts) do
    case Task.Supervisor.start_link() do
      {:ok, task_supervisor} ->
        {:ok,
         %{
           task_supervisor: task_supervisor,
           next_order_id: 1,
           next_operation_id: 1,
           orders: %{},
           operations: %{},
           expiry_tokens: %{},
           subscribers: %{},
           notifications: []
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create_order, attrs, ttl}, _from, state) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_create_attrs(attrs),
         :ok <- validate_ttl(ttl) do
      id = state.next_order_id
      now = now_ms()
      token = make_ref()

      order =
        attrs
        |> Map.put(:id, id)
        |> Map.put(:status, :pending)
        |> Map.put(:created_at, now)
        |> Map.put(:updated_at, now)
        |> Map.put(:expires_at, now + ttl)

      Process.send_after(self(), {:expire_order, id, token}, ttl)

      state = %{
        state
        | next_order_id: id + 1,
          orders: Map.put(state.orders, id, order),
          expiry_tokens: Map.put(state.expiry_tokens, id, token)
      }

      {:reply, {:ok, order}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:update_order, order_id, attrs}, _from, state) do
    with {:ok, _order} <- fetch_order(state, order_id),
         {:ok, attrs} <- normalize_attrs(attrs) do
      operation_id = state.next_operation_id
      server = self()

      case Task.Supervisor.start_child(state.task_supervisor, fn ->
             GenServer.cast(server, {:execute_update, operation_id, order_id, attrs})
           end) do
        {:ok, _task_pid} ->
          operation = %{
            id: operation_id,
            order_id: order_id,
            status: :queued,
            started_at: now_ms(),
            finished_at: nil,
            error: nil
          }

          state = %{
            state
            | next_operation_id: operation_id + 1,
              operations: Map.put(state.operations, operation_id, operation)
          }

          {:reply, {:ok, operation_id}, state}

        {:error, reason} ->
          {:reply, {:error, {:task_start_failed, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_order, order_id}, _from, state) do
    {:reply, fetch_order(state, order_id), state}
  end

  def handle_call(:list_orders, _from, state) do
    {:reply, state.orders |> Map.values() |> Enum.sort_by(& &1.id), state}
  end

  def handle_call({:get_operation, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :operation_not_found}, state}
    end
  end

  def handle_call(:list_operations, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, operations, state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) when is_pid(subscriber) do
    subscribers =
      if Map.has_key?(state.subscribers, subscriber) do
        state.subscribers
      else
        Map.put(state.subscribers, subscriber, Process.monitor(subscriber))
      end

    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_call(:notifications, _from, state) do
    {:reply, Enum.reverse(state.notifications), state}
  end

  @impl true
  def handle_cast({:execute_update, operation_id, order_id, attrs}, state) do
    case {Map.fetch(state.operations, operation_id), Map.fetch(state.orders, order_id)} do
      {{:ok, %{status: :queued} = operation}, {:ok, order}} ->
        case validate_update_attrs(attrs) do
          :ok ->
            updated_order =
              order
              |> Map.merge(attrs)
              |> Map.put(:updated_at, now_ms())

            completed_operation =
              operation
              |> Map.put(:status, :succeeded)
              |> Map.put(:finished_at, now_ms())

            state = %{
              state
              | orders: Map.put(state.orders, order_id, updated_order),
                operations: Map.put(state.operations, operation_id, completed_operation)
            }

            state =
              if Ecommerce.OrderNotifications.status_notifiable?(updated_order.status) do
                emit_notification(state, %{
                  type: :status_reached,
                  order_id: order_id,
                  status: updated_order.status
                })
              else
                state
              end

            {:noreply, state}

          {:error, reason} ->
            failed_operation =
              operation
              |> Map.put(:status, :failed)
              |> Map.put(:finished_at, now_ms())
              |> Map.put(:error, reason)

            {:noreply,
             %{state | operations: Map.put(state.operations, operation_id, failed_operation)}}
        end

      {{:ok, %{status: :queued} = operation}, :error} ->
        failed_operation =
          operation
          |> Map.put(:status, :failed)
          |> Map.put(:finished_at, now_ms())
          |> Map.put(:error, :order_not_found)

        {:noreply,
         %{state | operations: Map.put(state.operations, operation_id, failed_operation)}}

      _ ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:expire_order, order_id, token}, state) do
    if Map.get(state.expiry_tokens, order_id) == token do
      state = %{
        state
        | orders: Map.delete(state.orders, order_id),
          expiry_tokens: Map.delete(state.expiry_tokens, order_id)
      }

      {:noreply,
       emit_notification(state, %{type: :order_expired, order_id: order_id})}
    else
      {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, subscriber, _reason}, state) do
    case Map.get(state.subscribers, subscriber) do
      ^ref ->
        {:noreply, %{state | subscribers: Map.delete(state.subscribers, subscriber)}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}

  defp normalize_attrs(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      {:ok, Map.new(attrs)}
    else
      {:error, :invalid_attributes}
    end
  end

  defp normalize_attrs(_attrs), do: {:error, :invalid_attributes}

  defp validate_create_attrs(attrs) do
    cond do
      not fields_allowed?(attrs, @create_fields) -> {:error, :invalid_attributes}
      not valid_items?(Map.get(attrs, :items, [])) -> {:error, :invalid_items}
      not valid_total?(Map.get(attrs, :total, 0)) -> {:error, :invalid_total}
      true -> :ok
    end
  end

  defp validate_update_attrs(attrs) do
    cond do
      not fields_allowed?(attrs, @update_fields) -> {:error, :invalid_attributes}
      Map.has_key?(attrs, :items) and not valid_items?(attrs.items) -> {:error, :invalid_items}
      Map.has_key?(attrs, :total) and not valid_total?(attrs.total) -> {:error, :invalid_total}
      Map.has_key?(attrs, :status) and attrs.status not in @statuses -> {:error, :invalid_status}
      true -> :ok
    end
  end

  defp fields_allowed?(attrs, allowed_fields) do
    Enum.all?(Map.keys(attrs), &(&1 in allowed_fields))
  end

  defp valid_items?(items), do: is_list(items)
  defp valid_total?(total), do: is_number(total) and total >= 0

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0, do: :ok
  defp validate_ttl(_ttl), do: {:error, :invalid_ttl}

  defp fetch_order(state, order_id) do
    case Map.fetch(state.orders, order_id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :order_not_found}
    end
  end

  defp emit_notification(state, notification) do
    notification = Map.put(notification, :at, now_ms())

    Enum.each(Map.keys(state.subscribers), fn subscriber ->
      send(subscriber, {:order_notification, notification})
    end)

    %{state | notifications: [notification | state.notifications]}
  end

  defp now_ms, do: System.system_time(:millisecond)
end
