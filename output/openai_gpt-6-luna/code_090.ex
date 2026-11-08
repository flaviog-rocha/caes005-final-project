# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Executar determinadas operações periodicamente.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  use GenServer
  require Logger

  @statuses [:pending, :in_transit, :delivered, :cancelled]
  @active_statuses [:pending, :in_transit]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  def apply_operation(server, operation) do
    with :ok <- validate_operation(operation) do
      GenServer.call(server, {:apply_operation, operation})
    end
  end

  def ingest(server, enumerable) do
    if Enumerable.impl_for(enumerable) do
      stats =
        enumerable
        |> Stream.map(&apply_operation(server, &1))
        |> Enum.reduce(%{applied: 0, duplicates: 0, rejected: 0}, fn
          {:ok, :duplicate}, acc -> %{acc | duplicates: acc.duplicates + 1}
          {:ok, _result}, acc -> %{acc | applied: acc.applied + 1}
          {:error, _reason}, acc -> %{acc | rejected: acc.rejected + 1}
        end)

      {:ok, stats}
    else
      {:error, :invalid_enumerable}
    end
  end

  def get_delivery(server, delivery_id) when is_binary(delivery_id) do
    GenServer.call(server, {:get_delivery, delivery_id})
  end

  def get_delivery(_server, _delivery_id), do: {:error, :invalid_delivery_id}

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    notify_pid = Keyword.get(opts, :notify_pid)

    cond do
      not is_integer(interval) or interval <= 0 ->
        {:stop, :invalid_interval}

      not is_nil(notify_pid) and not is_pid(notify_pid) ->
        {:stop, :invalid_notify_pid}

      true ->
        schedule_check(interval)

        {:ok,
         %{
           interval: interval,
           notify_pid: notify_pid,
           deliveries: %{},
           operation_keys: MapSet.new(),
           overdue_notified: MapSet.new()
         }}
    end
  end

  @impl true
  def handle_call({:apply_operation, operation}, _from, state) do
    key = Map.fetch!(operation, :key)

    if MapSet.member?(state.operation_keys, key) do
      {:reply, {:ok, :duplicate}, state}
    else
      case execute_operation(operation, state) do
        {:ok, result, new_state} ->
          keys = MapSet.put(new_state.operation_keys, key)
          {:reply, {:ok, result}, %{new_state | operation_keys: keys}}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  def handle_call({:get_delivery, delivery_id}, _from, state) do
    case Map.fetch(state.deliveries, delivery_id) do
      {:ok, delivery} -> {:reply, {:ok, delivery}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info(:periodic_check, state) do
    now = DateTime.utc_now()

    new_state =
      state.deliveries
      |> Stream.filter(fn {_id, delivery} ->
        delivery.status in @active_statuses and
          DateTime.compare(delivery.due_at, now) != :gt
      end)
      |> Enum.reduce(state, fn {id, delivery}, acc ->
        if MapSet.member?(acc.overdue_notified, id) do
          acc
        else
          notify(acc, %{
            type: :delivery_overdue,
            delivery_id: id,
            due_at: delivery.due_at,
            detected_at: now
          })

          %{acc | overdue_notified: MapSet.put(acc.overdue_notified, id)}
        end
      end)

    schedule_check(new_state.interval)
    {:noreply, new_state}
  end

  defp execute_operation(%{type: :create, delivery: delivery}, state) do
    id = delivery.id

    if Map.has_key?(state.deliveries, id) do
      {:error, :delivery_already_exists}
    else
      now = DateTime.utc_now()

      record = %{
        id: id,
        recipient: delivery.recipient,
        address: delivery.address,
        due_at: delivery.due_at,
        status: :pending,
        created_at: now,
        updated_at: now
      }

      {:ok, :created, put_in(state.deliveries[id], record)}
    end
  end

  defp execute_operation(%{type: :status, delivery_id: id, status: status}, state) do
    case Map.fetch(state.deliveries, id) do
      :error ->
        {:error, :delivery_not_found}

      {:ok, delivery} ->
        if valid_transition?(delivery.status, status) do
          updated = %{delivery | status: status, updated_at: DateTime.utc_now()}
          new_state = put_in(state.deliveries[id], updated)

          if status == :delivered and delivery.status != :delivered do
            notify(new_state, %{
              type: :delivery_delivered,
              delivery_id: id,
              delivered_at: updated.updated_at
            })
          end

          {:ok, :updated, new_state}
        else
          {:error, :invalid_status_transition}
        end
    end
  end

  defp valid_transition?(current, target) when current == target, do: true
  defp valid_transition?(:pending, target), do: target in [:in_transit, :delivered, :cancelled]
  defp valid_transition?(:in_transit, target), do: target in [:delivered, :cancelled]
  defp valid_transition?(_, _), do: false

  defp validate_operation(operation) when is_map(operation) do
    key = Map.get(operation, :key)

    cond do
      not valid_text?(key) ->
        {:error, :invalid_operation_key}

      Map.get(operation, :type) == :create ->
        validate_delivery(Map.get(operation, :delivery))

      Map.get(operation, :type) == :status ->
        if valid_text?(Map.get(operation, :delivery_id)) and
             Map.get(operation, :status) in @statuses do
          :ok
        else
          {:error, :invalid_status_operation}
        end

      true ->
        {:error, :invalid_operation}
    end
  end

  defp validate_operation(_), do: {:error, :invalid_operation}

  defp validate_delivery(delivery) when is_map(delivery) do
    if valid_text?(Map.get(delivery, :id)) and
         valid_text?(Map.get(delivery, :recipient)) and
         valid_text?(Map.get(delivery, :address)) and
         valid_datetime?(Map.get(delivery, :due_at)) do
      :ok
    else
      {:error, :invalid_delivery}
    end
  end

  defp validate_delivery(_), do: {:error, :invalid_delivery}

  defp valid_text?(value), do: is_binary(value) and byte_size(value) > 0

  defp valid_datetime?(value) do
    if match?(%DateTime{}, value) do
      try do
        _ = DateTime.to_unix(value)
        true
      rescue
        _ -> false
      end
    else
      false
    end
  end

  defp notify(%{notify_pid: pid}, event) when is_pid(pid) do
    send(pid, {:delivery_notification, event})
  end

  defp notify(_state, event) do
    Logger.warning("Delivery notification: #{inspect(event)}")
  end

  defp schedule_check(interval) do
    Process.send_after(self(), :periodic_check, interval)
  end
end
