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

  @batch_size 500
  @default_interval_ms 60_000
  @statuses [:pending, :in_transit, :delivered, :delayed]

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  def add_delivery(server, delivery) do
    with {:ok, normalized} <- validate_delivery(delivery) do
      GenServer.call(server, {:add_delivery, normalized})
    end
  end

  def update_status(server, operation_id, delivery_id, status) do
    with :ok <- validate_operation_id(operation_id),
         :ok <- validate_delivery_id(delivery_id),
         :ok <- validate_status(status) do
      GenServer.call(server, {:update_status, operation_id, delivery_id, status})
    end
  end

  def get_delivery(server, delivery_id) do
    with :ok <- validate_delivery_id(delivery_id) do
      GenServer.call(server, {:get_delivery, delivery_id})
    end
  end

  def ingest_stream(server, enumerable, batch_size \\ @batch_size)

  def ingest_stream(server, enumerable, batch_size)
      when is_integer(batch_size) and batch_size > 0 do
    enumerable
    |> Stream.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, 0}, fn batch, {:ok, count} ->
      case validate_batch(batch) do
        {:ok, deliveries} ->
          case insert_batch(server, deliveries, count) do
            {:ok, new_count} -> {:cont, {:ok, new_count}}
            {:error, reason, new_count} -> {:halt, {:error, reason, new_count}}
          end

        {:error, reason} ->
          {:halt, {:error, reason, count}}
      end
    end)
  end

  def ingest_stream(_server, _enumerable, _batch_size),
    do: {:error, :invalid_batch_size, 0}

  # GenServer callbacks

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    target = Keyword.get(opts, :notification_target)

    cond do
      not is_integer(interval) or interval <= 0 ->
        {:stop, :invalid_interval}

      not (is_nil(target) or is_pid(target)) ->
        {:stop, :invalid_notification_target}

      true ->
        Process.send_after(self(), :periodic_sweep, interval)

        {:ok,
         %{
           deliveries: %{},
           operations: MapSet.new(),
           notified: MapSet.new(),
           interval_ms: interval,
           notification_target: target
         }}
    end
  end

  @impl true
  def handle_call({:add_delivery, delivery}, _from, state) do
    id = delivery.id

    if Map.has_key?(state.deliveries, id) do
      {:reply, {:ok, :duplicate}, state}
    else
      deliveries = Map.put(state.deliveries, id, delivery)
      {:reply, {:ok, :inserted}, %{state | deliveries: deliveries}}
    end
  end

  def handle_call({:update_status, operation_id, delivery_id, status}, _from, state) do
    cond do
      MapSet.member?(state.operations, operation_id) ->
        {:reply, {:ok, :duplicate}, state}

      not Map.has_key?(state.deliveries, delivery_id) ->
        {:reply, {:error, :delivery_not_found}, state}

      true ->
        delivery =
          state.deliveries
          |> Map.fetch!(delivery_id)
          |> Map.put(:status, status)

        state =
          state
          |> put_in([:deliveries, delivery_id], delivery)
          |> update_in([:operations], &MapSet.put(&1, operation_id))
          |> maybe_notify_status(delivery)

        {:reply, {:ok, :updated}, state}
    end
  end

  def handle_call({:get_delivery, delivery_id}, _from, state) do
    case Map.fetch(state.deliveries, delivery_id) do
      {:ok, delivery} -> {:reply, {:ok, delivery}, state}
      :error -> {:reply, {:error, :delivery_not_found}, state}
    end
  end

  @impl true
  def handle_info(:periodic_sweep, state) do
    now = DateTime.utc_now()

    state =
      Enum.reduce(state.deliveries, state, fn {_id, delivery}, acc ->
        if delivery.status != :delivered and DateTime.compare(delivery.due_at, now) != :gt do
          notify_once(acc, {:overdue, delivery.id}, %{
            type: :overdue,
            delivery_id: delivery.id,
            destination: delivery.destination,
            due_at: delivery.due_at,
            status: delivery.status
          })
        else
          acc
        end
      end)

    Process.send_after(self(), :periodic_sweep, state.interval_ms)
    {:noreply, state}
  end

  # Internal helpers

  defp validate_batch(batch) do
    Enum.reduce_while(batch, {:ok, []}, fn delivery, {:ok, acc} ->
      case validate_delivery(delivery) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, deliveries} -> {:ok, Enum.reverse(deliveries)}
      error -> error
    end
  end

  defp insert_batch(server, deliveries, count) do
    Enum.reduce_while(deliveries, {:ok, count}, fn delivery, {:ok, current_count} ->
      case GenServer.call(server, {:add_delivery, delivery}) do
        {:ok, :inserted} -> {:cont, {:ok, current_count + 1}}
        {:ok, :duplicate} -> {:cont, {:ok, current_count}}
        {:error, reason} -> {:halt, {:error, reason, current_count}}
      end
    end)
  end

  defp validate_delivery(delivery) when is_map(delivery) do
    delivery = Map.put_new(delivery, :status, :pending)

    cond do
      not valid_delivery_id?(Map.get(delivery, :id)) ->
        {:error, :invalid_delivery_id}

      not valid_non_empty_string?(Map.get(delivery, :destination)) ->
        {:error, :invalid_destination}

      not match?(%DateTime{}, Map.get(delivery, :due_at)) ->
        {:error, :invalid_due_at}

      Map.get(delivery, :status) not in @statuses ->
        {:error, :invalid_status}

      true ->
        {:ok, delivery}
    end
  end

  defp validate_delivery(_), do: {:error, :invalid_delivery}

  defp validate_delivery_id(id) do
    if valid_delivery_id?(id), do: :ok, else: {:error, :invalid_delivery_id}
  end

  defp validate_operation_id(id) do
    if valid_non_empty_string?(id), do: :ok, else: {:error, :invalid_operation_id}
  end

  defp validate_status(status) do
    if status in @statuses, do: :ok, else: {:error, :invalid_status}
  end

  defp valid_delivery_id?(id),
    do: (is_binary(id) and byte_size(id) > 0) or (is_integer(id) and id >= 0)

  defp valid_non_empty_string?(value),
    do: is_binary(value) and byte_size(value) > 0

  defp maybe_notify_status(state, %{status: status} = delivery)
       when status in [:delivered, :delayed] do
    notification = %{
      type: status,
      delivery_id: delivery.id,
      destination: delivery.destination
    }

    notify_once(state, {:status, status, delivery.id}, notification)
  end

  defp maybe_notify_status(state, _delivery), do: state

  defp notify_once(state, key, notification) do
    if MapSet.member?(state.notified, key) do
      state
    else
      dispatch_notification(state.notification_target, notification)
      %{state | notified: MapSet.put(state.notified, key)}
    end
  end

  defp dispatch_notification(target, notification) when is_pid(target) do
    send(target, {:delivery_notification, notification})
  end

  defp dispatch_notification(nil, notification) do
    :logger.notice("Delivery notification: ~p", [notification])
  end
end
