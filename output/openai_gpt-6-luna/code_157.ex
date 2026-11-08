# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  use GenServer

  @transitions %{
    pending: [:picked_up, :cancelled],
    picked_up: [:in_transit, :cancelled],
    in_transit: [:delivered, :failed]
  }

  # In-memory store. Operations and delivery changes are committed together
  # in a single GenServer callback.
  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, nil} -> GenServer.start_link(__MODULE__, %{})
      {:ok, name} -> GenServer.start_link(__MODULE__, %{}, name: name)
      :error -> GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
    end
  end

  @impl true
  def init(_opts) do
    {:ok, %{deliveries: %{}, operations: %{}}}
  end

  def create_delivery(operation_id, attrs, opts \\ []) do
    execute(operation_id, {:create, attrs}, opts)
  end

  def update_status(operation_id, delivery_id, status, location \\ nil, opts \\ []) do
    execute(operation_id, {:status, delivery_id, status, location}, opts)
  end

  def add_tracking_event(operation_id, delivery_id, location, note \\ nil, opts \\ []) do
    execute(operation_id, {:tracking_event, delivery_id, location, note}, opts)
  end

  def get_delivery(delivery_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    safe_call(server, {:get_delivery, delivery_id}, opts)
  end

  def list_deliveries(opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    safe_call(server, :list_deliveries, opts)
  end

  # Retries only explicitly temporary failures. max_attempts includes the
  # first invocation. The callback should be safe to repeat.
  def retry(fun, opts \\ []) when is_function(fun, 0) do
    max_attempts = positive_integer(Keyword.get(opts, :max_attempts, 3), 3)
    backoff_ms = non_negative_integer(Keyword.get(opts, :backoff_ms, 25), 25)
    max_backoff_ms = non_negative_integer(Keyword.get(opts, :max_backoff_ms, 500), 500)

    retry_loop(fun, 1, max_attempts, backoff_ms, max_backoff_ms)
  end

  @impl true
  def handle_call({:execute, operation_id, request}, _from, state) do
    cond do
      not valid_id?(operation_id) ->
        {:reply, {:error, :invalid_operation_id}, state}

      Map.has_key?(state.operations, operation_id) ->
        previous = Map.fetch!(state.operations, operation_id)

        if previous.request == request do
          {:reply, previous.reply, state}
        else
          {:reply, {:error, :idempotency_key_conflict}, state}
        end

      true ->
        case perform(request, state) do
          {:ok, value, updated_state} ->
            reply = {:ok, value}

            operations =
              Map.put(updated_state.operations, operation_id, %{
                request: request,
                reply: reply
              })

            {:reply, reply, %{updated_state | operations: operations}}

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

  def handle_call(:list_deliveries, _from, state) do
    {:reply, {:ok, Map.values(state.deliveries)}, state}
  end

  defp execute(operation_id, request, opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    safe_call(server, {:execute, operation_id, request}, opts)
  end

  defp safe_call(server, message, opts) do
    timeout = positive_integer(Keyword.get(opts, :timeout, 5_000), 5_000)

    retry(
      fn ->
        try do
          GenServer.call(server, message, timeout)
        catch
          :exit, reason -> {:error, {:temporary, reason}}
        end
      end,
      opts
    )
  end

  defp perform({:create, attrs}, state) when is_map(attrs) do
    delivery_id = Map.get(attrs, :id)

    cond do
      not valid_id?(delivery_id) ->
        {:error, :invalid_delivery_id}

      Map.has_key?(state.deliveries, delivery_id) ->
        {:error, :delivery_already_exists}

      true ->
        now = DateTime.utc_now()

        delivery =
          Map.merge(attrs, %{
            id: delivery_id,
            status: :pending,
            events: [],
            created_at: now,
            updated_at: now
          })

        deliveries = Map.put(state.deliveries, delivery_id, delivery)
        {:ok, delivery, %{state | deliveries: deliveries}}
    end
  end

  defp perform({:create, _attrs}, _state), do: {:error, :invalid_delivery}

  defp perform({:status, delivery_id, new_status, location}, state) do
    case Map.fetch(state.deliveries, delivery_id) do
      :error ->
        {:error, :not_found}

      {:ok, delivery} ->
        allowed = Map.get(@transitions, delivery.status, [])

        if new_status in allowed do
          now = DateTime.utc_now()

          event = %{
            type: :status_change,
            status: new_status,
            location: location,
            occurred_at: now
          }

          updated_delivery =
            delivery
            |> Map.put(:status, new_status)
            |> Map.update!(:events, &(&1 ++ [event]))
            |> Map.put(:updated_at, now)

          deliveries = Map.put(state.deliveries, delivery_id, updated_delivery)
          {:ok, updated_delivery, %{state | deliveries: deliveries}}
        else
          {:error, :invalid_status_transition}
        end
    end
  end

  defp perform({:tracking_event, delivery_id, location, note}, state) do
    case Map.fetch(state.deliveries, delivery_id) do
      :error ->
        {:error, :not_found}

      {:ok, _delivery} when is_nil(location) ->
        {:error, :invalid_location}

      {:ok, delivery} ->
        now = DateTime.utc_now()

        event = %{
          type: :tracking,
          status: delivery.status,
          location: location,
          note: note,
          occurred_at: now
        }

        updated_delivery =
          delivery
          |> Map.update!(:events, &(&1 ++ [event]))
          |> Map.put(:updated_at, now)

        deliveries = Map.put(state.deliveries, delivery_id, updated_delivery)
        {:ok, updated_delivery, %{state | deliveries: deliveries}}
    end
  end

  defp perform(_request, _state), do: {:error, :invalid_operation}

  defp retry_loop(fun, attempt, max_attempts, backoff_ms, max_backoff_ms) do
    result = fun.()

    if temporary_failure?(result) and attempt < max_attempts do
      delay =
        if backoff_ms == 0 do
          0
        else
          min(backoff_ms * trunc(:math.pow(2, attempt - 1)), max_backoff_ms)
        end

      if delay > 0, do: Process.sleep(delay)
      retry_loop(fun, attempt + 1, max_attempts, backoff_ms, max_backoff_ms)
    else
      result
    end
  end

  defp temporary_failure?({:error, :temporary}), do: true
  defp temporary_failure?({:error, {:temporary, _reason}}), do: true
  defp temporary_failure?(_result), do: false

  defp valid_id?(id), do: is_binary(id) and byte_size(id) > 0

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp non_negative_integer(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value, default), do: default
end
