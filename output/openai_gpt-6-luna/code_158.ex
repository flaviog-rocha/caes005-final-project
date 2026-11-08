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

  defmodule Delivery do
    defstruct [:id, :status, :events, :metadata, :created_at, :updated_at]
  end

  defmodule Event do
    defstruct [:status, :location, :occurred_at]
  end

  @transitions %{
    created: [:picked_up, :failed],
    picked_up: [:in_transit, :failed],
    in_transit: [:delivered, :failed],
    delivered: [],
    failed: []
  }

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    initial_failures = Keyword.get(opts, :initial_failures, 0)
    state = %{deliveries: %{}, idempotency: %{}, fail_next: initial_failures}

    case name do
      nil -> GenServer.start_link(__MODULE__, state)
      _ -> GenServer.start_link(__MODULE__, state, name: name)
    end
  end

  def create(idempotency_key, attributes, opts \\ []) do
    execute(idempotency_key, {:create, attributes}, opts)
  end

  def track(idempotency_key, delivery_id, status, location \\ nil, opts \\ []) do
    execute(idempotency_key, {:track, delivery_id, status, location}, opts)
  end

  def get(delivery_id, server \\ __MODULE__) do
    GenServer.call(server, {:get, delivery_id})
  end

  # Útil para simular falhas temporárias do banco de dados fictício.
  def fail_next(server \\ __MODULE__, count \\ 1)
      when is_integer(count) and count >= 0 do
    GenServer.call(server, {:fail_next, count})
  end

  def init(state), do: {:ok, state}

  def handle_call({:get, delivery_id}, _from, state) do
    reply =
      case Map.fetch(state.deliveries, delivery_id) do
        {:ok, delivery} -> {:ok, delivery}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:fail_next, count}, _from, state) do
    {:reply, :ok, %{state | fail_next: state.fail_next + count}}
  end

  def handle_call({:execute, key, request}, _from, state) do
    case Map.fetch(state.idempotency, key) do
      {:ok, %{request: ^request, response: response}} ->
        {:reply, response, state}

      {:ok, _record} ->
        {:reply, {:error, :idempotency_key_reused}, state}

      :error when state.fail_next > 0 ->
        next_state = %{state | fail_next: state.fail_next - 1}
        {:reply, {:error, {:temporary, :database_unavailable}}, next_state}

      :error ->
        case transact(state, request) do
          {:ok, result, deliveries} ->
            response = {:ok, result}

            record = %{request: request, response: response}

            next_state = %{
              state
              | deliveries: deliveries,
                idempotency: Map.put(state.idempotency, key, record)
            }

            {:reply, response, next_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp execute(key, request, opts) when is_binary(key) and byte_size(key) > 0 do
    server = Keyword.get(opts, :server, __MODULE__)
    max_attempts = opts |> Keyword.get(:max_attempts, 3) |> max(1)
    base_delay = opts |> Keyword.get(:base_delay, 10) |> max(0)
    max_delay = opts |> Keyword.get(:max_delay, 1_000) |> max(0)

    retry(server, key, request, 1, max_attempts, base_delay, max_delay)
  end

  defp execute(_key, _request, _opts), do: {:error, :invalid_idempotency_key}

  defp retry(server, key, request, attempt, max_attempts, base_delay, max_delay) do
    case GenServer.call(server, {:execute, key, request}) do
      {:error, {:temporary, _reason}} = error when attempt >= max_attempts ->
        error

      {:error, {:temporary, _reason}} ->
        exponent = min(attempt - 1, 10)
        delay = min(max_delay, base_delay * Integer.pow(2, exponent))
        if delay > 0, do: Process.sleep(delay)

        retry(
          server,
          key,
          request,
          attempt + 1,
          max_attempts,
          base_delay,
          max_delay
        )

      response ->
        response
    end
  end

  # A operação calcula o novo estado antes de publicar qualquer alteração.
  # Assim, uma falha não deixa entregas ou eventos parcialmente atualizados.
  defp transact(state, {:create, attributes}) when is_map(attributes) do
    with {:ok, id} <- fetch_delivery_id(attributes),
         false <- Map.has_key?(state.deliveries, id) do
      now = DateTime.utc_now()
      initial_event = %Event{status: :created, occurred_at: now}

      delivery = %Delivery{
        id: id,
        status: :created,
        events: [initial_event],
        metadata: Map.delete(attributes, :id),
        created_at: now,
        updated_at: now
      }

      {:ok, delivery, Map.put(state.deliveries, id, delivery)}
    else
      true -> {:error, :delivery_already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  defp transact(state, {:track, delivery_id, status, location}) do
    with {:ok, delivery} <- fetch_delivery(state.deliveries, delivery_id),
         true <- valid_transition?(delivery.status, status) do
      now = DateTime.utc_now()
      event = %Event{status: status, location: location, occurred_at: now}

      updated_delivery = %{
        delivery
        | status: status,
          events: delivery.events ++ [event],
          updated_at: now
      }

      {:ok, updated_delivery, Map.put(state.deliveries, delivery_id, updated_delivery)}
    else
      false -> {:error, :invalid_status_transition}
      {:error, reason} -> {:error, reason}
    end
  end

  defp transact(_state, _request), do: {:error, :invalid_operation}

  defp fetch_delivery_id(%{id: id}) when is_binary(id) and byte_size(id) > 0,
    do: {:ok, id}

  defp fetch_delivery_id(_attributes), do: {:error, :invalid_delivery_id}

  defp fetch_delivery(deliveries, id) do
    case Map.fetch(deliveries, id) do
      {:ok, delivery} -> {:ok, delivery}
      :error -> {:error, :not_found}
    end
  end

  defp valid_transition?(current_status, next_status) do
    next_status in Map.get(@transitions, current_status, [])
  end
end
