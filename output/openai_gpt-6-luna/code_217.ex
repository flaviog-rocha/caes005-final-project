# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Implementar expiração automática de registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Fazer uso de macros.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker.Validation do
  defmacro validate_keys(attrs, required, allowed) do
    quote do
      value = unquote(attrs)

      if is_map(value) do
        required_keys = unquote(required)
        allowed_keys = unquote(allowed)
        missing = Enum.reject(required_keys, &Map.has_key?(value, &1))
        unknown = Map.keys(value) -- allowed_keys

        cond do
          missing != [] -> {:error, {:missing_fields, missing}}
          unknown != [] -> {:error, {:unknown_fields, unknown}}
          true -> {:ok, value}
        end
      else
        {:error, :invalid_attributes}
      end
    end
  end
end

defmodule DeliveryTracker do
  use Supervisor

  @server DeliveryTracker.Server

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: DeliveryTracker.Supervisor)
  end

  @impl true
  def init(opts) do
    children = [
      {@server, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(attrs), do: GenServer.call(@server, {:create, attrs})
  def update_status(id, status), do: GenServer.call(@server, {:update_status, id, status})
  def get(id), do: GenServer.call(@server, {:get, id})
  def list, do: GenServer.call(@server, :list)
  def delete(id), do: GenServer.call(@server, {:delete, id})
  def history(limit \\ 100), do: GenServer.call(@server, {:history, limit})
end

defmodule DeliveryTracker.Server do
  use GenServer
  require DeliveryTracker.Validation

  @table :delivery_tracker_store
  @statuses [:pending, :picked_up, :in_transit, :delivered, :failed]
  @default_ttl 86_400_000
  @max_ttl 31_536_000_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file =
      opts
      |> Keyword.get(:file, "delivery_tracker.dets")
      |> to_string()
      |> String.to_charlist()

    case :dets.open_file(@table, file: file, type: :set, repair: true) do
      {:ok, @table} ->
        state = %{timers: %{}, sequence: last_sequence()}
        {:ok, recover(state)}

      {:error, reason} ->
        {:stop, {:storage_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, attrs} <- validate_attributes(attrs),
         {:ok, delivery} <- build_delivery(attrs),
         false <- exists?(delivery.id) do
      :ok = :dets.insert(@table, {{:delivery, delivery.id}, delivery})
      state = schedule_expiration(state, delivery)
      state = record(state, :created, delivery.id, %{status: delivery.status})
      {:reply, {:ok, delivery}, state}
    else
      true ->
        {:reply, {:error, :already_exists}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:update_status, id, status}, _from, state) do
    with :ok <- validate_id(id),
         :ok <- validate_status(status),
         {:ok, delivery} <- fetch_delivery(id) do
      updated = %{delivery | status: status, updated_at: now_ms()}
      :ok = :dets.insert(@table, {{:delivery, id}, updated})
      state = record(state, :status_updated, id, %{from: delivery.status, to: status})
      {:reply, {:ok, updated}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case validate_id(id) do
      :ok ->
        result =
          case fetch_delivery(id) do
            {:ok, delivery} -> {:ok, delivery}
            {:error, :not_found} -> {:error, :not_found}
          end

        state = record(state, :get, id, %{found: match?({:ok, _}, result)})
        {:reply, result, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:list, _from, state) do
    deliveries =
      @table
      |> :dets.foldl(fn
        {{:delivery, _id}, delivery}, acc -> [delivery | acc]
        _, acc -> acc
      end, [])
      |> Enum.sort_by(& &1.id)

    state = record(state, :list, nil, %{count: length(deliveries)})
    {:reply, {:ok, deliveries}, state}
  end

  def handle_call({:delete, id}, _from, state) do
    with :ok <- validate_id(id),
         {:ok, delivery} <- fetch_delivery(id) do
      :ok = :dets.delete(@table, {:delivery, id})
      state = cancel_expiration(state, id)
      state = record(state, :deleted, id, %{status: delivery.status})
      {:reply, :ok, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:history, limit}, _from, state) do
    if is_integer(limit) and limit in 1..1_000 do
      events =
        @table
        |> :dets.foldl(fn
          {{:event, _sequence}, event}, acc -> [event | acc]
          _, acc -> acc
        end, [])
        |> Enum.sort_by(& &1.sequence, :desc)
        |> Enum.take(limit)

      state = record(state, :history_read, nil, %{limit: limit})
      {:reply, {:ok, events}, state}
    else
      {:reply, {:error, :invalid_limit}, state}
    end
  end

  @impl true
  def handle_info({:expire, id, expires_at}, state) do
    case fetch_delivery(id) do
      {:ok, %{expires_at: ^expires_at}} = result ->
        now = now_ms()

        if expires_at <= now do
          :ok = :dets.delete(@table, {:delivery, id})
          state = %{state | timers: Map.delete(state.timers, id)}
          {:noreply, record(state, :expired, id, %{reason: :ttl})}
        else
          {:ok, delivery} = result
          {:noreply, schedule_expiration(state, delivery)}
        end

      _ ->
        {:noreply, %{state | timers: Map.delete(state.timers, id)}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp validate_attributes(attrs) do
    case DeliveryTracker.Validation.validate_keys(
           attrs,
           [:id, :recipient, :address],
           [:id, :recipient, :address, :metadata, :ttl]
         ) do
      {:ok, attrs} ->
        with :ok <- validate_id(attrs.id),
             :ok <- validate_text(attrs.recipient, :recipient),
             :ok <- validate_text(attrs.address, :address),
             :ok <- validate_metadata(Map.get(attrs, :metadata, %{})),
             :ok <- validate_ttl(Map.get(attrs, :ttl, @default_ttl)) do
          {:ok, attrs}
        end

      error ->
        error
    end
  end

  defp build_delivery(attrs) do
    now = now_ms()
    ttl = Map.get(attrs, :ttl, @default_ttl)

    {:ok,
     %{
       id: attrs.id,
       recipient: attrs.recipient,
       address: attrs.address,
       metadata: Map.get(attrs, :metadata, %{}),
       status: :pending,
       created_at: now,
       updated_at: now,
       expires_at: now + ttl
     }}
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) in 1..128, do: :ok
  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_text(value, _field)
       when is_binary(value) and byte_size(value) in 1..2_048,
       do: :ok

  defp validate_text(_, field), do: {:error, {:invalid_field, field}}

  defp validate_metadata(metadata) when is_map(metadata), do: :ok
  defp validate_metadata(_), do: {:error, :invalid_metadata}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0 and ttl <= @max_ttl, do: :ok
  defp validate_ttl(_), do: {:error, :invalid_ttl}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(_), do: {:error, :invalid_status}

  defp exists?(id), do: match?({:ok, _}, fetch_delivery(id))

  defp fetch_delivery(id) do
    case :dets.lookup(@table, {:delivery, id}) do
      [{{:delivery, ^id}, delivery}] -> {:ok, delivery}
      [] -> {:error, :not_found}
    end
  end

  defp recover(state) do
    deliveries =
      :dets.foldl(
        fn
          {{:delivery, _id}, delivery}, acc -> [delivery | acc]
          _, acc -> acc
        end,
        [],
        @table
      )

    Enum.reduce(deliveries, state, fn delivery, acc ->
      if delivery.expires_at <= now_ms() do
        :ok = :dets.delete(@table, {:delivery, delivery.id})
        record(acc, :expired, delivery.id, %{reason: :recovery})
      else
        schedule_expiration(acc, delivery)
      end
    end)
  end

  defp schedule_expiration(state, delivery) do
    state = cancel_expiration(state, delivery.id)
    delay = max(delivery.expires_at - now_ms(), 1)
    timer = Process.send_after(self(), {:expire, delivery.id, delivery.expires_at}, delay)
    %{state | timers: Map.put(state.timers, delivery.id, timer)}
  end

  defp cancel_expiration(state, id) do
    case Map.pop(state.timers, id) do
      {nil, timers} ->
        %{state | timers: timers}

      {timer, timers} ->
        Process.cancel_timer(timer)
        %{state | timers: timers}
    end
  end

  defp record(state, action, id, details) do
    sequence = state.sequence + 1

    event = %{
      sequence: sequence,
      at: now_ms(),
      action: action,
      id: id,
      details: details
    }

    :ok = :dets.insert(@table, {{:event, sequence}, event})
    :ok = :dets.sync(@table)
    %{state | sequence: sequence}
  end

  defp last_sequence do
    :dets.foldl(
      fn
        {{:event, sequence}, _event}, max_sequence when sequence > max_sequence -> sequence
        _, max_sequence -> max_sequence
      end,
      0,
      @table
    )
  end

  defp now_ms, do: System.system_time(:millisecond)
end
