# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Recuperar processos após falhas inesperadas.
# 3. Implementar expiração automática de registros.
# 4. Impedir atualizações parcialmente concluídas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomReservations do
  use Supervisor

  @store RoomReservations.Store

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "room_reservations.db")
    interval = Keyword.get(opts, :expiry_interval_ms, 1_000)

    children = [
      {@store, [path: path, expiry_interval_ms: interval]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def reserve(room, user, ttl_ms), do: reserve(@store, room, user, ttl_ms)

  def reserve(server, room, user, ttl_ms) do
    GenServer.call(server, {:reserve, room, user, ttl_ms})
  end

  def update(id, attrs), do: update(@store, id, attrs)

  def update(server, id, attrs) do
    GenServer.call(server, {:update, id, attrs})
  end

  def cancel(id), do: cancel(@store, id)

  def cancel(server, id) do
    GenServer.call(server, {:cancel, id})
  end

  def get(id), do: get(@store, id)

  def get(server, id) do
    GenServer.call(server, {:get, id})
  end

  def reservations, do: reservations(@store)

  def reservations(server) do
    GenServer.call(server, :reservations)
  end

  def history, do: history(@store)

  def history(server) do
    GenServer.call(server, :history)
  end
end

defmodule RoomReservations.Store do
  use GenServer

  @default_expiry_interval_ms 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    interval = Keyword.get(opts, :expiry_interval_ms, @default_expiry_interval_ms)

    if not (is_integer(interval) and interval > 0) do
      {:stop, :invalid_expiry_interval}
    else
      case load_state(path) do
        {:ok, state} ->
          Process.send_after(self(), :expire, interval)
          {:ok, %{state: state, path: path, interval: interval}}

        {:error, reason} ->
          {:stop, reason}
      end
    end
  end

  @impl true
  def handle_call({:reserve, room, user, ttl_ms}, _from, data) do
    now = System.system_time(:millisecond)
    {expired_state, expired?} = expire_due(data.state, now)

    case validate_reservation(room, user, ttl_ms) do
      :ok ->
        if Enum.any?(expired_state.reservations, fn {_id, reservation} ->
             reservation.room == room
           end) do
          finish(data, expired_state, expired?, {:error, :room_unavailable})
        else
          id = new_id()

          reservation = %{
            id: id,
            room: room,
            user: user,
            expires_at: now + ttl_ms
          }

          next_state =
            expired_state
            |> put_in([:reservations, id], reservation)
            |> append_history(:reserved, %{id: id, room: room, user: user})

          finish(data, next_state, true, {:ok, reservation})
        end

      {:error, reason} ->
        finish(data, expired_state, expired?, {:error, reason})
    end
  end

  def handle_call({:update, id, attrs}, _from, data) do
    now = System.system_time(:millisecond)
    {expired_state, expired?} = expire_due(data.state, now)

    case Map.fetch(expired_state.reservations, id) do
      :error ->
        finish(data, expired_state, expired?, {:error, :not_found})

      {:ok, current} ->
        case updated_reservation(current, attrs, now) do
          {:ok, reservation} ->
            room_taken? =
              Enum.any?(expired_state.reservations, fn {other_id, other} ->
                other_id != id and other.room == reservation.room
              end)

            if room_taken? do
              finish(data, expired_state, expired?, {:error, :room_unavailable})
            else
              next_state =
                expired_state
                |> put_in([:reservations, id], reservation)
                |> append_history(:updated, %{id: id, room: reservation.room})

              finish(data, next_state, true, {:ok, reservation})
            end

          {:error, reason} ->
            finish(data, expired_state, expired?, {:error, reason})
        end
    end
  end

  def handle_call({:cancel, id}, _from, data) do
    now = System.system_time(:millisecond)
    {expired_state, expired?} = expire_due(data.state, now)

    case Map.pop(expired_state.reservations, id) do
      {nil, _reservations} ->
        finish(data, expired_state, expired?, {:error, :not_found})

      {reservation, reservations} ->
        next_state =
          %{expired_state | reservations: reservations}
          |> append_history(:cancelled, %{id: id, room: reservation.room})

        finish(data, next_state, true, :ok)
    end
  end

  def handle_call({:get, id}, _from, data) do
    {:reply, Map.fetch(data.state.reservations, id), data}
  end

  def handle_call(:reservations, _from, data) do
    result =
      data.state.reservations
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, result, data}
  end

  def handle_call(:history, _from, data) do
    {:reply, data.state.history, data}
  end

  @impl true
  def handle_info(:expire, data) do
    now = System.system_time(:millisecond)
    {next_state, changed?} = expire_due(data.state, now)

    updated_data =
      case persist_if_changed(data, next_state, changed?) do
        {:ok, committed_data} -> committed_data
        {:error, _reason} -> data
      end

    Process.send_after(self(), :expire, data.interval)
    {:noreply, updated_data}
  end

  defp finish(data, candidate, changed?, reply) do
    case persist_if_changed(data, candidate, changed?) do
      {:ok, committed_data} -> {:reply, reply, committed_data}
      {:error, reason} -> {:reply, {:error, {:persistence, reason}}, data}
    end
  end

  defp persist_if_changed(data, _candidate, false), do: {:ok, data}

  defp persist_if_changed(data, candidate, true) do
    case persist_state(data.path, candidate) do
      :ok -> {:ok, %{data | state: candidate}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp expire_due(state, now) do
    expired_ids =
      state.reservations
      |> Task.async_stream(
        fn {id, reservation} ->
          {id, reservation.expires_at <= now}
        end,
        ordered: false,
        max_concurrency: max(System.schedulers_online(), 1),
        timeout: 5_000,
        on_timeout: :kill_task
      )
      |> Enum.flat_map(fn
        {:ok, {id, true}} -> [id]
        _ -> []
      end)
      |> Enum.sort()

    if expired_ids == [] do
      {state, false}
    else
      reservations = Map.drop(state.reservations, expired_ids)

      next_state =
        %{state | reservations: reservations}
        |> append_history(:expired, %{ids: expired_ids})

      {next_state, true}
    end
  end

  defp validate_reservation(room, user, ttl_ms) do
    cond do
      not valid_text?(room) -> {:error, :invalid_room}
      not valid_text?(user) -> {:error, :invalid_user}
      not (is_integer(ttl_ms) and ttl_ms > 0) -> {:error, :invalid_ttl}
      true -> :ok
    end
  end

  defp updated_reservation(current, attrs, now) when is_map(attrs) do
    allowed = [:room, :user, :ttl_ms]

    cond do
      Enum.any?(Map.keys(attrs), fn key -> key not in allowed end) ->
        {:error, :invalid_attributes}

      true ->
        room = Map.get(attrs, :room, current.room)
        user = Map.get(attrs, :user, current.user)
        ttl_ms = Map.get(attrs, :ttl_ms)

        cond do
          not valid_text?(room) -> {:error, :invalid_room}
          not valid_text?(user) -> {:error, :invalid_user}
          not is_nil(ttl_ms) and not (is_integer(ttl_ms) and ttl_ms > 0) ->
            {:error, :invalid_ttl}

          true ->
            expires_at =
              if is_nil(ttl_ms), do: current.expires_at, else: now + ttl_ms

            {:ok, %{current | room: room, user: user, expires_at: expires_at}}
        end
    end
  end

  defp updated_reservation(_current, _attrs, _now), do: {:error, :invalid_attributes}

  defp valid_text?(value), do: is_binary(value) and byte_size(value) > 0

  defp append_history(state, operation, details) do
    entry = %{
      at: System.system_time(:millisecond),
      operation: operation,
      details: details
    }

    %{state | history: state.history ++ [entry]}
  end

  defp new_id do
    :crypto.strong_rand_bytes(16)
    |> Base.url_encode64(padding: false)
  end

  defp load_state(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            %{reservations: reservations, history: history} = state
            when is_map(reservations) and is_list(history) ->
              {:ok, state}

            _ ->
              {:error, :invalid_snapshot}
          end
        rescue
          _ -> {:error, :invalid_snapshot}
        end

      {:error, :enoent} ->
        {:ok, %{reservations: %{}, history: []}}

      {:error, reason} ->
        {:error, {:snapshot_read_failed, reason}}
    end
  end

  defp persist_state(path, state) do
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- write_temporary_file(temporary_path, state),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, {:snapshot_write_failed, reason}}
    end
  end

  defp write_temporary_file(path, state) do
    case File.open(path, [:write, :binary, :exclusive]) do
      {:ok, file} ->
        result =
          try do
            with :ok <- IO.binwrite(file, :erlang.term_to_binary(state)),
                 :ok <- :file.sync(file) do
              :ok
            end
          after
            File.close(file)
          end

        case result do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
