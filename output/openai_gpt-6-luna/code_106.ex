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

  @default_server RoomReservations.Server

  def start_link(opts \\ []) do
    supervisor_name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: supervisor_name)
  end

  def init(opts) do
    server_name = Keyword.get(opts, :server_name, @default_server)
    path = Keyword.get(opts, :path, "room_reservations.snapshot")

    children = [
      {RoomReservations.Server, name: server_name, path: path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def reserve(room, occupant, ttl_ms, server \\ @default_server) do
    GenServer.call(server, {:reserve, room, occupant, ttl_ms})
  end

  def cancel(room, server \\ @default_server) do
    GenServer.call(server, {:cancel, room})
  end

  def get(room, server \\ @default_server) do
    GenServer.call(server, {:get, room})
  end

  def history(server \\ @default_server) do
    GenServer.call(server, :history)
  end
end

defmodule RoomReservations.Server do
  use GenServer

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    target = Keyword.get(opts, :name, __MODULE__)

    with {:ok, snapshot} <- load_snapshot(path) do
      state = %{
        path: path,
        target: target,
        reservations: snapshot.reservations,
        history: snapshot.history
      }

      {state, changed?} = prune_expired(state, System.system_time(:millisecond))

      with {:ok, state} <- persist_if_changed(state, changed?) do
        Enum.each(state.reservations, fn {room, reservation} ->
          schedule_expiration(state.target, room, reservation)
        end)

        {:ok, state}
      else
        {:error, reason} -> {:stop, reason}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:reserve, _room, _occupant, ttl_ms}, _from, state)
      when not is_integer(ttl_ms) or ttl_ms <= 0 do
    {:reply, {:error, :invalid_ttl}, state}
  end

  def handle_call({:reserve, room, occupant, ttl_ms}, _from, state) do
    {base, expired?} = prune_expired(state, System.system_time(:millisecond))

    cond do
      Map.has_key?(base.reservations, room) ->
        case persist_if_changed(base, expired?) do
          {:ok, next_state} -> {:reply, {:error, :already_reserved}, next_state}
          {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
        end

      true ->
        now = System.system_time(:millisecond)

        reservation = %{
          occupant: occupant,
          expires_at: now + ttl_ms,
          token: System.unique_integer([:positive, :monotonic])
        }

        next_state =
          base
          |> put_reservation(room, reservation, :reserved, now)

        case persist(next_state) do
          :ok ->
            schedule_expiration(next_state.target, room, reservation)
            {:reply, {:ok, reservation}, next_state}

          {:error, reason} ->
            {:reply, {:error, {:persistence, reason}}, state}
        end
    end
  end

  def handle_call({:cancel, room}, _from, state) do
    {base, expired?} = prune_expired(state, System.system_time(:millisecond))

    case Map.pop(base.reservations, room) do
      {nil, _reservations} ->
        case persist_if_changed(base, expired?) do
          {:ok, next_state} -> {:reply, {:error, :not_found}, next_state}
          {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
        end

      {reservation, reservations} ->
        now = System.system_time(:millisecond)

        next_state =
          base
          |> Map.put(:reservations, reservations)
          |> add_history(%{
            action: :cancelled,
            room: room,
            occupant: reservation.occupant,
            at: now
          })

        case persist(next_state) do
          :ok -> {:reply, :ok, next_state}
          {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
        end
    end
  end

  def handle_call({:get, room}, _from, state) do
    {base, expired?} = prune_expired(state, System.system_time(:millisecond))

    case persist_if_changed(base, expired?) do
      {:ok, next_state} ->
        reply =
          case Map.fetch(next_state.reservations, room) do
            {:ok, reservation} -> {:ok, reservation}
            :error -> :error
          end

        {:reply, reply, next_state}

      {:error, reason} ->
        {:reply, {:error, {:persistence, reason}}, state}
    end
  end

  def handle_call(:history, _from, state) do
    {base, expired?} = prune_expired(state, System.system_time(:millisecond))

    case persist_if_changed(base, expired?) do
      {:ok, next_state} ->
        {:reply, Enum.reverse(next_state.history), next_state}

      {:error, reason} ->
        {:reply, {:error, {:persistence, reason}}, state}
    end
  end

  @impl true
  def handle_cast({:expire, room, token}, state) do
    case Map.get(state.reservations, room) do
      %{token: ^token, expires_at: expires_at} = reservation ->
        now = System.system_time(:millisecond)

        if expires_at <= now do
          next_state =
            state
            |> Map.update!(:reservations, &Map.delete(&1, room))
            |> add_history(%{
              action: :expired,
              room: room,
              occupant: reservation.occupant,
              at: now
            })

          case persist(next_state) do
            :ok ->
              {:noreply, next_state}

            {:error, _reason} ->
              schedule_expiration(state.target, room, %{
                reservation
                | expires_at: now + 1_000
              })

              {:noreply, state}
          end
        else
          schedule_expiration(state.target, room, reservation)
          {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  defp put_reservation(state, room, reservation, action, now) do
    state
    |> Map.update!(:reservations, &Map.put(&1, room, reservation))
    |> add_history(%{
      action: action,
      room: room,
      occupant: reservation.occupant,
      expires_at: reservation.expires_at,
      at: now
    })
  end

  defp add_history(state, entry) do
    Map.update!(state, :history, &[entry | &1])
  end

  defp prune_expired(state, now) do
    expired =
      Enum.filter(state.reservations, fn {_room, reservation} ->
        reservation.expires_at <= now
      end)

    next_state =
      Enum.reduce(expired, state, fn {room, reservation}, acc ->
        acc
        |> Map.update!(:reservations, &Map.delete(&1, room))
        |> add_history(%{
          action: :expired,
          room: room,
          occupant: reservation.occupant,
          at: now
        })
      end)

    {next_state, expired != []}
  end

  defp persist_if_changed(state, true) do
    case persist(state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_if_changed(state, false), do: {:ok, state}

  defp persist(state) do
    snapshot = %{
      reservations: state.reservations,
      history: state.history
    }

    path = state.path
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, :erlang.term_to_binary(snapshot, [:compressed]), [:binary]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end

  defp load_snapshot(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            %{reservations: reservations, history: history}
            when is_map(reservations) and is_list(history) ->
              {:ok, %{reservations: reservations, history: history}}

            _ ->
              {:error, :invalid_snapshot}
          end
        rescue
          _ -> {:error, :invalid_snapshot}
        end

      {:error, :enoent} ->
        {:ok, %{reservations: %{}, history: []}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp schedule_expiration(target, room, reservation) do
    delay = max(reservation.expires_at - System.system_time(:millisecond), 0)

    Task.start(fn ->
      Process.sleep(delay)
      GenServer.cast(target, {:expire, room, reservation.token})
    end)

    :ok
  end
end
