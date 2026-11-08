# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Manter um histórico das operações realizadas.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomReservations do
  use GenServer

  @moduledoc """
  Gerenciador concorrente de reservas de salas usando ETS.

  Os horários são recebidos como inteiros ou como `DateTime` e armazenados
  como microssegundos desde a época Unix. Os intervalos são semiabertos:
  uma reserva pode começar exatamente quando outra termina.
  """

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @impl true
  def init(opts) do
    max_concurrent = Keyword.get(opts, :max_concurrent, 10)

    unless is_integer(max_concurrent) and max_concurrent > 0 do
      raise ArgumentError, "max_concurrent deve ser um inteiro positivo"
    end

    table_opts = [:set, :public, read_concurrency: true, write_concurrency: true]

    state = %{
      reservations: :ets.new(:reservations, table_opts),
      history: :ets.new(:reservation_history, [:ordered_set, :public]),
      notifications: :ets.new(:reservation_notifications, [:ordered_set, :public]),
      meta: :ets.new(:reservation_meta, [:set, :public]),
      slots: :ets.new(:reservation_slots, [:set, :public]),
      room_locks: :ets.new(:reservation_room_locks, [:set, :public]),
      slot_count: max_concurrent,
      on_notification: Keyword.get(opts, :on_notification)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:tables, _from, state), do: {:reply, state, state}

  def reserve(server, room, starts_at, ends_at, user \\ nil) do
    with_tables(server, fn state ->
      with_operation(state, fn ->
        with {:ok, start_time} <- normalize_time(starts_at),
             {:ok, end_time} <- normalize_time(ends_at),
             true <- room != nil and start_time < end_time do
          with_room_lock(state, room, fn ->
            overlapping =
              :ets.tab2list(state.reservations)
              |> Enum.any?(fn {_id, reservation} ->
                reservation.status == :active and reservation.room === room and
                  start_time < reservation.ends_at and reservation.starts_at < end_time
              end)

            if overlapping do
              details = %{room: room, starts_at: start_time, ends_at: end_time}
              record_history(state, :reservation_conflict, details)
              notify(state, :reservation_conflict, details)
              {:error, :conflict}
            else
              id = :erlang.unique_integer([:positive, :monotonic])

              reservation = %{
                id: id,
                room: room,
                starts_at: start_time,
                ends_at: end_time,
                user: user,
                status: :active
              }

              :ets.insert(state.reservations, {id, reservation})
              record_history(state, :reservation_created, reservation)
              notify(state, :reservation_created, reservation)
              {:ok, reservation}
            end
          end)
        else
          false ->
            record_history(state, :reservation_rejected, %{reason: :invalid_room_or_interval})
            {:error, :invalid_room_or_interval}

          :error ->
            record_history(state, :reservation_rejected, %{reason: :invalid_time})
            {:error, :invalid_time}
        end
      end)
    end)
  end

  def cancel(server, reservation_id) do
    with_tables(server, fn state ->
      with_operation(state, fn ->
        case :ets.lookup(state.reservations, reservation_id) do
          [] ->
            record_history(state, :reservation_not_found, %{id: reservation_id})
            {:error, :not_found}

          [{^reservation_id, reservation}] ->
            with_room_lock(state, reservation.room, fn ->
              case :ets.lookup(state.reservations, reservation_id) do
                [{^reservation_id, %{status: :active} = current}] ->
                  cancelled = %{current | status: :cancelled}
                  :ets.insert(state.reservations, {reservation_id, cancelled})
                  record_history(state, :reservation_cancelled, cancelled)
                  notify(state, :reservation_cancelled, cancelled)
                  {:ok, cancelled}

                [{^reservation_id, current}] ->
                  record_history(state, :reservation_already_cancelled, current)
                  {:error, :already_cancelled}

                [] ->
                  record_history(state, :reservation_not_found, %{id: reservation_id})
                  {:error, :not_found}
              end
            end)
        end
      end)
    end)
  end

  def reservations(server) do
    with_tables(server, fn state ->
      with_operation(state, fn ->
        :ets.tab2list(state.reservations)
        |> Enum.map(fn {_id, reservation} -> reservation end)
        |> Enum.sort_by(& &1.starts_at)
      end)
    end)
  end

  def history(server) do
    with_tables(server, fn state ->
      with_operation(state, fn ->
        :ets.tab2list(state.history)
        |> Enum.map(fn {_sequence, event} -> event end)
      end)
    end)
  end

  def notifications(server) do
    with_tables(server, fn state ->
      with_operation(state, fn ->
        :ets.tab2list(state.notifications)
        |> Enum.map(fn {_sequence, notification} -> notification end)
      end)
    end)
  end

  defp with_tables(server, fun) do
    state = GenServer.call(server, :tables)
    fun.(state)
  end

  defp with_operation(state, fun) do
    token = make_ref()
    acquire_slot(state, token)

    try do
      fun.()
    after
      release_slot(state, token)
    end
  end

  defp acquire_slot(state, token) do
    slot =
      Enum.find(1..state.slot_count, fn number ->
        :ets.insert_new(state.slots, {number, token})
      end)

    if slot == nil do
      Process.sleep(1)
      acquire_slot(state, token)
    end
  end

  defp release_slot(state, token) do
    :ets.match_object(state.slots, {:_, token})
    |> Enum.each(&:ets.delete_object(state.slots, &1))
  end

  defp with_room_lock(state, room, fun) do
    token = make_ref()
    acquire_room_lock(state, room, token)

    try do
      fun.()
    after
      :ets.delete_object(state.room_locks, {room, token})
    end
  end

  defp acquire_room_lock(state, room, token) do
    if :ets.insert_new(state.room_locks, {room, token}) do
      :ok
    else
      Process.sleep(1)
      acquire_room_lock(state, room, token)
    end
  end

  defp normalize_time(value) when is_integer(value), do: {:ok, value}

  defp normalize_time(%DateTime{} = value) do
    {:ok, DateTime.to_unix(value, :microsecond)}
  end

  defp normalize_time(_value), do: :error

  defp record_history(state, type, details) do
    sequence = next_sequence(state, :history_sequence)

    :ets.insert(state.history, {
      sequence,
      %{sequence: sequence, type: type, details: details, at: DateTime.utc_now()}
    })
  end

  defp notify(state, type, details) do
    sequence = next_sequence(state, :notification_sequence)

    notification = %{
      sequence: sequence,
      type: type,
      details: details,
      at: DateTime.utc_now()
    }

    :ets.insert(state.notifications, {sequence, notification})

    if is_function(state.on_notification, 1) do
      callback = state.on_notification
      spawn(fn -> safely_invoke(callback, notification) end)
    end
  end

  defp safely_invoke(callback, notification) do
    try do
      callback.(notification)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp next_sequence(state, key) do
    :ets.update_counter(state.meta, key, {2, 1}, {key, 0})
  end
end
