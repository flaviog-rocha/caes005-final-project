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

  Reservas da mesma sala são verificadas e alteradas de forma exclusiva.
  `max_concurrency` limita o número de operações processadas simultaneamente.
  """

  @type reservation :: %{
          id: pos_integer(),
          room: term(),
          user: term(),
          start_at: integer(),
          end_at: integer()
        }

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cria uma reserva. Os horários podem ser inteiros ou valores DateTime."
  def reserve(room, user, start_at, end_at) do
    GenServer.call(__MODULE__, {:operation, {:reserve, room, user, start_at, end_at}}, :infinity)
  end

  @doc "Cancela uma reserva pelo identificador."
  def cancel(reservation_id) do
    GenServer.call(__MODULE__, {:operation, {:cancel, reservation_id}}, :infinity)
  end

  @doc "Retorna as reservas existentes, opcionalmente filtradas por sala."
  def reservations(room \\ :all) do
    GenServer.call(__MODULE__, {:reservations, room})
  end

  @doc "Retorna o histórico das operações realizadas."
  def history do
    GenServer.call(__MODULE__, :history)
  end

  @doc "Retorna as notificações geradas."
  def notifications do
    GenServer.call(__MODULE__, :notifications)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    threshold = Keyword.get(opts, :notification_threshold, 5)

    unless is_integer(threshold) and threshold > 0 do
      raise ArgumentError, "notification_threshold deve ser um inteiro positivo"
    end

    reservations =
      :ets.new(:room_reservations, [
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    history =
      :ets.new(:room_reservation_history, [
        :ordered_set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    notifications =
      :ets.new(:room_reservation_notifications, [
        :ordered_set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    counters =
      :ets.new(:room_reservation_counters, [
        :set,
        :public,
        write_concurrency: true
      ])

    {:ok,
     %{
       reservations: reservations,
       history: history,
       notifications: notifications,
       counters: counters,
       threshold: threshold,
       max_concurrency: max_concurrency,
       active: %{},
       queue: :queue.new()
     }}
  end

  @impl true
  def handle_call({:operation, operation}, from, state) do
    if map_size(state.active) < state.max_concurrency do
      {:noreply, start_operation(operation, from, state)}
    else
      {:noreply, %{state | queue: :queue.in({operation, from}, state.queue)}}
    end
  end

  def handle_call({:reservations, room}, _from, state) do
    reservations =
      state.reservations
      |> :ets.tab2list()
      |> Enum.map(&elem(&1, 1))
      |> Enum.filter(fn reservation -> room == :all or reservation.room == room end)

    {:reply, reservations, state}
  end

  def handle_call(:history, _from, state) do
    entries = state.history |> :ets.tab2list() |> Enum.map(&elem(&1, 1))
    {:reply, entries, state}
  end

  def handle_call(:notifications, _from, state) do
    entries = state.notifications |> :ets.tab2list() |> Enum.map(&elem(&1, 1))
    {:reply, entries, state}
  end

  @impl true
  def handle_info({:operation_result, ref, result}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {from, active} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, result)
        {:noreply, dispatch_queued(%{state | active: active})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {from, active} ->
        GenServer.reply(from, {:error, {:worker_exit, reason}})
        {:noreply, dispatch_queued(%{state | active: active})}
    end
  end

  defp dispatch_queued(state) do
    if map_size(state.active) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, {operation, from}}, queue} ->
          state
          |> Map.put(:queue, queue)
          |> start_operation(operation, from)
          |> dispatch_queued()

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp start_operation(state, operation, from) do
    start_operation(operation, from, state)
  end

  defp start_operation(operation, from, state) do
    parent = self()
    tables = Map.take(state, [:reservations, :history, :notifications, :counters, :threshold])

    {_pid, ref} =
      spawn_monitor(fn ->
        result =
          try do
            execute(operation, tables)
          rescue
            exception -> {:error, {:worker_exception, exception}}
          catch
            kind, reason -> {:error, {:worker_failure, kind, reason}}
          end

        send(parent, {:operation_result, ref_for_worker(), result})
      end)

    # A worker needs its monitor reference in the result message. Start it with
    # a reference created before spawning so message ordering remains reliable.
    Process.demonitor(ref, [:flush])
    start_operation_with_ref(operation, from, state, tables)
  end

  defp start_operation_with_ref(operation, from, state, tables) do
    parent = self()
    ref = make_ref()

    {_pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            execute(operation, tables)
          rescue
            exception -> {:error, {:worker_exception, exception}}
          catch
            kind, reason -> {:error, {:worker_failure, kind, reason}}
          end

        send(parent, {:operation_result, ref, result})
      end)

    # Use the monitor reference to track worker failures; the result message
    # uses a separate reference tied to this operation.
    active = Map.put(state.active, monitor_ref, from)
    %{state | active: active}
  end

  defp ref_for_worker, do: make_ref()

  defp execute({:reserve, room, user, start_at, end_at}, tables) do
    :global.trans({{__MODULE__, room}, self()}, fn ->
      do_reserve(room, user, start_at, end_at, tables)
    end)
  end

  defp execute({:cancel, reservation_id}, tables) do
    case :ets.lookup(tables.reservations, reservation_id) do
      [{^reservation_id, reservation}] ->
        :global.trans({{__MODULE__, reservation.room}, self()}, fn ->
          do_cancel(reservation_id, tables)
        end)

      [] ->
        log_history(tables, %{action: :cancel, reservation_id: reservation_id, result: :not_found})
        {:error, :not_found}
    end
  end

  defp do_reserve(room, user, start_at, end_at, tables) do
    with {:ok, start_value} <- normalize_time(start_at),
         {:ok, end_value} <- normalize_time(end_at),
         true <- end_value > start_value do
      overlapping =
        tables.reservations
        |> :ets.tab2list()
        |> Enum.map(&elem(&1, 1))
        |> Enum.any?(fn reservation ->
          reservation.room == room and
            start_value < reservation.end_at and
            end_value > reservation.start_at
        end)

      if overlapping do
        log_history(tables, %{
          action: :reserve,
          room: room,
          user: user,
          start_at: start_value,
          end_at: end_value,
          result: :room_unavailable
        })

        notify(tables, %{
          condition: :room_unavailable,
          room: room,
          user: user,
          start_at: start_value,
          end_at: end_value
        })

        {:error, :room_unavailable}
      else
        id = System.unique_integer([:positive, :monotonic])

        reservation = %{
          id: id,
          room: room,
          user: user,
          start_at: start_value,
          end_at: end_value
        }

        true = :ets.insert_new(tables.reservations, {id, reservation})

        log_history(tables, %{
          action: :reserve,
          reservation_id: id,
          room: room,
          user: user,
          start_at: start_value,
          end_at: end_value,
          result: :ok
        })

        room_count =
          tables.reservations
          |> :ets.tab2list()
          |> Enum.count(fn {_id, item} -> item.room == room end)

        if room_count == tables.threshold do
          notify(tables, %{
            condition: :reservation_threshold_reached,
            room: room,
            count: room_count,
            threshold: tables.threshold
          })
        end

        {:ok, reservation}
      end
    else
      false ->
        log_history(tables, %{action: :reserve, room: room, user: user, result: :invalid_interval})
        {:error, :invalid_interval}

      {:error, reason} ->
        log_history(tables, %{action: :reserve, room: room, user: user, result: {:invalid_time, reason}})
        {:error, {:invalid_time, reason}}
    end
  end

  defp do_cancel(reservation_id, tables) do
    case :ets.lookup(tables.reservations, reservation_id) do
      [{^reservation_id, reservation}] ->
        :ets.delete(tables.reservations, reservation_id)

        log_history(tables, %{
          action: :cancel,
          reservation_id: reservation_id,
          room: reservation.room,
          result: :ok
        })

        {:ok, reservation}

      [] ->
        log_history(tables, %{action: :cancel, reservation_id: reservation_id, result: :not_found})
        {:error, :not_found}
    end
  end

  defp normalize_time(value) when is_integer(value), do: {:ok, value}

  defp normalize_time(%DateTime{} = value) do
    {:ok, DateTime.to_unix(value, :microsecond)}
  end

  defp normalize_time(_value), do: {:error, :unsupported_time_format}

  defp log_history(tables, event) do
    id = :ets.update_counter(tables.counters, :history, {2, 1}, {:history, 0})
    entry = Map.merge(event, %{id: id, at: DateTime.utc_now()})
    :ets.insert(tables.history, {id, entry})
    entry
  end

  defp notify(tables, event) do
    id = :ets.update_counter(tables.counters, :notifications, {2, 1}, {:notifications, 0})
    notification = Map.merge(event, %{id: id, at: DateTime.utc_now()})
    :ets.insert(tables.notifications, {id, notification})
    notification
  end
end
