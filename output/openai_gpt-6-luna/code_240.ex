# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Fazer uso de macros.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomReservations do
  use GenServer

  @table :room_reservations_store

  defmacro operation(name, parameters) do
    variables = Enum.map(parameters, &Macro.var(&1, nil))

    quote do
      def unquote(name)(server, operation_id, unquote_splicing(variables)) do
        GenServer.call(
          server,
          {:execute, operation_id, unquote(name), [unquote_splicing(variables)]},
          :infinity
        )
      end
    end
  end

  operation :reserve, [:room, :starts_at, :ends_at]
  operation :cancel, [:reservation_id]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def status(server), do: GenServer.call(server, :status)
  def in_progress(server), do: GenServer.call(server, :in_progress)
  def reservations(server), do: GenServer.call(server, :reservations)

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, "room_reservations.dets") |> Path.expand()
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} ->
        snapshot =
          case :dets.lookup(@table, :snapshot) do
            [{:snapshot, saved}] when is_map(saved) -> saved
            _ -> %{operations: %{}, reservations: %{}}
          end

        operations = recover_interrupted(Map.get(snapshot, :operations, %{}))
        reservations = Map.get(snapshot, :reservations, %{})

        case persist(operations, reservations) do
          :ok ->
            {:ok,
             %{
               operations: operations,
               reservations: reservations,
               active: %{},
               queue: :queue.new(),
               max_concurrency: max_concurrency
             }}

          {:error, reason} ->
            :dets.close(@table)
            {:stop, {:persistence_error, reason}}
        end

      {:error, reason} ->
        {:stop, {:dets_open_error, reason}}
    end
  end

  @impl true
  def handle_call({:execute, operation_id, action, args}, from, state) do
    cond do
      is_nil(operation_id) ->
        {:reply, {:error, :invalid_operation_id}, state}

      Map.has_key?(state.operations, operation_id) ->
        operation = Map.fetch!(state.operations, operation_id)

        reply =
          case operation.status do
            :running -> {:error, :in_progress}
            :done -> operation.result
          end

        {:reply, reply, state}

      action not in [:reserve, :cancel] ->
        {:reply, {:error, :unknown_operation}, state}

      true ->
        operation = %{
          status: :running,
          action: action,
          started_at: System.system_time(:millisecond)
        }

        operations = Map.put(state.operations, operation_id, operation)

        case persist(operations, state.reservations) do
          :ok ->
            state = %{state | operations: operations}
            enqueue_or_start(state, operation_id, action, args, from)

          {:error, reason} ->
            {:reply, {:error, {:persistence_error, reason}}, state}
        end
    end
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       max_concurrency: state.max_concurrency,
       active: map_size(state.active),
       queued: :queue.len(state.queue),
       operations: state.operations
     }, state}
  end

  def handle_call(:in_progress, _from, state) do
    operations =
      state.operations
      |> Enum.filter(fn {_id, operation} -> operation.status == :running end)
      |> Enum.map(fn {id, operation} -> Map.put(operation, :id, id) end)

    {:reply, operations, state}
  end

  def handle_call(:reservations, _from, state) do
    {:reply, Map.values(state.reservations), state}
  end

  @impl true
  def handle_info({:prepared, operation_id, prepared}, state) do
    case Map.pop(state.active, operation_id) do
      {nil, _active} ->
        {:noreply, state}

      {worker, active} ->
        Process.demonitor(worker.monitor, [:flush])
        state = %{state | active: active}

        {result, state} =
          case prepared do
            {:ok, command} ->
              {result, reservations} = commit(command, operation_id, state.reservations)
              complete_operation(state, operation_id, result, reservations)

            {:error, _reason} = error ->
              complete_operation(state, operation_id, error, state.reservations)
          end

        GenServer.reply(worker.from, result)
        {:noreply, start_queued(state)}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Enum.find(state.active, fn {_id, worker} -> worker.monitor == monitor end) do
      nil ->
        {:noreply, state}

      {operation_id, worker} ->
        active = Map.delete(state.active, operation_id)
        state = %{state | active: active}
        result = {:error, {:worker_failed, reason}}
        {result, state} = complete_operation(state, operation_id, result, state.reservations)
        GenServer.reply(worker.from, result)
        {:noreply, start_queued(state)}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  catch
    _, _ -> :ok
  end

  defp enqueue_or_start(state, operation_id, action, args, from) do
    if map_size(state.active) < state.max_concurrency do
      {:noreply, launch(state, operation_id, action, args, from)}
    else
      queue = :queue.in({operation_id, action, args, from}, state.queue)
      {:noreply, %{state | queue: queue}}
    end
  end

  defp launch(state, operation_id, action, args, from) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        send(parent, {:prepared, operation_id, prepare(action, args)})
      end)

    worker = %{pid: pid, monitor: monitor, from: from}
    %{state | active: Map.put(state.active, operation_id, worker)}
  end

  defp start_queued(state) do
    if map_size(state.active) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, {operation_id, action, args, from}}, queue} ->
          state
          |> Map.put(:queue, queue)
          |> launch(operation_id, action, args, from)
          |> start_queued()

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp prepare(:reserve, [room, starts_at, ends_at])
       when not is_nil(room) and is_integer(starts_at) and is_integer(ends_at) and
              starts_at < ends_at do
    {:ok, {:reserve, room, starts_at, ends_at}}
  end

  defp prepare(:reserve, _args), do: {:error, :invalid_reservation}

  defp prepare(:cancel, [reservation_id]) when not is_nil(reservation_id) do
    {:ok, {:cancel, reservation_id}}
  end

  defp prepare(:cancel, _args), do: {:error, :invalid_reservation_id}

  defp commit({:reserve, room, starts_at, ends_at}, operation_id, reservations) do
    conflict? =
      Enum.any?(reservations, fn {_id, reservation} ->
        reservation.room == room and
          starts_at < reservation.ends_at and
          reservation.starts_at < ends_at
      end)

    if conflict? do
      {{:error, :room_unavailable}, reservations}
    else
      reservation = %{
        id: operation_id,
        room: room,
        starts_at: starts_at,
        ends_at: ends_at
      }

      {{:ok, reservation}, Map.put(reservations, operation_id, reservation)}
    end
  end

  defp commit({:cancel, reservation_id}, _operation_id, reservations) do
    case Map.pop(reservations, reservation_id) do
      {nil, _reservations} ->
        {{:error, :not_found}, reservations}

      {reservation, remaining} ->
        {{:ok, reservation}, remaining}
    end
  end

  defp complete_operation(state, operation_id, result, reservations) do
    operation =
      state.operations
      |> Map.fetch!(operation_id)
      |> Map.merge(%{status: :done, result: result, completed_at: System.system_time(:millisecond)})

    operations = Map.put(state.operations, operation_id, operation)

    case persist(operations, reservations) do
      :ok ->
        {result, %{state | operations: operations, reservations: reservations}}

      {:error, reason} ->
        failure = {:error, {:persistence_error, reason}}

        failed_operation =
          Map.merge(operation, %{result: failure})

        failed_operations = Map.put(state.operations, operation_id, failed_operation)
        _ = persist(failed_operations, state.reservations)

        {failure, %{state | operations: failed_operations}}
    end
  end

  defp recover_interrupted(operations) do
    Map.new(operations, fn
      {id, %{status: :running} = operation} ->
        {id,
         Map.merge(operation, %{
           status: :done,
           result: {:error, :interrupted},
           completed_at: System.system_time(:millisecond)
         })}

      entry ->
        entry
    end)
  end

  defp persist(operations, reservations) do
    snapshot = %{operations: operations, reservations: reservations}

    try do
      case :dets.insert(@table, {:snapshot, snapshot}) do
        :ok -> :dets.sync(@table)
        {:error, reason} -> {:error, reason}
      end
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end
end
