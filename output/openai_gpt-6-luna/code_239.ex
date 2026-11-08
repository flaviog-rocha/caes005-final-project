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

defmodule RoomReservations.Persistence do
  @moduledoc false

  defmacro store!(table, data) do
    quote do
      :ok = :dets.insert(unquote(table), {:snapshot, unquote(data)})
      :ok = :dets.sync(unquote(table))
    end
  end
end

defmodule RoomReservations do
  @moduledoc """
  Gerencia reservas de salas com operações idempotentes, persistência em DETS
  e limite de operações simultâneas.

  `start_at` e `end_at` são inteiros; o intervalo da reserva é [start_at, end_at).
  """

  use GenServer
  require RoomReservations.Persistence

  @table :room_reservations_store

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def reserve(server, operation_id, room, start_at, end_at, user \\ nil) do
    GenServer.call(
      server,
      {:reserve, operation_id, room, start_at, end_at, user}
    )
  end

  def status(server, operation_id), do: GenServer.call(server, {:status, operation_id})
  def operations(server), do: GenServer.call(server, :operations)
  def in_progress(server), do: GenServer.call(server, :in_progress)
  def reservations(server), do: GenServer.call(server, :reservations)

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    worker_delay = Keyword.get(opts, :worker_delay, 0)
    path = opts |> Keyword.get(:path, "room_reservations.dets") |> Path.expand()

    cond do
      not (is_integer(max_concurrency) and max_concurrency > 0) ->
        {:stop, {:invalid_option, :max_concurrency}}

      not (is_integer(worker_delay) and worker_delay >= 0) ->
        {:stop, {:invalid_option, :worker_delay}}

      true ->
        case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
          {:ok, _table} ->
            state = load_state(max_concurrency, worker_delay)
            {:ok, state |> recover_interrupted() |> persist() |> dispatch()}

          {:error, reason} ->
            {:stop, {:dets_open_failed, reason}}
        end
    end
  end

  @impl true
  def handle_call(
        {:reserve, operation_id, room, start_at, end_at, user},
        _from,
        state
      ) do
    request = %{
      room: room,
      start_at: start_at,
      end_at: end_at,
      user: user
    }

    cond do
      is_nil(operation_id) ->
        {:reply, {:error, :invalid_operation_id}, state}

      not valid_request?(request) ->
        {:reply, {:error, :invalid_reservation}, state}

      Map.has_key?(state.ops, operation_id) ->
        existing = Map.fetch!(state.ops, operation_id)

        if existing.request == request do
          {:reply, {:ok, existing.status}, state}
        else
          {:reply, {:error, :operation_id_conflict}, state}
        end

      true ->
        operation = %{request: request, status: :queued, result: nil}
        state = %{state | ops: Map.put(state.ops, operation_id, operation),
                          queue: state.queue ++ [operation_id]}
        state = state |> persist() |> dispatch()
        {:reply, {:ok, :queued}, state}
    end
  end

  def handle_call({:status, operation_id}, _from, state) do
    reply =
      case Map.fetch(state.ops, operation_id) do
        {:ok, operation} -> {:ok, operation}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:operations, _from, state) do
    {:reply, state.ops, state}
  end

  def handle_call(:in_progress, _from, state) do
    operations =
      state.ops
      |> Enum.filter(fn {_id, operation} ->
        operation.status in [:queued, :running]
      end)
      |> Map.new()

    {:reply, operations, state}
  end

  def handle_call(:reservations, _from, state) do
    {:reply, state.reservations, state}
  end

  @impl true
  def handle_info({:worker_done, token}, state) do
    case Map.pop(state.running, token) do
      {nil, _running} ->
        {:noreply, state}

      {%{monitor: monitor, operation_id: operation_id}, running} ->
        Process.demonitor(monitor, [:flush])
        state = %{state | running: running}
        state = state |> complete_operation(operation_id) |> persist() |> dispatch()
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Enum.find(state.running, fn {_token, info} -> info.monitor == monitor end) do
      nil ->
        {:noreply, state}

      {token, %{operation_id: operation_id}} ->
        operation = Map.fetch!(state.ops, operation_id)
        updated = %{operation | status: :failed, result: {:error, :worker_failed}}

        state = %{
          state
          | running: Map.delete(state.running, token),
            ops: Map.put(state.ops, operation_id, updated)
        }

        {:noreply, state |> persist() |> dispatch()}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp valid_request?(%{
         room: room,
         start_at: start_at,
         end_at: end_at
       }) do
    not is_nil(room) and is_integer(start_at) and is_integer(end_at) and
      end_at > start_at
  end

  defp load_state(max_concurrency, worker_delay) do
    snapshot =
      case :dets.lookup(@table, :snapshot) do
        [{:snapshot, saved}] when is_map(saved) -> saved
        _ -> %{}
      end

    %{
      ops: Map.get(snapshot, :ops, %{}),
      reservations: Map.get(snapshot, :reservations, []),
      queue: Map.get(snapshot, :queue, []),
      running: %{},
      max_concurrency: max_concurrency,
      worker_delay: worker_delay
    }
  end

  # Operações que estavam em execução não são repetidas após uma reinicialização.
  defp recover_interrupted(state) do
    ops =
      Map.new(state.ops, fn {id, operation} ->
        if operation.status == :running do
          {id, %{operation | status: :interrupted, result: {:error, :interrupted}}}
        else
          {id, operation}
        end
      end)

    queued_ids =
      Enum.filter(state.queue, fn id ->
        case Map.get(ops, id) do
          %{status: :queued} -> true
          _ -> false
        end
      end)

    %{state | ops: ops, queue: queued_ids}
  end

  defp dispatch(state) do
    if length(state.queue) > 0 and map_size(state.running) < state.max_concurrency do
      [operation_id | queue] = state.queue
      operation = Map.fetch!(state.ops, operation_id)
      updated = %{operation | status: :running}
      token = make_ref()
      parent = self()
      delay = state.worker_delay

      {pid, monitor} =
        spawn_monitor(fn ->
          if delay > 0, do: Process.sleep(delay)
          send(parent, {:worker_done, token})
        end)

      running_info = %{monitor: monitor, pid: pid, operation_id: operation_id}

      state = %{
        state
        | queue: queue,
          running: Map.put(state.running, token, running_info),
          ops: Map.put(state.ops, operation_id, updated)
      }

      state |> persist() |> dispatch()
    else
      state
    end
  end

  defp complete_operation(state, operation_id) do
    operation = Map.fetch!(state.ops, operation_id)
    request = operation.request

    conflict? =
      Enum.any?(state.reservations, fn reservation ->
        reservation.room == request.room and
          request.start_at < reservation.end_at and
          reservation.start_at < request.end_at
      end)

    if conflict? do
      updated = %{operation | status: :rejected, result: {:error, :room_unavailable}}
      %{state | ops: Map.put(state.ops, operation_id, updated)}
    else
      reservation = Map.put(request, :operation_id, operation_id)
      updated = %{operation | status: :completed, result: {:ok, reservation}}

      %{
        state
        | ops: Map.put(state.ops, operation_id, updated),
          reservations: [reservation | state.reservations]
      }
    end
  end

  defp persist(state) do
    snapshot = %{
      ops: state.ops,
      reservations: state.reservations,
      queue: state.queue
    }

    RoomReservations.Persistence.store!(@table, snapshot)
    state
  end
end
