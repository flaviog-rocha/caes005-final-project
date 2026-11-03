# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Recuperar processos após falhas inesperadas.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir e não adicione dependências externas. Inclua testes ExUnit que verifiquem os requisitos. Não escreva nada além do código (e comentários do código).

defmodule RoomReservations.Manager do
  use GenServer

  @call_timeout 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  def reserve(server, operation_key, room, guest, starts_at, ends_at) do
    safe_call(server, {:reserve, operation_key, room, guest, starts_at, ends_at})
  end

  def cancel(server, operation_key, reservation_id) do
    safe_call(server, {:cancel, operation_key, reservation_id})
  end

  def reservations(server) do
    safe_call(server, :reservations)
  end

  @impl true
  def init(opts) do
    storage_key = {:room_reservations_state, Keyword.fetch!(opts, :storage_key)}

    state =
      case :persistent_term.get(storage_key, :undefined) do
        :undefined ->
          initial = %{reservations: %{}, operations: %{}, next_id: 1}
          :persistent_term.put(storage_key, initial)
          initial

        saved ->
          saved
      end

    {:ok, %{data: state, storage_key: storage_key}}
  end

  @impl true
  def handle_call({:reserve, key, room, guest, starts_at, ends_at}, _from, state) do
    request = {:reserve, room, guest, starts_at, ends_at}

    with :ok <- validate_key(key),
         :ok <- validate_reservation(room, guest, starts_at, ends_at) do
      execute_once(state, key, request, fn data ->
        if overlaps?(data.reservations, room, starts_at, ends_at) do
          {{:error, {:validation, :room_unavailable}}, data}
        else
          id = data.next_id

          reservation = %{
            id: id,
            room: room,
            guest: guest,
            starts_at: starts_at,
            ends_at: ends_at
          }

          result = {:ok, reservation}

          {result,
           %{
             data
             | reservations: Map.put(data.reservations, id, reservation),
               next_id: id + 1
           }}
        end
      end)
      |> persist_and_reply(state)
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:cancel, key, reservation_id}, _from, state) do
    request = {:cancel, reservation_id}

    with :ok <- validate_key(key),
         :ok <- validate_reservation_id(reservation_id) do
      execute_once(state, key, request, fn data ->
        if Map.has_key?(data.reservations, reservation_id) do
          result = {:ok, :cancelled}
          {result, %{data | reservations: Map.delete(data.reservations, reservation_id)}}
        else
          {{:error, {:validation, :reservation_not_found}}, data}
        end
      end)
      |> persist_and_reply(state)
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call(:reservations, _from, state) do
    result =
      state.data.reservations
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, result}, state}
  end

  defp execute_once(state, key, request, operation) do
    case Map.fetch(state.data.operations, key) do
      {:ok, %{request: ^request, result: result}} ->
        {:reply, result, state}

      {:ok, _different_request} ->
        {:reply, {:error, {:validation, :idempotency_key_reused}}, state}

      :error ->
        {result, updated_data} = operation.(state.data)

        updated_data = %{
          updated_data
          | operations:
              Map.put(updated_data.operations, key, %{request: request, result: result})
        }

        {:updated, result, %{state | data: updated_data}}
    end
  end

  defp persist_and_reply({:reply, result, state}, _original_state) do
    {:reply, result, state}
  end

  defp persist_and_reply({:updated, result, state}, _original_state) do
    :persistent_term.put(state.storage_key, state.data)
    {:reply, result, state}
  end

  defp validate_key(key) when is_binary(key) do
    if String.trim(key) == "" do
      {:error, {:validation, :invalid_idempotency_key}}
    else
      :ok
    end
  end

  defp validate_key(_), do: {:error, {:validation, :invalid_idempotency_key}}

  defp validate_reservation(room, guest, starts_at, ends_at) do
    cond do
      not valid_text?(room) -> {:error, {:validation, :invalid_room}}
      not valid_text?(guest) -> {:error, {:validation, :invalid_guest}}
      not is_integer(starts_at) or not is_integer(ends_at) ->
        {:error, {:validation, :invalid_time}}

      starts_at >= ends_at ->
        {:error, {:validation, :invalid_time_range}}

      true ->
        :ok
    end
  end

  defp validate_reservation_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_reservation_id(_), do: {:error, {:validation, :invalid_reservation_id}}

  defp valid_text?(value) when is_binary(value), do: String.trim(value) != ""
  defp valid_text?(_), do: false

  defp overlaps?(reservations, room, starts_at, ends_at) do
    Enum.any?(reservations, fn {_id, reservation} ->
      reservation.room == room and
        starts_at < reservation.ends_at and ends_at > reservation.starts_at
    end)
  end

  defp safe_call(server, message) do
    try do
      GenServer.call(server, message, @call_timeout)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end

defmodule RoomReservations.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    manager_opts = [
      name: Keyword.fetch!(opts, :name),
      storage_key: Keyword.get(opts, :storage_key, Keyword.fetch!(opts, :name))
    ]

    children = [
      %{
        id: RoomReservations.Manager,
        start: {RoomReservations.Manager, :start_link, [manager_opts]},
        restart: :permanent,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

ExUnit.start()

defmodule RoomReservationsTest do
  use ExUnit.Case, async: false

  setup do
    unique = System.unique_integer([:positive])
    name = String.to_atom("room_reservations_#{unique}")
    storage_key = {:test, unique}

    {:ok, supervisor} =
      RoomReservations.Supervisor.start_link(name: name, storage_key: storage_key)

    on_exit(fn ->
      if Process.alive?(supervisor), do: Supervisor.stop(supervisor)
    end)

    %{name: name, supervisor: supervisor}
  end

  test "replays an operation and rejects reuse of its key for another request", %{name: name} do
    first = RoomReservations.Manager.reserve(name, "op-1", "A", "Ada", 10, 20)
    assert {:ok, reservation} = first
    assert RoomReservations.Manager.reserve(name, "op-1", "A", "Ada", 10, 20) == first
    assert RoomReservations.Manager.reservations(name) == {:ok, [reservation]}

    assert {:error, {:validation, :idempotency_key_reused}} =
             RoomReservations.Manager.reserve(name, "op-1", "B", "Ada", 30, 40)
  end

  test "serializes concurrent reservations so overlapping bookings cannot both succeed",
       %{name: name} do
    results =
      1..2
      |> Task.async_stream(
        fn index ->
          RoomReservations.Manager.reserve(
            name,
            "parallel-#{index}",
            "A",
            "Guest #{index}",
            100,
            200
          )
        end,
        max_concurrency: 2
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, {:validation, :room_unavailable}}, &1)) == 1
    assert {:ok, [_]} = RoomReservations.Manager.reservations(name)
  end

  test "recovers persisted reservations after the manager process crashes", %{
    name: name,
    supervisor: supervisor
  } do
    assert {:ok, reservation} =
             RoomReservations.Manager.reserve(name, "recover-op", "A", "Ada", 10, 20)

    old_pid = Process.whereis(name)
    Process.exit(old_pid, :kill)

    assert eventually(fn ->
             new_pid = Process.whereis(name)
             is_pid(new_pid) and new_pid != old_pid
           end)

    assert {:ok, [^reservation]} = RoomReservations.Manager.reservations(name)
    assert Process.alive?(supervisor)
  end

  test "returns validation errors separately from operational failures", %{name: name} do
    assert {:error, {:validation, :invalid_time_range}} =
             RoomReservations.Manager.reserve(name, "bad-time", "A", "Ada", 20, 10)

    assert {:error, {:operational, _reason}} =
             RoomReservations.Manager.reservations(:room_reservations_missing_server)
  end

  test "makes cancellation idempotent", %{name: name} do
    assert {:ok, reservation} =
             RoomReservations.Manager.reserve(name, "reserve-op", "A", "Ada", 10, 20)

    assert {:ok, :cancelled} =
             RoomReservations.Manager.cancel(name, "cancel-op", reservation.id)

    assert {:ok, :cancelled} =
             RoomReservations.Manager.cancel(name, "cancel-op", reservation.id)

    assert {:ok, []} = RoomReservations.Manager.reservations(name)
  end

  defp eventually(predicate, attempts \\ 50)
  defp eventually(_predicate, 0), do: false

  defp eventually(predicate, attempts) do
    if predicate.() do
      true
    else
      Process.sleep(20)
      eventually(predicate, attempts - 1)
    end
  end
end
