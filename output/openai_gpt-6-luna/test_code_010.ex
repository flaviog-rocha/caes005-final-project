# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar expiração automática de registros.
# 3. Impedir que uma mesma operação seja executada mais de uma vez.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do ElixirNão escreva nada além do código (e comentários do código).

defmodule RoomReservations do
  use GenServer

  @moduledoc """
  Gerencia reservas de salas em um processo GenServer.

  Cada operação de escrita exige um identificador idempotente. Reservas expiram
  automaticamente após o prazo informado. O histórico e os identificadores de
  operação são mantidos em memória durante a vida do processo.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  @doc "Reserva uma sala por `ttl_ms` milissegundos."
  def reserve(server, room, holder, ttl_ms, operation_id) do
    GenServer.call(server, {:execute, operation_id, {:reserve, room, holder, ttl_ms}})
  end

  @doc "Cancela a reserva existente para a sala."
  def cancel(server, room, operation_id) do
    GenServer.call(server, {:execute, operation_id, {:cancel, room}})
  end

  @doc "Retorna a reserva atual da sala, ou `nil` se não estiver reservada."
  def reservation(server, room) do
    GenServer.call(server, {:reservation, room})
  end

  @doc "Retorna todas as reservas ativas."
  def reservations(server) do
    GenServer.call(server, :reservations)
  end

  @doc "Retorna o histórico em ordem cronológica."
  def history(server) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(_initial_state) do
    {:ok, %{reservations: %{}, operations: %{}, history: []}}
  end

  @impl true
  def handle_call({:execute, nil, _operation}, _from, state) do
    {:reply, {:error, :invalid_operation_id}, state}
  end

  def handle_call({:execute, operation_id, operation}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{request: ^operation, result: result}} ->
        {:reply, result, state}

      {:ok, _previous_operation} ->
        {:reply, {:error, :idempotency_key_reused}, state}

      :error ->
        {result, state} = execute_operation(state, operation)
        state = record_operation(state, operation_id, operation, result)

        operations =
          Map.put(state.operations, operation_id, %{request: operation, result: result})

        {:reply, result, %{state | operations: operations}}
    end
  end

  def handle_call({:reservation, room}, _from, state) do
    state = expire_room_if_due(state, room)
    result = state.reservations |> Map.get(room) |> public_reservation(room)
    {:reply, result, state}
  end

  def handle_call(:reservations, _from, state) do
    state =
      Enum.reduce(Map.keys(state.reservations), state, fn room, acc ->
        expire_room_if_due(acc, room)
      end)

    result =
      Map.new(state.reservations, fn {room, reservation} ->
        {room, public_reservation(reservation, room)}
      end)

    {:reply, result, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call(_request, _from, state) do
    {:reply, {:error, :unsupported_request}, state}
  end

  @impl true
  def handle_info({:expire, room, token}, state) do
    case Map.get(state.reservations, room) do
      %{token: ^token, expires_at: expires_at} = reservation ->
        remaining = expires_at - now_ms()

        if remaining <= 0 do
          {:noreply, expire_room_if_due(state, room)}
        else
          timer_ref = Process.send_after(self(), {:expire, room, token}, remaining)
          updated = %{reservation | timer_ref: timer_ref}
          {:noreply, put_in(state.reservations[room], updated)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp execute_operation(state, {:reserve, _room, _holder, ttl_ms})
       when not is_integer(ttl_ms) or ttl_ms <= 0 do
    {{:error, :invalid_ttl}, state}
  end

  defp execute_operation(state, {:reserve, room, holder, ttl_ms}) do
    state = expire_room_if_due(state, room)

    case Map.fetch(state.reservations, room) do
      {:ok, _reservation} ->
        {{:error, :already_reserved}, state}

      :error ->
        token = make_ref()
        expires_at = now_ms() + ttl_ms
        timer_ref = Process.send_after(self(), {:expire, room, token}, ttl_ms)

        reservation = %{
          holder: holder,
          expires_at: expires_at,
          token: token,
          timer_ref: timer_ref
        }

        state = put_in(state.reservations[room], reservation)
        {{:ok, public_reservation(reservation, room)}, state}
    end
  end

  defp execute_operation(state, {:cancel, room}) do
    state = expire_room_if_due(state, room)

    case Map.pop(state.reservations, room) do
      {nil, _reservations} ->
        {{:error, :not_reserved}, state}

      {reservation, reservations} ->
        if reservation.timer_ref, do: Process.cancel_timer(reservation.timer_ref)
        result = {:ok, public_reservation(reservation, room)}
        {result, %{state | reservations: reservations}}
    end
  end

  defp execute_operation(state, _operation) do
    {{:error, :unsupported_operation}, state}
  end

  defp expire_room_if_due(state, room) do
    case Map.get(state.reservations, room) do
      %{expires_at: expires_at} = reservation when expires_at <= now_ms() ->
        if reservation.timer_ref, do: Process.cancel_timer(reservation.timer_ref)

        state
        |> update_in([:reservations], &Map.delete(&1, room))
        |> record_expiration(room, reservation.holder)

      _ ->
        state
    end
  end

  defp record_expiration(state, room, holder) do
    record(state, %{action: :expired, room: room, holder: holder})
  end

  defp record_operation(state, operation_id, operation, result) do
    record(state, %{
      action: operation_name(operation),
      operation_id: operation_id,
      details: operation,
      result: result
    })
  end

  defp record(state, entry) do
    %{state | history: [Map.put(entry, :at, DateTime.utc_now()) | state.history]}
  end

  defp operation_name({action, _, _, _}), do: action
  defp operation_name({action, _, _}), do: action
  defp operation_name({action, _}), do: action
  defp operation_name(_), do: :unknown

  defp public_reservation(nil, _room), do: nil

  defp public_reservation(reservation, room) do
    %{
      room: room,
      holder: reservation.holder,
      expires_at: reservation.expires_at
    }
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
