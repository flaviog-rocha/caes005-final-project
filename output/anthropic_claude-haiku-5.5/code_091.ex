# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomBooking do
  @moduledoc """
  Gerenciamento de reservas de salas com:

  1. Idempotência: cada operação é identificada por um `op_id`. Se a mesma
     operação for enviada novamente para a mesma sala, ela não é executada de
     novo e o resultado original é devolvido com `{:already_applied, result}`.
  2. Histórico: todas as operações aplicadas são registradas por sala, na ordem
     em que ocorreram.
  3. Independência: cada sala é um `Agent` próprio, gerenciado por um
     `DynamicSupervisor` e localizado via `Registry`. Operações em salas
     diferentes não se bloqueiam nem compartilham estado.

  Dentro de uma mesma sala, `Agent.get_and_update/2` serializa as operações,
  tornando atômicas a verificação de idempotência, a checagem de conflito e a
  gravação do histórico.

  Os tempos são representados por `NaiveDateTime`. Os intervalos são
  semiabertos: `[start_at, end_at)`.
  """

  @registry RoomBooking.Registry
  @room_sup RoomBooking.RoomSupervisor

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o Registry e o DynamicSupervisor necessários para as salas.
  """
  def start_link(_opts \\ []) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @room_sup}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Reserva a sala `room_id` para `user` no intervalo `[start_at, end_at)`.

  Retorna `{:applied, {:ok, booking}}`, `{:applied, {:error, reason}}` na
  primeira execução, ou `{:already_applied, result}` se `op_id` já foi usado
  nessa sala.
  """
  def reserve(room_id, op_id, user, %NaiveDateTime{} = start_at, %NaiveDateTime{} = end_at) do
    op = %{
      type: :reserve,
      payload: %{user: user, start_at: start_at, end_at: end_at}
    }

    execute(room_id, op_id, op, &do_reserve(&1, room_id, user, start_at, end_at))
  end

  @doc """
  Cancela a reserva `booking_id` da sala `room_id`.

  Segue as mesmas regras de idempotência de `reserve/5`.
  """
  def cancel(room_id, op_id, booking_id) do
    op = %{type: :cancel, payload: %{booking_id: booking_id}}

    execute(room_id, op_id, op, &do_cancel(&1, booking_id))
  end

  @doc """
  Lista as reservas ativas da sala, ordenadas pelo id.
  """
  def bookings(room_id) do
    case fetch_room(room_id) do
      nil ->
        []

      pid ->
        Agent.get(pid, fn state ->
          state.bookings
          |> Map.values()
          |> Enum.sort_by(& &1.id)
        end)
    end
  end

  @doc """
  Retorna o histórico de operações aplicadas à sala, em ordem cronológica.
  """
  def history(room_id) do
    case fetch_room(room_id) do
      nil -> []
      pid -> Agent.get(pid, fn state -> Enum.reverse(state.history) end)
    end
  end

  # ---------------------------------------------------------------------------
  # Execução idempotente (serializada pelo Agent da sala)
  # ---------------------------------------------------------------------------

  defp execute(room_id, op_id, op, fun) do
    pid = ensure_room(room_id)

    Agent.get_and_update(pid, fn state ->
      case Map.fetch(state.ops, op_id) do
        # Operação já executada: devolve o resultado original sem alterar o estado.
        {:ok, previous_result} ->
          {{:already_applied, previous_result}, state}

        # Primeira execução: aplica a lógica, registra o histórico e o op_id.
        :error ->
          {result, new_state} = fun.(state)
          new_state = record(new_state, op_id, op, result)
          {{:applied, result}, new_state}
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio (funções puras sobre o estado da sala)
  # ---------------------------------------------------------------------------

  defp do_reserve(state, room_id, user, start_at, end_at) do
    cond do
      NaiveDateTime.compare(start_at, end_at) != :lt ->
        {{:error, :invalid_interval}, state}

      conflict?(state.bookings, start_at, end_at) ->
        {{:error, :conflict}, state}

      true ->
        id = state.next_booking_id

        booking = %{
          id: id,
          room_id: room_id,
          user: user,
          start_at: start_at,
          end_at: end_at
        }

        new_state = %{
          state
          | bookings: Map.put(state.bookings, id, booking),
            next_booking_id: id + 1
        }

        {{:ok, booking}, new_state}
    end
  end

  defp do_cancel(state, booking_id) do
    case Map.pop(state.bookings, booking_id) do
      {nil, _} ->
        {{:error, :not_found}, state}

      {booking, remaining} ->
        {{:ok, booking}, %{state | bookings: remaining}}
    end
  end

  # Dois intervalos [a1, a2) e [b1, b2) se sobrepõem quando a1 < b2 e b1 < a2.
  defp conflict?(bookings, start_at, end_at) do
    Enum.any?(bookings, fn {_id, b} ->
      NaiveDateTime.compare(start_at, b.end_at) == :lt and
        NaiveDateTime.compare(b.start_at, end_at) == :lt
    end)
  end

  # ---------------------------------------------------------------------------
  # Histórico
  # ---------------------------------------------------------------------------

  defp record(state, op_id, op, result) do
    seq = state.seq + 1

    entry = %{
      seq: seq,
      op_id: op_id,
      type: op.type,
      payload: op.payload,
      result: result,
      at: DateTime.utc_now()
    }

    %{
      state
      | seq: seq,
        ops: Map.put(state.ops, op_id, result),
        history: [entry | state.history]
    }
  end

  # ---------------------------------------------------------------------------
  # Gerenciamento de processos por sala
  # ---------------------------------------------------------------------------

  defp via(room_id), do: {:via, Registry, {@registry, room_id}}

  defp fetch_room(room_id) do
    case Registry.lookup(@registry, room_id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  # Retorna o pid do Agent da sala, criando-o sob demanda se necessário.
  defp ensure_room(room_id) do
    case fetch_room(room_id) do
      nil ->
        spec = %{
          id: {:room, room_id},
          start: {Agent, :start_link, [fn -> init_state(room_id) end, [name: via(room_id)]]},
          restart: :transient
        }

        case DynamicSupervisor.start_child(@room_sup, spec) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      pid ->
        pid
    end
  end

  defp init_state(room_id) do
    %{
      room_id: room_id,
      bookings: %{},
      next_booking_id: 1,
      ops: %{},
      history: [],
      seq: 0
    }
  end
end

# Exemplo de uso (para referência, não faz parte do módulo):
#
#   {:ok, _sup} = RoomBooking.start_link()
#
#   inicio = ~N[2025-01-10 09:00:00]
#   fim    = ~N[2025-01-10 10:00:00]
#
#   {:applied, {:ok, reserva}} = RoomBooking.reserve("sala-1", "op-001", "ana", inicio, fim)
#   {:already_applied, {:ok, _}} = RoomBooking.reserve("sala-1", "op-001", "ana", inicio, fim)
#   {:applied, {:error, :conflict}} = RoomBooking.reserve("sala-1", "op-002", "bruno", inicio, fim)
#   {:applied, {:ok, _}} = RoomBooking.reserve("sala-2", "op-001", "bruno", inicio, fim)
#   {:applied, {:ok, _}} = RoomBooking.cancel("sala-1", "op-003", reserva.id)
#
#   RoomBooking.bookings("sala-1")
#   RoomBooking.history("sala-1")
