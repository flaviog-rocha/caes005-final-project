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
  Gerenciamento de reservas de salas.

  - Idempotência: cada operação possui um `op_id`. Uma operação já executada
    nunca é reexecutada; a chamada retorna `{:duplicate, resultado_original}`.
  - Histórico: todas as operações executadas são registradas em ordem.
  - Independência: cada sala é um `Agent` próprio, registrado via `Registry` e
    supervisionado por um `DynamicSupervisor`. Operações em salas diferentes
    não bloqueiam umas às outras.
  """

  @registry RoomBooking.Registry
  @supervisor RoomBooking.RoomSupervisor
  @root RoomBooking.Root

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc "Inicia a árvore de supervisão (Registry + DynamicSupervisor)."
  def start_link(_opts \\ []) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @supervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: @root)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Reserva uma sala.

  `attrs` deve conter `:id`, `:user`, `:starts_at` e `:ends_at`
  (estruturas `NaiveDateTime`).

  Retorna `{:executed, {:ok, booking}}`, `{:executed, {:error, motivo}}`
  ou `{:duplicate, resultado_original}` caso `op_id` já tenha sido usado.
  """
  def reserve(room_id, op_id, attrs) do
    run(room_id, op_id, {:reserve, attrs})
  end

  @doc """
  Cancela uma reserva existente.

  Retorna `{:executed, {:ok, reservation_id}}`, `{:executed, {:error, :not_found}}`
  ou `{:duplicate, resultado_original}`.
  """
  def cancel(room_id, op_id, reservation_id) do
    run(room_id, op_id, {:cancel, reservation_id})
  end

  @doc "Retorna as reservas ativas da sala."
  def bookings(room_id) do
    {:ok, _pid} = ensure_room(room_id)
    Agent.get(via(room_id), fn state -> Map.values(state.bookings) end)
  end

  @doc "Retorna o histórico de operações da sala, da mais antiga para a mais recente."
  def history(room_id) do
    {:ok, _pid} = ensure_room(room_id)
    Agent.get(via(room_id), fn state -> Enum.reverse(state.history) end)
  end

  # ---------------------------------------------------------------------------
  # Execução de comandos
  # ---------------------------------------------------------------------------

  # A verificação de idempotência e a execução ocorrem dentro de um único
  # `Agent.get_and_update/2`, o que torna a operação atômica por sala.
  defp run(room_id, op_id, command) do
    {:ok, _pid} = ensure_room(room_id)
    Agent.get_and_update(via(room_id), fn state -> execute(state, op_id, command) end)
  end

  defp execute(state, op_id, command) do
    case Map.fetch(state.operations, op_id) do
      {:ok, previous_result} ->
        # Operação já executada: não altera o estado, apenas devolve o resultado original.
        {{:duplicate, previous_result}, state}

      :error ->
        {result, new_state} = apply_command(state, command)

        entry = %{
          op_id: op_id,
          command: command,
          result: result,
          at: NaiveDateTime.utc_now()
        }

        new_state = %{
          new_state
          | operations: Map.put(new_state.operations, op_id, result),
            history: [entry | new_state.history]
        }

        {{:executed, result}, new_state}
    end
  end

  defp apply_command(state, {:reserve, attrs}) do
    with {:ok, booking} <- build_booking(attrs),
         :ok <- check_available(state.bookings, booking) do
      new_state = %{state | bookings: Map.put(state.bookings, booking.id, booking)}
      {{:ok, booking}, new_state}
    else
      {:error, _reason} = error -> {error, state}
    end
  end

  defp apply_command(state, {:cancel, reservation_id}) do
    if Map.has_key?(state.bookings, reservation_id) do
      new_state = %{state | bookings: Map.delete(state.bookings, reservation_id)}
      {{:ok, reservation_id}, new_state}
    else
      {{:error, :not_found}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio
  # ---------------------------------------------------------------------------

  defp build_booking(%{
         id: id,
         user: user,
         starts_at: %NaiveDateTime{} = starts_at,
         ends_at: %NaiveDateTime{} = ends_at
       }) do
    case NaiveDateTime.compare(starts_at, ends_at) do
      :lt -> {:ok, %{id: id, user: user, starts_at: starts_at, ends_at: ends_at}}
      _ -> {:error, :invalid_interval}
    end
  end

  defp build_booking(_attrs), do: {:error, :invalid_attrs}

  defp check_available(bookings, booking) do
    cond do
      Map.has_key?(bookings, booking.id) ->
        {:error, :reservation_id_in_use}

      Enum.any?(Map.values(bookings), &overlaps?(&1, booking)) ->
        {:error, :conflict}

      true ->
        :ok
    end
  end

  # Intervalos [a.start, a.end) e [b.start, b.end) se sobrepõem.
  defp overlaps?(a, b) do
    NaiveDateTime.compare(a.starts_at, b.ends_at) == :lt and
      NaiveDateTime.compare(b.starts_at, a.ends_at) == :lt
  end

  # ---------------------------------------------------------------------------
  # Gerenciamento de processos por sala
  # ---------------------------------------------------------------------------

  defp via(room_id), do: {:via, Registry, {@registry, room_id}}

  defp initial_state(room_id) do
    %{
      room_id: room_id,
      bookings: %{},
      operations: %{},
      history: []
    }
  end

  # Garante que o Agent da sala exista, criando-o sob demanda.
  defp ensure_room(room_id) do
    case Registry.lookup(@registry, room_id) do
      [{pid, _value}] ->
        {:ok, pid}

      [] ->
        spec = %{
          id: {:room, room_id},
          start: {Agent, :start_link, [fn -> initial_state(room_id) end, [name: via(room_id)]]},
          restart: :temporary
        }

        case DynamicSupervisor.start_child(@supervisor, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
        end
    end
  end
end
