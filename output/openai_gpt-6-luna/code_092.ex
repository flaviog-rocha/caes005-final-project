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

defmodule RoomReservations do
  @moduledoc """
  Gerencia reservas de salas com operações idempotentes e histórico por entidade.
  """

  def start_link(opts \\ []) do
    Agent.start_link(fn -> %{entities: %{}} end, Keyword.put_new(opts, :name, __MODULE__))
  end

  def reserve(server \\ __MODULE__, entity, operation_id, reservation_id, room, details \\ %{}) do
    execute(server, entity, operation_id, :reserve, %{
      reservation_id: reservation_id,
      room: room,
      details: details
    })
  end

  def cancel(server \\ __MODULE__, entity, operation_id, reservation_id) do
    execute(server, entity, operation_id, :cancel, %{reservation_id: reservation_id})
  end

  def reservations(server \\ __MODULE__, entity) do
    Agent.get(server, fn state ->
      state
      |> get_entity(entity)
      |> Map.fetch!(:reservations)
      |> Map.values()
    end)
  end

  def history(server \\ __MODULE__, entity) do
    Agent.get(server, fn state ->
      state
      |> get_entity(entity)
      |> Map.fetch!(:history)
      |> Enum.reverse()
    end)
  end

  defp execute(server, entity, operation_id, action, payload) do
    Agent.get_and_update(server, fn state ->
      entity_state = get_entity(state, entity)

      case Map.fetch(entity_state.operations, operation_id) do
        {:ok, previous_result} ->
          {previous_result, state}

        :error ->
          {result, updated_entity} = apply_operation(entity_state, action, payload)
          updated_entity = record_operation(updated_entity, operation_id, action, payload, result)

          updated_state = put_in(state, [:entities, entity], updated_entity)
          {result, updated_state}
      end
    end)
  end

  defp apply_operation(entity_state, :reserve, payload) do
    reservation_id = payload.reservation_id
    room = payload.room

    cond do
      Map.has_key?(entity_state.reservations, reservation_id) ->
        {{:error, :reservation_id_exists}, entity_state}

      Enum.any?(entity_state.reservations, fn {_id, reservation} -> reservation.room == room end) ->
        {{:error, :room_unavailable}, entity_state}

      true ->
        reservation = %{
          id: reservation_id,
          room: room,
          details: payload.details
        }

        updated = put_in(entity_state, [:reservations, reservation_id], reservation)
        {{:ok, reservation}, updated}
    end
  end

  defp apply_operation(entity_state, :cancel, payload) do
    reservation_id = payload.reservation_id

    case Map.fetch(entity_state.reservations, reservation_id) do
      {:ok, reservation} ->
        updated = update_in(entity_state.reservations, &Map.delete(&1, reservation_id))
        {{:ok, reservation}, updated}

      :error ->
        {{:error, :reservation_not_found}, entity_state}
    end
  end

  defp record_operation(entity_state, operation_id, action, payload, result) do
    entry = %{
      operation_id: operation_id,
      action: action,
      payload: payload,
      result: result,
      sequence: entity_state.sequence + 1,
      timestamp: System.system_time(:millisecond)
    }

    entity_state
    |> put_in([:operations, operation_id], result)
    |> update_in([:history], &[entry | &1])
    |> update_in([:sequence], &(&1 + 1))
  end

  defp get_entity(state, entity) do
    Map.get(state.entities, entity, %{
      reservations: %{},
      operations: %{},
      history: [],
      sequence: 0
    })
  end
end
