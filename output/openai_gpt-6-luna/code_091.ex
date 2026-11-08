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
  Gerencia reservas com operações idempotentes e histórico separado por entidade.
  """

  def start_link(opts \\ []) do
    Agent.start_link(fn -> %{} end, opts)
  end

  @doc """
  Executa uma operação uma única vez para a entidade e o identificador informados.

  Operações com o mesmo identificador em entidades diferentes são independentes.
  """
  def execute(agent, entity, operation_id, operation)
      when is_function(operation, 0) do
    Agent.get_and_update(agent, fn state ->
      entity_state = Map.get(state, entity, %{operations: %{}, history: []})

      case Map.fetch(entity_state.operations, operation_id) do
        {:ok, outcome} ->
          {{:already_executed, outcome}, state}

        :error ->
          outcome = invoke(operation)

          entry = %{
            operation_id: operation_id,
            outcome: outcome
          }

          updated_entity = %{
            operations: Map.put(entity_state.operations, operation_id, outcome),
            history: [entry | entity_state.history]
          }

          result =
            case outcome do
              {:ok, value} -> {:executed, value}
              {:error, reason} -> {:failed, reason}
            end

          {result, Map.put(state, entity, updated_entity)}
      end
    end)
  end

  @doc """
  Registra uma reserva como operação idempotente.
  """
  def reserve(agent, entity, operation_id, room, details \\ %{}) do
    execute(agent, entity, operation_id, fn ->
      %{room: room, details: details}
    end)
  end

  @doc """
  Retorna o histórico de uma entidade em ordem cronológica.
  """
  def history(agent, entity) do
    Agent.get(agent, fn state ->
      state
      |> Map.get(entity, %{history: []})
      |> Map.fetch!(:history)
      |> Enum.reverse()
    end)
  end

  @doc """
  Retorna o histórico de todas as entidades, separado por entidade.
  """
  def history(agent) do
    Agent.get(agent, fn state ->
      Map.new(state, fn {entity, entity_state} ->
        {entity, Enum.reverse(entity_state.history)}
      end)
    end)
  end

  defp invoke(operation) do
    try do
      {:ok, operation.()}
    rescue
      exception -> {:error, exception}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end
end
