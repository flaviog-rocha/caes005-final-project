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

defmodule RoomBookingManager do
  @moduledoc """
  Gerenciador de reservas utilizando Agent para persistência de estado.
  - Impede duplicidade através de um conjunto de IDs de operações (Idempotência).
  - Mantém histórico completo de operações.
  - Suporta múltiplos contextos (entidades) isolados.
  """

  use Agent

  @doc "Inicia um novo gerenciador para uma entidade específica."
  def start_link(entity_id) do
    Agent.start_link(fn -> %{operations: MapSet.new(), history: []} end, name: via_tuple(entity_id))
  end

  @doc "Executa uma operação garantindo idempotência."
  def execute(entity_id, operation_id, action_func) do
    Agent.get_and_update(via_tuple(entity_id), fn state ->
      if MapSet.member?(state.operations, operation_id) do
        {:already_processed, state}
      else
        result = action_func.()
        new_state = %{
          operations: MapSet.put(state.operations, operation_id),
          history: state.history ++ [%{id: operation_id, result: result, timestamp: DateTime.utc_now()}]
        }
        {result, new_state}
      end
    end)
  end

  @doc "Retorna o histórico de operações de uma entidade."
  def get_history(entity_id) do
    Agent.get(via_tuple(entity_id), & &1.history)
  end

  defp via_tuple(entity_id), do: {:via, Registry, {RoomRegistry, entity_id}}
end

# Setup de infraestrutura mínima para o exemplo funcionar
defmodule RoomRegistry do
  def child_spec(_), do: Registry.child_spec(keys: :unique, name: __MODULE__)
end

# Exemplo de uso:
# 1. Iniciar o Registry
# Supervisor.start_link([RoomRegistry], strategy: :one_for_one)
#
# 2. Iniciar um gerenciador para a "Sala-A"
# RoomBookingManager.start_link("Sala-A")
#
# 3. Executar reserva (idempotente)
# RoomBookingManager.execute("Sala-A", "op_001", fn -> "Reserva Confirmada" end)
# RoomBookingManager.execute("Sala-A", "op_001", fn -> "Reserva Confirmada" end) # Retorna :already_processed
