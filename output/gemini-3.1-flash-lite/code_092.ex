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
  Cada entidade (sala ou cliente) é gerenciada por um Agent único.
  """

  use Agent

  @doc """
  Inicia um novo gerenciador de estado para uma entidade específica.
  """
  def start_link(entity_id) do
    # O estado é um mapa com:
    # - processed_ops: MapSet para garantir idempotência
    # - history: List para manter o histórico
    Agent.start_link(fn -> %{processed_ops: MapSet.new(), history: []} end, name: via_tuple(entity_id))
  end

  @doc """
  Executa uma operação de reserva de forma idempotente.
  """
  def book_room(entity_id, operation_id, room_name) do
    Agent.get_and_update(via_tuple(entity_id), fn state ->
      if MapSet.member?(state.processed_ops, operation_id) do
        {:already_processed, state}
      else
        new_state = %{
          processed_ops: MapSet.put(state.processed_ops, operation_id),
          history: [%{op_id: operation_id, room: room_name, timestamp: DateTime.utc_now()} | state.history]
        }
        {:ok, new_state}
      end
    end)
  end

  @doc """
  Retorna o histórico de operações de uma entidade.
  """
  def get_history(entity_id) do
    Agent.get(via_tuple(entity_id), & &1.history)
  end

  defp via_tuple(entity_id), do: {:via, Registry, {RoomRegistry, entity_id}}
end

# Setup de infraestrutura necessária para o uso de nomes dinâmicos (Registry)
defmodule RoomRegistry do
  def child_spec(_), do: Registry.child_spec(keys: :unique, name: __MODULE__)
end

# Exemplo de uso:
# 1. Iniciar o Registry
# Registry.start_link(keys: :unique, name: RoomRegistry)
#
# 2. Iniciar um agente para uma entidade "usuario_1"
# RoomBookingManager.start_link("usuario_1")
#
# 3. Realizar reservas
# RoomBookingManager.book_room("usuario_1", "op_001", "Sala A") # Retorna :ok
# RoomBookingManager.book_room("usuario_1", "op_001", "Sala A") # Retorna :already_processed
#
# 4. Consultar histórico
# RoomBookingManager.get_history("usuario_1")
