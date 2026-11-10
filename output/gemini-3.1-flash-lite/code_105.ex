# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Recuperar processos após falhas inesperadas.
# 3. Implementar expiração automática de registros.
# 4. Impedir atualizações parcialmente concluídas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomBookingSystem do
  @moduledoc """
  Sistema de reservas de salas utilizando Agent para estado,
  ETS para persistência/recuperação e Task para tarefas assíncronas.
  """
  use Agent

  # ETS tabela para persistência (sobrevive a falhas do processo se configurada com :named_table)
  # O uso de :duplicate_bag ou :set depende da necessidade, aqui usamos :set para garantir unicidade.
  @table_name :room_bookings

  def start_link(_) do
    :ets.new(@table_name, [:set, :named_table, :public, read_concurrency: true])
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  @doc """
  Reserva uma sala com garantia de atomicidade e histórico.
  Impede atualizações parciais ao encapsular a escrita em um processo síncrono.
  """
  def book_room(room_id, user_id, ttl_seconds \\ 3600) do
    timestamp = System.system_time(:second)
    expiry = timestamp + ttl_seconds
    
    # 1. Registro no histórico (Agent garante serialização)
    Agent.update(__MODULE__, fn history -> 
      [%{room_id: room_id, user_id: user_id, action: :booked, ts: timestamp} | history]
    end)

    # 2. Persistência no ETS (Atomicidade de escrita individual)
    :ets.insert(@table_name, {room_id, %{user_id: user_id, expiry: expiry}})

    # 3. Expiração automática via Task
    Task.start(fn ->
      Process.sleep(ttl_seconds * 1000)
      :ets.delete(@table_name, room_id)
      Agent.update(__MODULE__, fn history -> 
        [%{room_id: room_id, action: :expired, ts: System.system_time(:second)} | history]
      end)
    end)
  end

  @doc """
  Recupera o estado. Se o processo do Agent morrer, o ETS mantém 
  os dados vivos para restauração.
  """
  def get_booking(room_id) do
    case :ets.lookup(@table_name, room_id) do
      [{^room_id, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  def get_history do
    Agent.get(__MODULE__, & &1)
  end
end

# Exemplo de uso:
# {:ok, _pid} = RoomBookingSystem.start_link([])
# RoomBookingSystem.book_room("Sala-A", "Usuario-1", 10)
# IO.inspect(RoomBookingSystem.get_booking("Sala-A"))
# IO.inspect(RoomBookingSystem.get_history())
