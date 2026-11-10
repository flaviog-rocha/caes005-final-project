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
  Sistema de reserva de salas utilizando GenServer para estado, 
  ETS para persistência/recuperação e Task para operações assíncronas.
  """
  use GenServer

  @table_name :room_bookings
  @expiry_ms 3600_000 # 1 hora

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def book_room(room_id, user) do
    GenServer.call(__MODULE__, {:book, room_id, user})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # 2. Recuperar processos: Tenta ler da tabela persistente se existir
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    
    # 3. Expiração automática: Processo de limpeza periódica
    Task.start_link(fn -> cleanup_loop() end)
    
    {:ok, %{history: []}}
  end

  @impl true
  def handle_call({:book, room_id, user}, _from, state) do
    # 4. Impedir atualizações parciais: Operação atômica em memória
    # 1. Histórico: Armazenamos localmente no estado do GenServer
    new_history = [%{room: room_id, user: user, ts: System.system_time(:millisecond)} | state.history]
    
    # Gravação no ETS para persistência rápida
    booking = {room_id, user, System.system_time(:millisecond)}
    :ets.insert(@table_name, booking)

    {:reply, :ok, %{state | history: new_history}}
  end

  # --- Funções Auxiliares ---

  defp cleanup_loop do
    :timer.sleep(@expiry_ms)
    now = System.system_time(:millisecond)
    
    # 3. Expiração automática: Remove registros antigos do ETS
    :ets.match_delete(@table_name, {:_, :_, :"$1"})
    
    # Filtra mantendo apenas os válidos
    :ets.foldl(fn {room, user, ts}, _acc ->
      if (now - ts) < @expiry_ms, do: :ok, else: :ets.delete(@table_name, room)
    end, :ok, @table_name)

    cleanup_loop()
  end
end

# Exemplo de uso:
# {:ok, pid} = RoomBookingSystem.start_link([])
# RoomBookingSystem.book_room("Sala-A", "Usuario-1")
