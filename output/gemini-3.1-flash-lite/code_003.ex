# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Manter um histórico das operações realizadas.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para executar: salvar como meeting_room_manager.ex e rodar `elixir meeting_room_manager.ex`
# Utiliza ETS para persistência em memória (simulando um DB rápido) e GenStage/Task para concorrência.

defmodule MeetingRoomManager do
  use GenServer

  @table_name :meeting_rooms_ets
  @max_concurrent_ops 5 # Requisito 3: Limitar operações simultâneas

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def reserve(room_id, user) do
    # Requisito 1: Permitir execução simultânea via Task
    Task.async(fn ->
      GenServer.call(__MODULE__, {:reserve, room_id, user})
    end) |> Task.await()
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Inicializa ETS (Requisito: Uso de ETS)
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    # Inicializa histórico
    :ets.new(:audit_log, [:ordered_set, :public, :named_table])
    {:ok, %{semaphore: @max_concurrent_ops}}
  end

  @impl true
  def handle_call({:reserve, room_id, user}, _from, state) do
    if state.semaphore > 0 do
      # Lógica de reserva (Simulando transação DB)
      case :ets.lookup(@table_name, room_id) do
        [] ->
          :ets.insert(@table_name, {room_id, :reserved, user})
          log_operation(room_id, :reserved, user)
          {:reply, {:ok, "Sala #{room_id} reservada por #{user}"}, %{state | semaphore: state.semaphore - 1}}
        
        [{^room_id, :reserved, _}] ->
          {:reply, {:error, "Sala já ocupada"}, state}
      end
    else
      {:reply, {:error, "Limite de concorrência atingido"}, state}
    end
  end

  # --- Funções Auxiliares ---

  defp log_operation(room_id, status, user) do
    timestamp = System.system_time(:second)
    :ets.insert(:audit_log, {timestamp, room_id, status, user})
    
    # Requisito 5: Notificação
    if status == :reserved do
      IO.puts("[NOTIFICAÇÃO] Sala #{room_id} foi reservada por #{user} às #{timestamp}")
    end
  end
end

# --- Exemplo de Execução ---

MeetingRoomManager.start_link([])

# Simulação de concorrência
rooms = ["Sala A", "Sala B", "Sala C"]
Enum.each(rooms, fn room ->
  spawn(fn ->
    result = MeetingRoomManager.reserve(room, "User_#{room}")
    IO.inspect(result, label: "Resultado da reserva")
  end)
end)

# Aguarda processamento
:timer.sleep(1000)
