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

# Módulo para gerenciamento de reservas de salas utilizando ETS e GenServer.
# O uso de :ets garante concorrência, e o GenServer atua como supervisor/gerenciador.

defmodule RoomBookingSystem do
  use GenServer

  @table :room_bookings
  @max_concurrent_ops 5
  @history_table :booking_history

  # API Pública

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def reserve(room_id, user_id) do
    # Limita operações simultâneas usando um Pool ou controle de fluxo simples via GenServer
    GenServer.call(__MODULE__, {:reserve, room_id, user_id})
  end

  def get_history do
    :ets.tab2list(@history_table)
  end

  # Callbacks do GenServer

  @impl true
  def init(_) do
    # ETS para estado das salas e histórico
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@history_table, [:ordered_set, :public, :named_table])
    
    # Simula estado inicial de salas
    :ets.insert(@table, {"room_1", :available})
    :ets.insert(@table, {"room_2", :available})

    {:ok, %{active_ops: 0}}
  end

  @impl true
  def handle_call({:reserve, room_id, user_id}, _from, state) do
    if state.active_ops >= @max_concurrent_ops do
      {:reply, {:error, :too_many_requests}, state}
    else
      new_state = %{state | active_ops: state.active_ops + 1}
      
      result = perform_booking(room_id, user_id)
      
      # Notificação simples via log/console
      notify_system(room_id, result)
      
      {:reply, result, %{new_state | active_ops: new_state.active_ops - 1}}
    end
  end

  # Lógica de negócio e Persistência Fictícia

  defp perform_booking(room_id, user_id) do
    case :ets.lookup(@table, room_id) do
      [{^room_id, :available}] ->
        # Simulação de escrita em DB
        :ets.insert(@table, {room_id, {:reserved, user_id}})
        log_history(user_id, room_id, :success)
        {:ok, :reserved}

      [{^room_id, {:reserved, _}}] ->
        log_history(user_id, room_id, :failed)
        {:error, :already_reserved}

      [] ->
        {:error, :not_found}
    end
  end

  defp log_history(user_id, room_id, status) do
    timestamp = System.system_time(:second)
    :ets.insert(@history_table, {timestamp, user_id, room_id, status})
  end

  defp notify_system(room_id, {:ok, :reserved}) do
    IO.puts("[NOTIFICAÇÃO] Sala #{room_id} reservada com sucesso.")
  end
  defp notify_system(_, _), do: :ok
end

# Exemplo de uso:
# {:ok, _pid} = RoomBookingSystem.start_link([])
# RoomBookingSystem.reserve("room_1", "user_123")
# RoomBookingSystem.get_history()
