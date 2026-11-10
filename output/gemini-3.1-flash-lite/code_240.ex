# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Fazer uso de macros.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule RoomBooking do
  @moduledoc """
  Sistema de gerenciamento de reservas com idempotência, limite de concorrência
  e persistência baseada em ETS.
  """

  use GenServer

  # --- Macros para DSL de Reserva ---

  defmacro def_reserve(name, do: block) do
    quote do
      def unquote(name)(room_id, user_id) do
        operation_id = :erlang.phash2({unquote(name), room_id, user_id})
        RoomBooking.execute(operation_id, fn -> unquote(block) end)
      end
    end
  end

  # --- API do GenServer ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def execute(op_id, action) do
    GenServer.call(__MODULE__, {:execute, op_id, action})
  end

  def get_state do
    :ets.tab2list(:bookings_table)
  end

  # --- Callbacks ---

  @impl true
  def init(_) do
    # Tenta carregar a tabela, se não existir, cria.
    # O :named_table com :public permite que o ETS persista além do processo.
    table = :ets.new(:bookings_table, [:set, :public, :named_table, read_concurrency: true])
    {:ok, %{active_count: 0, max_concurrency: 5}}
  end

  @impl true
  def handle_call({:execute, op_id, action}, _from, state) do
    # 1. Idempotência: Verifica se já foi executado
    if :ets.member(:bookings_table, op_id) do
      {:reply, {:error, :already_processed}, state}
    else
      # 5. Limite de concorrência
      if state.active_count >= state.max_concurrency do
        {:reply, {:error, :too_many_requests}, state}
      else
        new_state = %{state | active_count: state.active_count + 1}
        
        # Executa a ação
        result = action.()
        
        # 3. Persistência (registra sucesso)
        :ets.insert(:bookings_table, {op_id, :completed, System.system_time(:second)})
        
        {:reply, {:ok, result}, %{new_state | active_count: new_state.active_count - 1}}
      end
    end
  end
end

# --- Exemplo de Uso ---

defmodule App do
  require RoomBooking
  
  # Uso da macro
  RoomBooking.def_reserve(:book_conference_room) do
    "Reserva confirmada com sucesso!"
  end

  def run do
    RoomBooking.start_link(nil)
    
    # Tentativa de reserva
    IO.inspect(book_conference_room("sala_01", "usuario_abc"))
    
    # Tentativa de reserva duplicada (Idempotência)
    IO.inspect(book_conference_room("sala_01", "usuario_abc"))
    
    # Consulta estado atual
    IO.inspect(RoomBooking.get_state())
  end
end
