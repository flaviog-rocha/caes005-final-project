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
  Sistema de reserva de salas com idempotência, persistência e limitação de concorrência.
  """

  use GenServer

  # --- Macros para facilitar a API ---

  defmacro def_operation(name, do: block) do
    quote do
      def unquote(name)(booking_id, room_id) do
        GenServer.call(__MODULE__, {:execute, unquote(name), booking_id, room_id, fn -> unquote(block) end})
      end
    end
  end

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def_operation :book_room do
    :timer.sleep(1000) # Simula processamento
    :ok
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  # --- Callback GenServer ---

  @impl true
  def init(:ok) do
    # Carrega estado do disco (simulado via arquivo)
    state = load_state()
    # Semáforo para limitar operações simultâneas (ex: 5 conexões)
    {:ok, %{processed: state, semaphore: 5}}
  end

  @impl true
  def handle_call({:execute, op_name, id, room, func}, _from, state) do
    if Map.has_key?(state.processed, id) do
      {:reply, {:error, :already_processed}, state}
    else
      if state.semaphore > 0 do
        result = func.()
        new_processed = Map.put(state.processed, id, %{room: room, op: op_name, status: :completed})
        save_state(new_processed)
        {:reply, result, %{state | processed: new_processed, semaphore: state.semaphore - 1}}
      else
        {:reply, {:error, :limit_reached}, state}
      end
    end
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state.processed, state}
  end

  # --- Persistência Simples ---

  defp db_file, do: "bookings.dets"

  defp save_state(data) do
    {:ok, db} = :dets.open_file(db_file(), type: :set)
    :dets.insert(db, {:data, data})
    :dets.close(db)
  end

  defp load_state do
    case :dets.open_file(db_file(), type: :set) do
      {:ok, db} ->
        data = case :dets.lookup(db, :data) do
          [{:data, state}] -> state
          [] -> %{}
        end
        :dets.close(db)
        data
      _ -> %{}
    end
  end
end
