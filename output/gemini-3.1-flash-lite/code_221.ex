# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para garantir consistência em concorrência, utilizamos GenServer para serializar o acesso
# ao estado da sala. Para grandes volumes, utilizamos Streams para lazy evaluation.

defmodule RoomBookingSystem do
  use GenServer

  # --- API Pública ---

  def start_link(room_id) do
    GenServer.start_link(__MODULE__, %{room_id: room_id, bookings: MapSet.new()}, name: via_tuple(room_id))
  end

  def book(room_id, time_slot) do
    GenServer.call(via_tuple(room_id), {:book, time_slot})
  end

  # Processamento de grandes volumes via Stream (Lazy)
  # Simula a leitura de um arquivo ou stream de dados sem carregar tudo na RAM
  def process_batch(booking_requests) do
    booking_requests
    |> Stream.map(fn {room_id, slot} -> book(room_id, slot) end)
    |> Stream.run()
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:book, time_slot}, _from, state) do
    if MapSet.member?(state.bookings, time_slot) do
      {:reply, {:error, :already_booked}, state}
    else
      # Simulação de persistência (Banco de dados fictício)
      persist_to_db(state.room_id, time_slot)
      
      new_state = %{state | bookings: MapSet.put(state.bookings, time_slot)}
      {:reply, :ok, new_state}
    end
  end

  # --- Helpers ---

  defp via_tuple(room_id), do: {:via, Registry, {RoomRegistry, room_id}}

  defp persist_to_db(room_id, slot) do
    # Placeholder para Ecto: Repo.insert!(%Booking{room_id: room_id, slot: slot})
    IO.puts("Persistindo reserva: Sala #{room_id} em #{inspect(slot)}")
  end
end

# --- Setup de Infraestrutura ---

# Inicializa o Registry para gerenciar os processos das salas dinamicamente
# Isso é essencial para escalar o sistema sem um único gargalo global.
{:ok, _} = Registry.start_link(keys: :unique, name: RoomRegistry)

# --- Exemplo de Uso ---

# 1. Iniciar gerenciador de uma sala
RoomBookingSystem.start_link("sala-01")

# 2. Processar grande volume de reservas (Lazy)
# O Stream garante que cada item seja processado um a um, economizando memória
requests = [
  {"sala-01", ~U[2023-10-01T10:00:00Z]},
  {"sala-01", ~U[2023-10-01T11:00:00Z]}
]

RoomBookingSystem.process_batch(requests)
