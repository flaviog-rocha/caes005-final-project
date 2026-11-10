# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de reservas de salas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MeetingRoomSystem do
  @moduledoc """
  Sistema de gerenciamento de reservas utilizando GenServer para concorrência
  e Streams para processamento lazy de grandes volumes de dados.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def reserve(room_id, time_slot) do
    GenServer.call(__MODULE__, {:reserve, room_id, time_slot})
  end

  @doc """
  Processa um grande volume de reservas (ex: um arquivo ou stream de dados)
  sem carregar tudo na memória, usando Lazy Evaluation.
  """
  def process_bulk_reservations(stream) do
    stream
    |> Stream.map(fn {room_id, time_slot} -> reserve(room_id, time_slot) end)
    |> Stream.run()
  end

  # --- Callbacks do GenServer (Garantia de Consistência) ---

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_call({:reserve, room_id, time_slot}, _from, state) do
    # O GenServer processa mensagens sequencialmente, garantindo consistência
    # O estado local atua como nossa "Single Source of Truth"
    case Map.get(state, {room_id, time_slot}) do
      nil ->
        # Simulação de escrita em DB (ex: Ecto.Repo.insert)
        new_state = Map.put(state, {room_id, time_slot}, :reserved)
        {:reply, {:ok, :reserved}, new_state}

      :reserved ->
        {:reply, {:error, :already_reserved}, state}
    end
  end

  # --- Exemplo de Persistência com Ecto (Conceitual) ---
  # Em um cenário real, usariamos Ecto.Multi para transações atômicas:
  # Ecto.Multi.new()
  # |> Ecto.Multi.insert(:reservation, %Reservation{...})
  # |> Repo.transaction()
end

# --- Exemplo de Uso ---

# 1. Iniciar o sistema
{:ok, _pid} = MeetingRoomSystem.start_link([])

# 2. Processamento de grandes volumes (Streaming)
# Imagine que 'large_data_source' seja um Stream vindo de um arquivo CSV ou Socket
large_data_source = Stream.cycle([{"sala_1", "10:00"}]) |> Stream.take(5)

MeetingRoomSystem.process_bulk_reservations(large_data_source)

# 3. Teste de Concorrência
# O uso de Task.async garante que o processamento seja paralelo,
# mas o GenServer serializa as chamadas, mantendo a consistência.
1..10
|> Enum.map(fn _ -> Task.async(fn -> MeetingRoomSystem.reserve("sala_2", "14:00") end) end)
|> Enum.map(&Task.await/1)
|> IO.inspect(label: "Resultado da concorrência")
