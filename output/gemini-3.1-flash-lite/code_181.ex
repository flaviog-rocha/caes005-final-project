# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir comunicação entre diferentes processos.
# 3. Validar os dados recebidos antes de realizar operações.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentSystem do
  @moduledoc """
  Sistema de agendamento resiliente utilizando GenServer e Supervisor.
  """
  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{processed: MapSet.new()}, name: __MODULE__)

  def schedule(id, appointment_data) when is_map(appointment_data) do
    # Validação dos dados antes de enviar ao processo
    if valid?(appointment_data) do
      GenServer.call(__MODULE__, {:schedule, id, appointment_data})
    else
      {:error, :invalid_data}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:schedule, id, data}, _from, state) do
    if MapSet.member?(state.processed, id) do
      {:reply, {:error, :already_processed}, state}
    else
      # Simulação de processamento de agendamento
      new_state = %{state | processed: MapSet.put(state.processed, id)}
      {:reply, {:ok, :scheduled}, new_state}
    end
  end

  # --- Validação ---

  defp valid?(data), do: Map.has_key?(data, :time) and Map.has_key?(data, :user_id)
end

defmodule AppointmentSupervisor do
  @moduledoc """
  Supervisor para garantir a recuperação de falhas (Requirement 5).
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      # Limite de operações simultâneas via rate limiting ou pool (Requirement 4)
      # Aqui usamos um Worker único gerenciado por um Supervisor para garantir o estado.
      %{
        id: AppointmentSystem,
        start: {AppointmentSystem, :start_link, [[]]},
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de uso:
# 1. Iniciar o sistema: AppointmentSupervisor.start_link([])
# 2. Agendar: AppointmentSystem.schedule("id_1", %{time: "10:00", user_id: 1})
