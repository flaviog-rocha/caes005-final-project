# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Sistema de monitoramento de sensores com suporte a estados, validação 
  e separação de erros operacionais vs validação.
  """

  use GenServer

  # --- Tipos e Estruturas ---

  defmodule SensorState do
    defstruct id: nil, value: 0.0, status: :ok, last_updated: nil
  end

  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalFailure, do: defexception [:message]

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def update_sensor(id, value) when not is_number(value) do
    raise ValidationError, message: "Valor do sensor deve ser numérico"
  end

  def update_sensor(id, value) do
    GenServer.call(__MODULE__, {:update, id, value})
  end

  def get_status(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_call({:update, id, value}, _from, state) do
    # Simulação de lógica de persistência (DB fictício)
    case persist_to_db(id, value) do
      :ok ->
        new_state = %SensorState{id: id, value: value, last_updated: DateTime.utc_now()}
        
        # Lógica de notificação baseada em limites
        if value > 100, do: trigger_notification(id, value)

        {:reply, :ok, Map.put(state, id, new_state)}

      {:error, reason} ->
        {:reply, {:error, %OperationalFailure{message: reason}}, state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state, id, :not_found), state}
  end

  # --- Funções Auxiliares / Privadas ---

  defp trigger_notification(id, value) do
    IO.puts("[ALERTA] Sensor #{id} atingiu valor crítico: #{value}")
  end

  # Simulação de interação com Banco de Dados (e.g., Ecto repository)
  defp persist_to_db(id, value) do
    # Simulação de falha aleatória (Operational Failure)
    if :rand.uniform(10) == 1 do
      {:error, "Conexão com banco de dados perdida"}
    else
      :ok
    end
  end
end

# --- Exemplo de Uso ---
# {:ok, _pid} = SensorMonitor.start_link([])
# SensorMonitor.update_sensor("temp_01", 120.5)
# IO.inspect SensorMonitor.get_status("temp_01")
