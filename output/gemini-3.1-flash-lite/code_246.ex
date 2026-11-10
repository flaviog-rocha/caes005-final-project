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
  Sistema de monitoramento de sensores usando GenServer para estado
  e um design orientado a processos para isolamento de falhas.
  """

  use GenServer

  # --- Tipos e Definições ---

  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  @type sensor_id :: String.t()
  @type reading :: %{value: float(), threshold: float()}

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Registra uma leitura. Pode lançar ValidationError ou OperationalError."
  def process_reading(sensor_id, value, threshold) when value < 0,
    do: raise(ValidationError, message: "Valor do sensor não pode ser negativo")

  def process_reading(sensor_id, value, threshold) do
    GenServer.call(__MODULE__, {:process, sensor_id, value, threshold})
  end

  def get_status, do: GenServer.call(__MODULE__, :get_all)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_call({:process, id, value, threshold}, _from, state) do
    try do
      # Simulação de escrita em banco (ex: Ecto.Repo.insert/1)
      db_persist(id, value)

      if value > threshold do
        notify_alert(id, value)
      end

      new_state = Map.put(state, id, %{value: value, updated_at: DateTime.utc_now()})
      {:reply, :ok, new_state}
    rescue
      e in RuntimeError -> {:reply, {:error, %OperationalError{message: e.message}}, state}
    end
  end

  @impl true
  def handle_call(:get_all, _from, state), do: {:reply, state, state}

  # --- Funções Auxiliares (Domínio) ---

  defp notify_alert(id, value) do
    IO.puts("[ALERTA] Sensor #{id} atingiu valor crítico: #{value}")
  end

  defp db_persist(id, value) do
    # Simula falha operacional aleatória (ex: queda na conexão do banco)
    if :rand.uniform(10) == 1,
      do: raise(RuntimeError, message: "Falha ao gravar no banco de dados")
    
    IO.puts("[DB] Persistindo #{id}: #{value}")
  end
end

# --- Exemplo de Uso ---

# SensorMonitor.start_link([])
# SensorMonitor.process_reading("temp-01", 95.0, 90.0)
# SensorMonitor.get_status()
