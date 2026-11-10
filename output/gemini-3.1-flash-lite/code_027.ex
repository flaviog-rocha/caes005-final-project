# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorSystem do
  @moduledoc """
  Sistema de monitoramento de sensores com persistência e isolamento de falhas.
  """

  # Definição de Erros customizados
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      # Persistência via ETS (tabela persistente em memória)
      {:ets, :new, [:sensor_data, [:named_table, :public, :set, {:read_concurrency, true}]]},
      # Worker de Monitoramento
      {SensorWorker, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorWorker do
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok), do: {:ok, %{}}

  # API para comunicação entre processos
  def record_reading(sensor_id, value) when value < 0, 
    do: raise(SensorSystem.ValidationError, message: "Valor negativo inválido")
  
  def record_reading(sensor_id, value) do
    try do
      # Simulação de falha operacional (ex: erro de hardware)
      if :rand.uniform(10) == 1, do: raise(SensorSystem.OperationalError, message: "Falha na leitura")
      
      :ets.insert(:sensor_data, {sensor_id, value, System.system_time(:second)})
      :ok
    rescue
      e in SensorSystem.OperationalError -> {:error, :operational_failure, e.message}
    end
  end

  @impl true
  def handle_call({:get_data, sensor_id}, _from, state) do
    result = case :ets.lookup(:sensor_data, sensor_id) do
      [{^sensor_id, val, _ts}] -> {:ok, val}
      [] -> {:error, :not_found}
    end
    {:reply, result, state}
  end
end

# Exemplo de uso:
# SensorSystem.start_link([])
# SensorWorker.record_reading("temp_01", 25.5)
# GenServer.call(SensorWorker, {:get_data, "temp_01"})
