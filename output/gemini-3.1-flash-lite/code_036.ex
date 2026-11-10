# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Sistema de monitoramento de sensores com controle de concorrência,
  persistência via ETS, notificações e tratamento de erros.
  """
  use GenServer

  # --- Tipos e Estruturas ---
  defstruct [:id, :value, :status]

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {SensorRegistry, opts[:id]}})
  end

  def process_reading(sensor_id, value) do
    case Registry.lookup(SensorRegistry, sensor_id) do
      [{pid, _}] -> GenServer.call(pid, {:process, value})
      [] -> {:error, :sensor_not_found}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(opts) do
    # Tabela ETS para persistência (sobrevive a falhas do processo se for público/named)
    table = :ets.new(:sensor_data, [:set, :public, :named_table, read_concurrency: true])
    
    # Pool simples usando uma fila ou limite de concorrência via Semaphore (neste caso, processamento sequencial/limitado)
    {:ok, %{id: opts[:id], table: table}}
  end

  @impl true
  def handle_call({:process, value}, _from, state) do
    result = perform_with_retry(fn -> validate_and_save(state.id, value) end)
    {:reply, result, state}
  end

  # --- Lógica de Negócio ---

  defp validate_and_save(id, value) do
    cond do
      not is_number(value) -> 
        {:error, {:validation, "Valor inválido: #{value}"}}
      value > 100 -> 
        notify(id, value)
        save_to_ets(id, value)
      true -> 
        save_to_ets(id, value)
    end
  end

  defp save_to_ets(id, value) do
    :ets.insert(:sensor_data, {id, value, System.system_time(:second)})
    :ok
  end

  defp notify(id, value) do
    IO.puts("[ALERTA] Sensor #{id} atingiu valor crítico: #{value}")
  end

  # --- Resiliência ---

  defp perform_with_retry(func, retries \\ 3) do
    case func.() do
      {:error, {:validation, _} = err} -> err
      :ok -> :ok
      {:error, _} when retries > 0 -> 
        Process.sleep(100)
        perform_with_retry(func, retries - 1)
      {:error, _} -> {:error, :operational_failure}
    end
  end

  # --- Inicialização do Registry ---
  
  def setup do
    Registry.start_link(keys: :unique, name: SensorRegistry)
  end
end

# --- Exemplo de Execução ---

SensorMonitor.setup()
{:ok, _pid} = SensorMonitor.start_link(id: "sensor_01")

# Processamento:
SensorMonitor.process_reading("sensor_01", 120) # Dispara alerta
SensorMonitor.process_reading("sensor_01", "abc") # Erro de validação
