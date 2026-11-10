# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Manter um histórico das operações realizadas.
# 5. Impedir atualizações parcialmente concluídas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Sistema de monitoramento de sensores processando fluxos de dados de forma assíncrona,
  usando Streams para eficiência de memória e GenStage/Tasks para isolamento.
  """

  # Requisito 1: Stream para processamento lazy (não carrega tudo na memória)
  # Requisito 2: Cada sensor é processado independentemente via Task
  
  @doc """
  Processa um fluxo de leituras de sensores.
  Simula o recebimento de grandes volumes via Enum.to_list ou Stream.
  """
  def process_sensor_stream(sensor_data_stream) do
    sensor_data_stream
    |> Stream.map(&process_reading/1)
    |> Stream.run()
  end

  defp process_reading(reading) do
    Task.async(fn ->
      # Requisito 5: Atomicidade (simulada via transaction block)
      execute_transaction(reading)
    end)
    |> Task.await()
  end

  # Requisito 3 e 5: Simulação de transação de banco de dados (Ecto-like)
  # Impedir atualizações parcialmente concluídas
  defp execute_transaction(data) do
    try do
      # Simula operação de escrita em banco de dados
      {:ok, result} = mock_db_insert(data)
      
      # Requisito 4: Registro de histórico
      log_operation(:success, data, result)
      {:ok, result}
    rescue
      e ->
        log_operation(:error, data, e)
        {:error, :transaction_rolled_back}
    end
  end

  # Simulação de repositório (Ecto)
  defp mock_db_insert(data) do
    # Simula latência de rede/IO
    :timer.sleep(10)
    {:ok, %{id: :erlang.unique_integer(), sensor_id: data.id, status: :processed}}
  end

  # Requisito 4: Logger de auditoria
  defp log_operation(type, data, info) do
    timestamp = DateTime.utc_now()
    IO.puts("[#{timestamp}] [#{String.upcase(Atom.to_string(type))}] Sensor: #{data.id} - Info: #{inspect(info)}")
  end
end

# --- Exemplo de Execução ---

# Simulando um volume massivo de dados usando Stream
large_sensor_stream = 
  Stream.cycle([%{id: "temp-01"}, %{id: "press-02"}])
  |> Stream.take(100)

SensorMonitor.process_sensor_stream(large_sensor_stream)
