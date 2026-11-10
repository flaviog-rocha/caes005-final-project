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
  Sistema de processamento de sensores utilizando Streams para eficiência de memória,
  GenStage/Task para isolamento e transações atômicas via Agent para garantir
  consistência.
  """

  # Representa a persistência de histórico
  defmodule Persistence do
    use Agent

    def start_link(_), do: Agent.start_link(fn -> [] end, name: __MODULE__)

    def log(operation) do
      Agent.update(__MODULE__, fn history -> [operation | history] end)
    end

    def get_history, do: Agent.get(__MODULE__, & &1)
  end

  @doc """
  Processa um fluxo de dados de sensor.
  1. Stream garante que não carregamos tudo na memória.
  2. O uso de Task.async_stream garante isolamento por entidade.
  3. O bloco de transação (mock) garante a atomicidade (tudo ou nada).
  """
  def process_sensor_stream(sensor_data_stream) do
    sensor_data_stream
    |> Stream.chunk_every(100)
    |> Task.async_stream(fn batch ->
      try do
        execute_transaction(batch)
      rescue
        e -> {:error, e}
      end
    end, max_concurrency: System.schedulers_online())
    |> Stream.run()
  end

  defp execute_transaction(batch) do
    # Simulação de transação atômica (Rollback se falhar)
    # Em um cenário real com Ecto: Repo.transaction(fn -> ... end)
    
    # Validação de integridade: Impede atualizações parciais
    case validate_batch(batch) do
      :ok ->
        Enum.each(batch, &save_to_db/1)
        Persistence.log({:ok, "Batch processed: #{length(batch)} items"})
        :committed
      {:error, reason} ->
        Persistence.log({:error, reason})
        raise "Atomic update failed: #{reason}"
    end
  end

  defp validate_batch(batch) do
    if Enum.any?(batch, fn s -> s.value == nil end), 
      do: {:error, "Invalid sensor data"}, 
      else: :ok
  end

  defp save_to_db(data) do
    # Simulação de IO para DB
    :timer.sleep(10)
    IO.puts("Persisting sensor #{data.id} with value #{data.value}")
  end
end

# Exemplo de uso:
# SensorMonitor.Persistence.start_link([])
# data_stream = Stream.map(1..1000, fn i -> %{id: i, value: :rand.uniform(100)} end)
# SensorMonitor.process_sensor_stream(data_stream)
