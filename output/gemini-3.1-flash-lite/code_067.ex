# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Detectar encerramentos inesperados de processos.
# 4. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Módulo responsável pelo processamento de fluxos de dados de sensores
# Utiliza Stream para lazy evaluation e Task.Supervisor para gestão de concorrência.

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores com processamento de streams, controle de concorrência
  e monitoramento de falhas.
  """

  @doc """
  Processa leituras de sensores em lotes, garantindo que o volume total não
  sobrecarregue a memória e limitando a concorrência via Task.async_stream.
  """
  def start_monitoring(sensor_stream, max_concurrency \\ 10) do
    # Inicia um supervisor para gerenciar as tarefas (detectando encerramentos)
    {:ok, supervisor} = Task.Supervisor.start_link()

    sensor_stream
    |> Stream.chunk_every(100) # Processamento em lotes para eficiência
    |> Task.Supervisor.async_stream(
      supervisor,
      fn batch -> process_batch(batch) end,
      max_concurrency: max_concurrency,
      ordered: false,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  # Simula a persistência em banco de dados ou integração com Ecto
  # O uso de Stream garante que o banco não receba um dump de memória gigante
  defp process_batch(batch) do
    Enum.each(batch, fn reading ->
      # Aqui poderia haver uma chamada: Repo.insert!(%SensorReading{...})
      IO.puts("Processando leitura: #{inspect(reading)}")
    end)
  rescue
    e -> IO.warn("Erro ao processar lote: #{inspect(e)}")
  end

  @doc """
  Exemplo de gerador de stream infinito para simular grandes volumes
  sem carregar tudo na memória.
  """
  def sensor_data_stream do
    Stream.repeatedly(fn -> 
      %{id: :rand.uniform(1000), value: :rand.uniform(100), ts: System.system_time()} 
    end)
  end
end

# Exemplo de uso:
# SensorMonitor.start_monitoring(SensorMonitor.sensor_data_stream(), 5)

# Notas sobre os requisitos:
# 1. Banco de Dados: A função process_batch é o ponto de integração. 
#    Para Ecto, basta substituir o IO.puts por uma chamada de transação.
# 2. Grandes volumes: Stream.chunk_every/Stream.run garante que apenas 
#    pedaços do fluxo existam na memória.
# 3. Encerramentos: Task.Supervisor.async_stream monitora os processos filhos;
#    se uma tarefa falhar, o supervisor garante a limpeza.
# 4. Concorrência: O parâmetro max_concurrency controla exatamente 
#    quantas tarefas rodam simultaneamente.
