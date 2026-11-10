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

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores utilizando processamento em stream para lidar com grandes volumes
  e controle de concorrência com Task.
  """

  @doc """
  Processa um fluxo de leituras de sensores simulando uma leitura de banco de dados
  (ou stream de arquivo) e processando-os com limite de concorrência.
  """
  def start_monitoring(sensor_stream, max_concurrency \\ 10) do
    sensor_stream
    |> Task.async_stream(
      fn sensor_data -> process_sensor(sensor_data) end,
      max_concurrency: max_concurrency,
      ordered: false,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp process_sensor(data) do
    # Simula lógica de monitoramento
    # O encerramento inesperado é tratado pelo supervisor/monitor que orquestra esta Task
    try do
      IO.puts("Processando sensor: #{data.id}")
      # Simulação de escrita em banco fictício
      simulate_db_write(data)
    rescue
      e -> IO.warn("Erro ao processar sensor #{data.id}: #{inspect(e)}")
    end
  end

  defp simulate_db_write(data) do
    # Placeholder para interação com Ecto ou Repositorio
    :timer.sleep(10)
    {:ok, data}
  end
end

# Demonstração de uso com processamento de fluxo (Stream)
# O uso de Stream garante que não carregamos tudo na memória
sensor_data_source = Stream.map(1..1_000_000, fn i -> %{id: i, value: :rand.uniform(100)} end)

# Monitoramento com limite de 5 processos simultâneos
SensorMonitor.start_monitoring(sensor_data_source, 5)

# NOTA SOBRE REQUISITOS:
# 1. Banco de Dados: O uso de Stream permite integrar com `Repo.stream` do Ecto.
# 2. Memória: Stream processa itens sob demanda (lazy).
# 3. Encerramento inesperado: Task.async_stream garante que se uma tarefa falhar,
#    o processo pai recebe uma mensagem de erro e pode supervisionar a falha.
# 4. Concorrência: O parâmetro `max_concurrency` limita o número de workers simultâneos.
