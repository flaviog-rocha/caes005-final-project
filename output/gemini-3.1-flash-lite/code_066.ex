# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores utilizando Stream para processamento de dados,
  validação, execução periódica e manutenção de histórico.
  """

  defstruct [:id, :value, :timestamp]

  @doc """
  Inicia o monitoramento.
  Gera um stream infinito de leituras simuladas, valida, processa e armazena.
  """
  def start_monitoring(interval_ms \\ 1000) do
    # 1. Stream infinito de dados simulados
    Stream.repeatedly(fn -> 
      Process.sleep(interval_ms)
      %{id: "sensor_#{:rand.uniform(3)}", value: :rand.uniform(100), timestamp: System.system_time(:second)}
    end)
    # 1. Validação: Filtra leituras inválidas (ex: valores negativos)
    |> Stream.filter(fn data -> data.value >= 0 end)
    # 2. Execução periódica: Mapeamento para operações
    |> Stream.map(&process_reading/1)
    # 3. Histórico: Acumula as operações em um estado via agente
    |> Stream.each(&save_to_history/1)
    |> Stream.run()
  end

  # Simulação de validação e processamento
  defp process_reading(data) do
    IO.puts("Processando #{data.id} com valor #{data.value}...")
    struct(__MODULE__, data)
  end

  # Agente para manter o histórico em memória
  defp save_to_history(reading) do
    Agent.update(__MODULE__, fn state -> [reading | state] end)
    IO.inspect(get_history(), label: "Histórico Atualizado")
  end

  def start_link do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def get_history do
    Agent.get(__MODULE__, & &1)
  end
end

# Exemplo de execução:
# SensorMonitor.start_link()
# SensorMonitor.start_monitoring()
