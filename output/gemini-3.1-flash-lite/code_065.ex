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
  Monitor de sensores utilizando Stream para processamento de dados e ciclo de vida.
  """

  defstruct history: []

  @doc """
  Inicia o monitoramento.
  - Valida: Apenas leituras positivas.
  - Periódico: Simulado por Stream.interval.
  - Histórico: Mantido no estado do processo.
  """
  def start_monitoring(sensor_source_fn) do
    # Stream.interval gera um evento a cada 1 segundo
    Stream.interval(1000)
    |> Stream.map(fn _ -> sensor_source_fn.() end)
    |> Stream.filter(&valid_reading?/1)
    |> Stream.scan(%__MODULE__{}, fn reading, acc ->
      new_entry = %{timestamp: System.system_time(:second), value: reading}
      %{acc | history: [new_entry | acc.history] |> Enum.take(10)}
    end)
    |> Enum.each(fn state ->
      IO.inspect(state.history, label: "Histórico Recente (últimas 10 leituras)")
    end)
  end

  # Requisito 1: Validação
  defp valid_reading?(value) when is_number(value) and value >= 0, do: true
  defp valid_reading?(_), do: false
end

# Exemplo de uso:
# SensorMonitor.start_monitoring(fn -> :rand.uniform(100) - 20 end)
