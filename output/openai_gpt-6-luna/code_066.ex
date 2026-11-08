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
  Monitora leituras de sensores em intervalos regulares.

  O leitor deve ser uma função de aridade zero que retorna uma lista de leituras.
  Cada leitura válida é um mapa com `:id` (string não vazia) e `:value` (número).
  A operação recebe uma leitura validada.
  """

  @spec run((() -> term()), (map() -> term()), pos_integer(), keyword()) ::
          %{history: list(), rejected: list()}
  def run(read_sensors, operation, interval_ms, opts \\ [])
      when is_function(read_sensors, 0) and is_function(operation, 1) and
             is_integer(interval_ms) and interval_ms > 0 do
    iterations = Keyword.get(opts, :iterations, :infinity)
    validate_iterations!(iterations)

    ticks =
      Stream.interval(interval_ms)
      |> limit_ticks(iterations)

    result =
      Enum.reduce(ticks, %{tick: 0, history: [], rejected: []}, fn _,
                                                                   %{tick: tick} = acc ->
        current_tick = tick + 1
        readings = read_sensors.()

        process_readings(readings, operation, current_tick, %{
          acc
          | tick: current_tick
        })
      end)

    %{
      history: Enum.reverse(result.history),
      rejected: Enum.reverse(result.rejected)
    }
  end

  def run(_read_sensors, _operation, _interval_ms, _opts) do
    raise ArgumentError,
          "esperados um leitor de aridade zero, uma operação de aridade um e um intervalo positivo"
  end

  defp limit_ticks(stream, :infinity), do: stream

  defp limit_ticks(stream, iterations) when is_integer(iterations) and iterations > 0 do
    Stream.take(stream, iterations)
  end

  defp validate_iterations!(:infinity), do: :ok

  defp validate_iterations!(iterations)
       when is_integer(iterations) and iterations > 0,
       do: :ok

  defp validate_iterations!(_iterations) do
    raise ArgumentError, "iterations deve ser um inteiro positivo ou :infinity"
  end

  defp process_readings(readings, operation, tick, acc) when is_list(readings) do
    Enum.reduce(readings, acc, fn reading, current ->
      case validate_reading(reading) do
        {:ok, valid_reading} ->
          result = operation.(valid_reading)

          entry = %{
            tick: tick,
            sensor_id: valid_reading.id,
            reading: valid_reading,
            result: result,
            executed_at: DateTime.utc_now()
          }

          %{current | history: [entry | current.history]}

        {:error, reason} ->
          rejection = %{tick: tick, reading: reading, reason: reason}
          %{current | rejected: [rejection | current.rejected]}
      end
    end)
  end

  defp process_readings(readings, _operation, tick, acc) do
    rejection = %{tick: tick, reading: readings, reason: :expected_a_list}
    %{acc | rejected: [rejection | acc.rejected]}
  end

  defp validate_reading(%{id: id, value: value} = reading)
       when is_binary(id) and byte_size(id) > 0 and is_number(value) do
    {:ok, reading}
  end

  defp validate_reading(_reading), do: {:error, :invalid_reading}
end
