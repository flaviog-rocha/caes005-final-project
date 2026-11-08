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
  Processa leituras de sensores em fluxo, com concorrência limitada.
  """

  @doc """
  Consome `readings` sem materializá-lo na memória.

  `:max_concurrency` limita o número de tarefas simultâneas.
  `:processor` pode ser uma função de aridade 1 que retorna `{:ok, resultado}`,
  `{:error, motivo}` ou um resultado direto.
  `:on_crash` é chamada com o motivo quando uma tarefa termina inesperadamente.
  """
  def monitor(readings, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    processor = Keyword.get(opts, :processor, &validate_reading/1)
    on_crash = Keyword.get(opts, :on_crash, fn _reason -> :ok end)
    timeout = Keyword.get(opts, :timeout, :infinity)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, ":max_concurrency deve ser um inteiro positivo"
    end

    unless is_function(processor, 1) do
      raise ArgumentError, ":processor deve ser uma função de aridade 1"
    end

    unless is_function(on_crash, 1) do
      raise ArgumentError, ":on_crash deve ser uma função de aridade 1"
    end

    readings
    |> Task.async_stream(
      fn reading -> invoke_processor(processor, reading) end,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce(%{processed: 0, failed: 0, crashed: 0}, fn
      {:ok, {:ok, _result}}, stats ->
        %{stats | processed: stats.processed + 1}

      {:ok, {:error, _reason}}, stats ->
        %{stats | failed: stats.failed + 1}

      {:exit, reason}, stats ->
        on_crash.(reason)

        %{
          stats
          | failed: stats.failed + 1,
            crashed: stats.crashed + 1
        }
    end)
  end

  defp invoke_processor(processor, reading) do
    case processor.(reading) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      result -> {:ok, result}
    end
  end

  defp validate_reading(
         %{sensor_id: sensor_id, value: value, timestamp: _timestamp} = reading
       )
       when not is_nil(sensor_id) and is_number(value) do
    {:ok, reading}
  end

  defp validate_reading(_reading) do
    {:error, :invalid_reading}
  end
end
