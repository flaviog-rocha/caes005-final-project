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
  Processa eventos de sensores em fluxo, com limite de concorrência.

  O fluxo é preguiçoso: os eventos são consumidos conforme o chamador percorre
  o resultado, sem carregar a entrada inteira na memória. Falhas inesperadas
  nas tarefas são retornadas como `{:process_exit, motivo}`.
  """

  @doc """
  Cria um fluxo de resultados para os eventos de sensores.

  Opções:
    * `:max_concurrency` — quantidade máxima de tarefas simultâneas (padrão: 10).
    * `:timeout` — tempo limite por tarefa em milissegundos (padrão: 5_000).
    * `:database_writer` — função que recebe um evento validado e simula sua
      gravação em um banco de dados. Por padrão, retorna `:ok`.
  """
  def stream(events, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 10)
    timeout = Keyword.get(opts, :timeout, 5_000)
    database_writer = Keyword.get(opts, :database_writer, fn _event -> :ok end)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, ":max_concurrency deve ser um inteiro positivo"
    end

    unless is_integer(timeout) and timeout > 0 do
      raise ArgumentError, ":timeout deve ser um inteiro positivo"
    end

    unless is_function(database_writer, 1) do
      raise ArgumentError, ":database_writer deve ser uma função de aridade 1"
    end

    events
    |> Task.async_stream(
      fn event -> process_event(event, database_writer) end,
      max_concurrency: max_concurrency,
      timeout: timeout,
      ordered: false,
      on_timeout: :kill_task
    )
    |> Stream.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:process_exit, reason}
    end)
  end

  defp process_event(event, database_writer) do
    case validate_event(event) do
      :ok ->
        case database_writer.(event) do
          {:error, reason} -> {:write_error, event, reason}
          result -> {:stored, event, result}
        end

      {:error, reason} ->
        {:invalid_event, event, reason}
    end
  end

  defp validate_event(event) when is_map(event) do
    cond do
      not Map.has_key?(event, :sensor_id) or is_nil(event.sensor_id) ->
        {:error, :missing_sensor_id}

      not Map.has_key?(event, :value) ->
        {:error, :missing_value}

      not is_number(event.value) ->
        {:error, :value_must_be_numeric}

      true ->
        :ok
    end
  end

  defp validate_event(_event), do: {:error, :event_must_be_a_map}
end
