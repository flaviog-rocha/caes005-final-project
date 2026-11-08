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
  use GenServer

  @moduledoc """
  Processa leituras de um sensor por vez, consumindo enumeráveis de forma incremental.
  Cada entidade deve ter seu próprio processo.
  """

  @type reading :: %{required(:value) => number(), optional(:timestamp) => term()}
  @type stats :: %{
          count: non_neg_integer(),
          sum: number(),
          min: number() | nil,
          max: number() | nil,
          latest: map() | nil
        }

  def start_link(entity_id, opts \\ []) do
    GenServer.start_link(__MODULE__, entity_id, opts)
  end

  @doc """
  Consome `readings` sem materializá-lo. O enumerável é percorrido no processo da entidade.
  """
  def ingest(server, readings, timeout \\ :infinity) do
    GenServer.call(server, {:ingest, readings}, timeout)
  end

  def snapshot(server, timeout \\ 5_000) do
    GenServer.call(server, :snapshot, timeout)
  end

  @doc "Retorna o histórico em ordem cronológica."
  def history(server, timeout \\ 5_000) do
    GenServer.call(server, :history, timeout)
  end

  @impl true
  def init(entity_id) do
    {:ok,
     %{
       entity_id: entity_id,
       stats: empty_stats(),
       history: [],
       next_operation_id: 1
     }}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, %{entity_id: state.entity_id, stats: state.stats}, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:ingest, readings}, _from, state) do
    operation_id = state.next_operation_id
    started_at = DateTime.utc_now()

    case consume(readings, state.stats) do
      {:ok, stats, reading_count} ->
        entry = %{
          operation_id: operation_id,
          status: :committed,
          reading_count: reading_count,
          started_at: started_at,
          completed_at: DateTime.utc_now()
        }

        new_state = %{
          state
          | stats: stats,
            history: [entry | state.history],
            next_operation_id: operation_id + 1
        }

        {:reply, {:ok, entry}, new_state}

      {:error, reason} ->
        entry = %{
          operation_id: operation_id,
          status: :failed,
          reason: reason,
          started_at: started_at,
          completed_at: DateTime.utc_now()
        }

        new_state = %{
          state
          | history: [entry | state.history],
            next_operation_id: operation_id + 1
        }

        {:reply, {:error, reason}, new_state}
    end
  end

  defp consume(readings, initial_stats) do
    try do
      {stats, reading_count} =
        Enum.reduce(readings, {initial_stats, 0}, fn reading, {stats, index} ->
          {update_stats(stats, reading, index + 1), index + 1}
        end)

      {:ok, stats, reading_count}
    rescue
      error ->
        {:error, {:stream_failure, error}}
    catch
      :throw, {:invalid_reading, index, reason} ->
        {:error, {:invalid_reading, index, reason}}

      kind, reason ->
        {:error, {:stream_failure, kind, reason}}
    end
  end

  defp update_stats(stats, %{value: value} = reading, index) when is_number(value) do
    %{
      stats
      | count: stats.count + 1,
        sum: stats.sum + value,
        min: if(stats.min == nil, do: value, else: min(stats.min, value)),
        max: if(stats.max == nil, do: value, else: max(stats.max, value)),
        latest: %{value: value, timestamp: Map.get(reading, :timestamp)}
    }
  end

  defp update_stats(_stats, %{value: value}, index) do
    throw({:invalid_reading, index, {:value_must_be_numeric, value}})
  end

  defp update_stats(_stats, reading, index) do
    throw({:invalid_reading, index, {:expected_map_with_value, reading}})
  end

  defp empty_stats do
    %{count: 0, sum: 0, min: nil, max: nil, latest: nil}
  end
end
