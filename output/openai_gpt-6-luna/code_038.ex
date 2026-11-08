# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  def start_link(path) when is_binary(path) do
    GenServer.start_link(__MODULE__, path)
  end

  def start_link(path, options) when is_binary(path) do
    GenServer.start_link(__MODULE__, path, options)
  end

  def process(server, operation_id, readings) do
    GenServer.call(server, {:process, operation_id, readings}, :infinity)
  end

  def history(server) do
    path = GenServer.call(server, :history_path)

    path
    |> File.stream!([], :line)
    |> Stream.flat_map(fn line ->
      case decode_record(line) do
        {:ok, record} -> [record]
        :error -> []
      end
    end)
  end

  @impl true
  def init(path) do
    File.mkdir_p!(Path.dirname(path))
    File.touch!(path)

    seen =
      path
      |> File.stream!([], :line)
      |> Enum.reduce(MapSet.new(), fn line, acc ->
        case decode_record(line) do
          {:ok, %{operation_id: operation_id}} -> MapSet.put(acc, operation_id)
          _ -> acc
        end
      end)

    {:ok, %{path: path, seen: seen}}
  end

  @impl true
  def handle_call(:history_path, _from, state) do
    {:reply, state.path, state}
  end

  def handle_call({:process, operation_id, _readings}, _from, %{seen: seen} = state)
      when is_map_key(seen.map, operation_id) do
    {:reply, {:already_processed, operation_id}, state}
  end

  def handle_call({:process, operation_id, readings}, _from, state) do
    if MapSet.member?(state.seen, operation_id) do
      {:reply, {:already_processed, operation_id}, state}
    else
      case summarize(readings) do
        {:ok, summary} ->
          record = %{
            operation_id: operation_id,
            completed_at: DateTime.utc_now(),
            summary: summary
          }

          case append_record(state.path, record) do
            :ok ->
              {:reply, {:ok, summary}, %{state | seen: MapSet.put(state.seen, operation_id)}}

            {:error, reason} ->
              {:reply, {:error, {:history_write_failed, reason}}, state}
          end

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  defp summarize(readings) do
    initial = %{count: 0, sum: 0, min: nil, max: nil, sensors: %{}}

    try do
      Enum.reduce_while(readings, {:ok, initial}, fn reading, {:ok, totals} ->
        case reading_data(reading) do
          {:ok, sensor_id, value} ->
            sensor_stats = Map.get(totals.sensors, sensor_id, empty_stats())
            updated_sensor_stats = add_value(sensor_stats, value)

            updated_totals =
              totals
              |> add_value(value)
              |> Map.put(:sensors, Map.put(totals.sensors, sensor_id, updated_sensor_stats))

            {:cont, {:ok, updated_totals}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)
    rescue
      exception ->
        {:error, {:processing_failed, Exception.message(exception)}}
    catch
      kind, reason ->
        {:error, {:processing_failed, {kind, reason}}}
    end
  end

  defp reading_data(reading) when is_map(reading) do
    with {:ok, sensor_id} <- Map.fetch(reading, :sensor_id),
         {:ok, value} <- Map.fetch(reading, :value),
         true <- is_number(value) do
      {:ok, sensor_id, value}
    else
      :error -> {:error, :reading_missing_sensor_id_or_value}
      false -> {:error, :reading_value_must_be_numeric}
    end
  end

  defp reading_data(_), do: {:error, :reading_must_be_a_map}

  defp empty_stats, do: %{count: 0, sum: 0, min: nil, max: nil}

  defp add_value(stats, value) do
    %{
      count: stats.count + 1,
      sum: stats.sum + value,
      min: if(is_nil(stats.min), do: value, else: min(stats.min, value)),
      max: if(is_nil(stats.max), do: value, else: max(stats.max, value))
    }
  end

  defp append_record(path, record) do
    line =
      record
      |> :erlang.term_to_binary()
      |> Base.encode64()
      |> Kernel.<>("\n")

    File.write(path, line, [:append, :binary])
  end

  defp decode_record(line) do
    with {:ok, binary} <- Base.decode64(String.trim(line)) do
      {:ok, :erlang.binary_to_term(binary)}
    else
      _ -> :error
    end
  rescue
    _ -> :error
  end
end
