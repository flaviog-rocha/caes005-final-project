# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  def start_link(path, supervisor_options \\ []) do
    SensorMonitor.Supervisor.start_link(path, supervisor_options)
  end

  def record(sensor_id, value, server \\ SensorMonitor.Store) do
    with :ok <- validate_sensor_id(sensor_id),
         :ok <- validate_value(value) do
      safe_call(server, {:record, sensor_id, value})
    end
  end

  def readings(server \\ SensorMonitor.Store) do
    safe_call(server, :readings)
  end

  def sensor_readings(sensor_id, server \\ SensorMonitor.Store) do
    with :ok <- validate_sensor_id(sensor_id) do
      safe_call(server, {:sensor_readings, sensor_id})
    end
  end

  defp validate_sensor_id(sensor_id)
       when is_binary(sensor_id) and byte_size(sensor_id) > 0,
       do: :ok

  defp validate_sensor_id(_), do: {:error, {:validation, :invalid_sensor_id}}

  defp validate_value(value) when is_number(value), do: :ok
  defp validate_value(_), do: {:error, {:validation, :invalid_value}}

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end

defmodule SensorMonitor.Store do
  use GenServer

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  @impl true
  def init(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, readings} <- load(path) do
      {:ok, %{path: path, readings: readings}}
    else
      {:error, reason} -> {:stop, {:storage_error, reason}}
    end
  end

  @impl true
  def handle_call({:record, sensor_id, value}, _from, state) do
    reading = {:reading, sensor_id, value, System.system_time(:millisecond)}

    case persist(state.path, reading) do
      :ok ->
        {:reply, {:ok, reading}, %{state | readings: [reading | state.readings]}}

      {:error, reason} ->
        {:reply, {:error, {:operational, reason}}, state}
    end
  end

  def handle_call(:readings, _from, state) do
    {:reply, {:ok, Enum.reverse(state.readings)}, state}
  end

  def handle_call({:sensor_readings, sensor_id}, _from, state) do
    readings =
      state.readings
      |> Enum.filter(fn {:reading, id, _, _} -> id == sensor_id end)
      |> Enum.reverse()

    {:reply, {:ok, readings}, state}
  end

  defp load(path) do
    case File.read(path) do
      {:ok, contents} -> decode(contents)
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode(contents), do: decode_records(contents, [])

  defp decode_records(<<length::unsigned-big-32, rest::binary>>, acc)
       when byte_size(rest) >= length do
    <<payload::binary-size(length), tail::binary>> = rest

    try do
      case :erlang.binary_to_term(payload, [:safe]) do
        {:reading, sensor_id, value, timestamp} = reading
        when is_binary(sensor_id) and is_number(value) and is_integer(timestamp) ->
          decode_records(tail, [reading | acc])

        _ ->
          {:error, :invalid_persisted_record}
      end
    rescue
      ArgumentError -> {:error, :invalid_persisted_record}
    end
  end

  # Ignore an incomplete trailing frame, which can result from an interrupted write.
  defp decode_records(_incomplete_frame, acc), do: {:ok, acc}

  defp persist(path, reading) do
    payload = :erlang.term_to_binary(reading)
    frame = <<byte_size(payload)::unsigned-big-32, payload::binary>>

    case File.open(path, [:append, :binary]) do
      {:ok, file} ->
        write_result =
          try do
            with :ok <- IO.binwrite(file, frame),
                 :ok <- :file.sync(file) do
              :ok
            end
          rescue
            error -> {:error, error}
          end

        close_result = File.close(file)

        case {write_result, close_result} do
          {:ok, :ok} -> :ok
          {{:error, reason}, _} -> {:error, reason}
          {:ok, {:error, reason}} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end

defmodule SensorMonitor.Supervisor do
  use Supervisor

  def start_link(path, supervisor_options \\ []) do
    Supervisor.start_link(__MODULE__, path, supervisor_options)
  end

  @impl true
  def init(path) do
    children = [
      {SensorMonitor.Store, path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
