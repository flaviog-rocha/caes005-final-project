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
  @moduledoc false

  def start_link(opts \\ []) do
    SensorMonitor.Supervisor.start_link(opts)
  end

  def report(sensor_id, value, server \\ SensorMonitor.Server) do
    safe_call(server, {:report, sensor_id, value})
  end

  def readings(server \\ SensorMonitor.Server) do
    safe_call(server, :readings)
  end

  defp safe_call(server, message) do
    try do
      GenServer.call(server, message)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end

defmodule SensorMonitor.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :supervisor_name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    server_name = Keyword.get(opts, :server_name, SensorMonitor.Server)
    store_path = Keyword.get(opts, :store_path, "sensor_monitor.term")

    children = [
      {SensorMonitor.Server, name: server_name, store_path: store_path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorMonitor.Server do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    store_path = Keyword.fetch!(opts, :store_path)

    case load_readings(store_path) do
      {:ok, readings} ->
        {:ok, %{store_path: store_path, readings: readings}}

      {:error, reason} ->
        {:stop, {:operational, reason}}
    end
  end

  @impl true
  def handle_call({:report, sensor_id, value}, _from, state) do
    case validate_reading(sensor_id, value) do
      :ok ->
        reading = %{
          value: value,
          recorded_at: System.system_time(:millisecond)
        }

        updated_readings = Map.put(state.readings, sensor_id, reading)

        case persist(state.store_path, updated_readings) do
          :ok ->
            {:reply, :ok, %{state | readings: updated_readings}}

          {:error, reason} ->
            {:reply, {:error, {:operational, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call(:readings, _from, state) do
    {:reply, {:ok, state.readings}, state}
  end

  defp validate_reading(sensor_id, value) do
    cond do
      not is_binary(sensor_id) or sensor_id == "" ->
        {:error, :invalid_sensor_id}

      not is_number(value) ->
        {:error, :invalid_sensor_value}

      true ->
        :ok
    end
  end

  defp load_readings(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          readings = :erlang.binary_to_term(binary, [:safe])

          if valid_readings?(readings) do
            {:ok, readings}
          else
            {:error, :invalid_persisted_data}
          end
        rescue
          ArgumentError -> {:error, :invalid_persisted_data}
        catch
          _, _ -> {:error, :invalid_persisted_data}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, {:read_failed, reason}}
    end
  end

  defp valid_readings?(readings) when is_map(readings) do
    Enum.all?(readings, fn
      {sensor_id, %{value: value, recorded_at: recorded_at}} ->
        is_binary(sensor_id) and sensor_id != "" and
          is_number(value) and is_integer(recorded_at)

      _ ->
        false
    end)
  end

  defp valid_readings?(_), do: false

  defp persist(path, readings) do
    temp_path = path <> ".tmp"
    binary = :erlang.term_to_binary(readings)

    case File.write(temp_path, binary, [:binary, :sync]) do
      :ok ->
        case File.rename(temp_path, path) do
          :ok ->
            :ok

          {:error, reason} ->
            File.rm(temp_path)
            {:error, {:rename_failed, reason}}
        end

      {:error, reason} ->
        File.rm(temp_path)
        {:error, {:write_failed, reason}}
    end
  end
end
