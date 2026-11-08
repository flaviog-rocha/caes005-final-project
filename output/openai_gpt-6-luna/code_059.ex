# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorSystem.Macros do
  defmacro validation_boundary(do: body) do
    quote do
      case unquote(body) do
        :ok -> :ok
        {:error, reason} -> {:error, {:validation, reason}}
      end
    end
  end
end

defmodule SensorSystem.Store do
  use GenServer
  require SensorSystem.Macros

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  def init(state), do: {:ok, state}

  def handle_call({:register, id}, _from, state) do
    case validate_id(id) do
      :ok ->
        if Map.has_key?(state, id) do
          {:reply, {:error, {:validation, :duplicate_sensor}}, state}
        else
          record = %{id: id, value: nil, updated_at: nil}
          {:reply, {:ok, record}, Map.put(state, id, record)}
        end

      {:error, reason} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call({:update, id, attrs}, _from, state) do
    validation =
      SensorSystem.Macros.validation_boundary do
        validate_attrs(attrs)
      end

    case validation do
      :ok ->
        case Map.fetch(state, id) do
          {:ok, record} ->
            # O registro completo é substituído em uma única operação do GenServer.
            updated = %{
              record
              | value: Map.fetch!(attrs, :value),
                updated_at: System.system_time(:millisecond)
            }

            {:reply, {:ok, updated}, Map.put(state, id, updated)}

          :error ->
            {:reply, {:error, {:validation, :unknown_sensor}}, state}
        end

      {:error, {:validation, reason}} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_id), do: {:error, :invalid_sensor_id}

  defp validate_attrs(%{value: value} = attrs)
       when map_size(attrs) == 1 and is_number(value),
       do: :ok

  defp validate_attrs(%{value: _value}), do: {:error, :invalid_value}
  defp validate_attrs(_attrs), do: {:error, :expected_value}
end

defmodule SensorSystem.Monitor do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put_new(opts, :name, __MODULE__))
  end

  def init(_state), do: {:ok, %{sensors: %{}, failures: []}}

  def failures do
    GenServer.call(__MODULE__, :failures)
  end

  def handle_call(:failures, _from, state) do
    {:reply, Enum.reverse(state.failures), state}
  end

  def handle_info({:sensor_up, id, pid}, state) do
    ref = Process.monitor(pid)
    sensors = Map.put(state.sensors, ref, %{id: id, pid: pid})
    {:noreply, %{state | sensors: sensors}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.sensors, ref) do
      {nil, sensors} ->
        {:noreply, %{state | sensors: sensors}}

      {%{id: id}, sensors} ->
        failures =
          if unexpected?(reason) do
            [%{sensor_id: id, reason: reason, at: System.system_time(:millisecond)} |
             state.failures]
          else
            state.failures
          end

        {:noreply, %{state | sensors: sensors, failures: failures}}
    end
  end

  defp unexpected?(:normal), do: false
  defp unexpected?(:shutdown), do: false
  defp unexpected?({:shutdown, _reason}), do: false
  defp unexpected?(_reason), do: true
end

defmodule SensorSystem.Sensor do
  use GenServer

  def start_link(id), do: GenServer.start_link(__MODULE__, id)

  def init(id) do
    case Registry.register(SensorSystem.Registry, id, nil) do
      {:ok, _} ->
        case store_call({:register, id}) do
          {:ok, _record} ->
            send(SensorSystem.Monitor, {:sensor_up, id, self()})
            {:ok, %{id: id}}

          {:error, {:validation, reason}} ->
            {:stop, {:validation, reason}}

          {:error, {:operational, reason}} ->
            {:stop, {:operational, reason}}
        end

      {:error, {:already_registered, _pid}} ->
        {:stop, {:validation, :duplicate_sensor}}
    end
  end

  def handle_call({:update, attrs}, _from, state) do
    case store_call({:update, state.id, attrs}) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp store_call(message) do
    try do
      GenServer.call(SensorSystem.Store, message, 5_000)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end

defmodule SensorSystem.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: SensorSystem.Registry},
      SensorSystem.Store,
      SensorSystem.Monitor,
      {DynamicSupervisor, strategy: :one_for_one, name: SensorSystem.DynamicSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorSystem do
  def start_link(opts \\ []), do: SensorSystem.Supervisor.start_link(opts)

  def start_sensor(id) when is_binary(id) and byte_size(id) > 0 do
    child = %{
      id: {SensorSystem.Sensor, id},
      start: {SensorSystem.Sensor, :start_link, [id]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }

    try do
      case DynamicSupervisor.start_child(SensorSystem.DynamicSupervisor, child) do
        {:ok, pid} -> {:ok, pid}
        {:ok, pid, _info} -> {:ok, pid}
        {:error, reason} -> normalize_start_error(reason)
      end
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  def start_sensor(_id), do: {:error, {:validation, :invalid_sensor_id}}

  def update_sensor(id, attrs) do
    case Registry.lookup(SensorSystem.Registry, id) do
      [{pid, _value}] ->
        try do
          GenServer.call(pid, {:update, attrs}, 5_000)
        catch
          :exit, reason -> {:error, {:operational, reason}}
        end

      [] ->
        {:error, {:operational, :sensor_not_running}}
    end
  rescue
    error -> {:error, {:operational, error}}
  end

  def stop_sensor(id) do
    case Registry.lookup(SensorSystem.Registry, id) do
      [{pid, _value}] ->
        try do
          DynamicSupervisor.terminate_child(SensorSystem.DynamicSupervisor, pid)
        catch
          :exit, reason -> {:error, {:operational, reason}}
        end

      [] ->
        {:error, {:validation, :sensor_not_running}}
    end
  rescue
    error -> {:error, {:operational, error}}
  end

  def failures, do: SensorSystem.Monitor.failures()

  defp normalize_start_error({:validation, _reason} = error), do: {:error, error}
  defp normalize_start_error({:already_registered, _id}), do: {:error, {:validation, :duplicate_sensor}}
  defp normalize_start_error({:shutdown, {:validation, _reason} = error}), do: {:error, error}
  defp normalize_start_error(reason), do: {:error, {:operational, reason}}
end
