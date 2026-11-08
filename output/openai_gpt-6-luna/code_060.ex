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

defmodule SensorMonitor.Schema do
  @moduledoc false

  defmacro sensor_schema(ranges) do
    quote bind_quoted: [ranges: ranges] do
      @sensor_ranges ranges
      @sensor_fields Keyword.keys(ranges)

      def validate_changes(changes) when is_map(changes) do
        cond do
          map_size(changes) == 0 ->
            {:error, {:validation, :empty_update}}

          unknown = Map.keys(changes) -- @sensor_fields; unknown != [] ->
            {:error, {:validation, {:unknown_fields, unknown}}}

          true ->
            Enum.reduce_while(changes, :ok, fn {field, value}, :ok ->
              {minimum, maximum} = Keyword.fetch!(@sensor_ranges, field)

              if is_number(value) and value >= minimum and value <= maximum do
                {:cont, :ok}
              else
                {:halt,
                 {:error,
                  {:validation, {:out_of_range, field, minimum, maximum}}}}
              end
            end)
        end
      end

      def validate_changes(_changes), do: {:error, {:validation, :expected_map}}
    end
  end
end

defmodule SensorMonitor.Store do
  @moduledoc false
  use GenServer

  @name __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put(opts, :name, @name))
  end

  def ensure_sensor(id, initial) do
    GenServer.call(@name, {:ensure_sensor, id, initial})
  end

  def transaction_update(id, changes) do
    GenServer.call(@name, {:transaction_update, id, changes})
  end

  def fail_next(reason) do
    GenServer.call(@name, {:fail_next, reason})
  end

  @impl true
  def init(_state), do: {:ok, %{sensors: %{}, fail_next: nil}}

  @impl true
  def handle_call({:ensure_sensor, id, initial}, _from, state) do
    case Map.fetch(state.sensors, id) do
      {:ok, sensor} ->
        {:reply, {:ok, sensor}, state}

      :error ->
        sensors = Map.put(state.sensors, id, initial)
        {:reply, {:ok, initial}, %{state | sensors: sensors}}
    end
  end

  @impl true
  def handle_call({:transaction_update, _id, _changes}, _from, %{fail_next: reason} = state)
      when not is_nil(reason) do
    {:reply, {:error, {:operational, {:database_failure, reason}}}, %{state | fail_next: nil}}
  end

  def handle_call({:transaction_update, id, changes}, _from, state) do
    case Map.fetch(state.sensors, id) do
      {:ok, sensor} ->
        # Um único merge no GenServer torna a atualização indivisível para os leitores.
        updated = Map.merge(sensor, changes)
        sensors = Map.put(state.sensors, id, updated)
        {:reply, {:ok, updated}, %{state | sensors: sensors}}

      :error ->
        {:reply, {:error, {:operational, :sensor_not_found}}, state}
    end
  end

  @impl true
  def handle_call({:fail_next, reason}, _from, state) do
    {:reply, :ok, %{state | fail_next: reason}}
  end
end

defmodule SensorMonitor.Watcher do
  @moduledoc false
  use GenServer

  @name __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.put(opts, :name, @name))
  end

  def track(pid, sensor_id) do
    GenServer.call(@name, {:track, pid, sensor_id})
  end

  def events do
    GenServer.call(@name, :events)
  end

  @impl true
  def init(_state), do: {:ok, %{refs: %{}, events: []}}

  @impl true
  def handle_call({:track, pid, sensor_id}, _from, state) do
    if Enum.any?(state.refs, fn {_ref, tracked_pid, _id} -> tracked_pid == pid end) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | refs: Map.put(state.refs, ref, {pid, sensor_id})}}
    end
  end

  def handle_call(:events, _from, state) do
    {:reply, Enum.reverse(state.events), state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {{^pid, sensor_id}, refs} ->
        if unexpected?(reason) do
          event = %{
            sensor_id: sensor_id,
            pid: pid,
            reason: reason,
            detected_at: System.system_time(:millisecond)
          }

          {:noreply, %{state | refs: refs, events: [event | state.events]}}
        else
          {:noreply, %{state | refs: refs}}
        end

      {nil, refs} ->
        {:noreply, %{state | refs: refs}}
    end
  end

  defp unexpected?(:normal), do: false
  defp unexpected?(:shutdown), do: false
  defp unexpected?({:shutdown, _reason}), do: false
  defp unexpected?(_reason), do: true
end

defmodule SensorMonitor.Sensor do
  @moduledoc false
  use GenServer

  require SensorMonitor.Schema

  SensorMonitor.Schema.sensor_schema(
    temperature: {-80, 150},
    humidity: {0, 100},
    battery: {0, 100}
  )

  def start_link({id, initial}) do
    GenServer.start_link(__MODULE__, {id, initial},
      name: {:via, Registry, {SensorMonitor.Registry, id}}
    )
  end

  def child_spec({id, initial}) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [{id, initial}]},
      restart: :temporary,
      type: :worker
    }
  end

  def read(pid), do: GenServer.call(pid, :read)
  def update(pid, changes), do: GenServer.call(pid, {:update, changes})

  @impl true
  def init({id, initial}) do
    data = Map.put(initial, :id, id)

    case safe_store_call(fn -> SensorMonitor.Store.ensure_sensor(id, data) end) do
      {:ok, stored} -> {:ok, %{id: id, data: stored}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:read, _from, state), do: {:reply, {:ok, state.data}, state}

  def handle_call({:update, changes}, _from, state) do
    case validate_changes(changes) do
      :ok ->
        case safe_store_call(fn ->
               SensorMonitor.Store.transaction_update(state.id, changes)
             end) do
          {:ok, updated} ->
            {:reply, {:ok, updated}, %{state | data: updated}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp safe_store_call(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:operational, {:store_unavailable, reason}}}
  end
end

defmodule SensorMonitor.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      SensorMonitor.Store,
      {DynamicSupervisor, strategy: :one_for_one, name: SensorMonitor.SensorSupervisor},
      SensorMonitor.Watcher
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorMonitor do
  @moduledoc false

  def start_link(opts \\ []), do: SensorMonitor.Supervisor.start_link(opts)

  def start_sensor(id, initial \\ %{}) when is_map(initial) do
    with :ok <- SensorMonitor.Sensor.validate_changes(initial),
         false <- is_nil(id) do
      case DynamicSupervisor.start_child(
             SensorMonitor.SensorSupervisor,
             {SensorMonitor.Sensor, {id, initial}}
           ) do
        {:ok, pid} ->
          case safe_call(fn -> SensorMonitor.Watcher.track(pid, id) end) do
            :ok ->
              {:ok, pid}

            {:error, reason} ->
              DynamicSupervisor.terminate_child(SensorMonitor.SensorSupervisor, pid)
              {:error, {:operational, reason}}
          end

        {:error, {:already_started, _pid}} ->
          {:error, {:validation, :sensor_already_running}}

        {:error, reason} ->
          {:error, {:operational, {:sensor_start_failed, reason}}}
      end
    else
      {:error, reason} -> {:error, reason}
      true -> {:error, {:validation, :invalid_sensor_id}}
    end
  end

  def start_sensor(_id, _initial), do: {:error, {:validation, :expected_map}}

  def update_sensor(id, changes) do
    with {:ok, pid} <- lookup_sensor(id) do
      case safe_call(fn -> SensorMonitor.Sensor.update(pid, changes) end) do
        {:error, {:exit, reason}} ->
          {:error, {:operational, {:sensor_unavailable, reason}}}

        result ->
          result
      end
    end
  end

  def read_sensor(id) do
    with {:ok, pid} <- lookup_sensor(id) do
      case safe_call(fn -> SensorMonitor.Sensor.read(pid) end) do
        {:error, {:exit, reason}} ->
          {:error, {:operational, {:sensor_unavailable, reason}}}

        result ->
          result
      end
    end
  end

  def stop_sensor(id) do
    with {:ok, pid} <- lookup_sensor(id) do
      case DynamicSupervisor.terminate_child(SensorMonitor.SensorSupervisor, pid) do
        :ok -> :ok
        {:error, reason} -> {:error, {:operational, {:sensor_stop_failed, reason}}}
      end
    end
  end

  def unexpected_shutdowns, do: SensorMonitor.Watcher.events()

  def fail_next_store_operation(reason), do: SensorMonitor.Store.fail_next(reason)

  defp lookup_sensor(id) do
    case Registry.lookup(SensorMonitor.Registry, id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, {:operational, :sensor_not_found}}
    end
  end

  defp safe_call(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
