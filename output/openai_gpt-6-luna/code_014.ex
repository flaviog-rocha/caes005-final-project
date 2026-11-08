# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use Supervisor

  @default_ttl 60_000
  @default_timeout 5_000

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    ttl = Keyword.get(opts, :ttl, @default_ttl)

    unless is_integer(ttl) and ttl > 0 do
      raise ArgumentError, "ttl deve ser um inteiro positivo"
    end

    children = [
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: SensorMonitor.SensorSupervisor},
      {Task.Supervisor, name: SensorMonitor.TaskSupervisor},
      {SensorMonitor.TTL, ttl: ttl}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def measure(sensor_id, operation, timeout \\ @default_timeout) do
    with :ok <- validate_sensor_id(sensor_id),
         :ok <- validate_operation(operation),
         :ok <- validate_timeout(timeout),
         {:ok, pid} <- ensure_sensor(sensor_id) do
      result = run_operation(sensor_id, operation, timeout)

      case append_result(pid, result) do
        :ok -> result
        {:error, reason} -> {:error, {:operational, reason}}
      end
    end
  end

  def history(sensor_id) do
    with :ok <- validate_sensor_id(sensor_id),
         {:ok, pid} <- ensure_sensor(sensor_id) do
      try do
        {:ok, GenServer.call(pid, :history)}
      catch
        :exit, reason -> {:error, {:operational, reason}}
      end
    end
  end

  defp validate_sensor_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_sensor_id(_), do: {:error, {:validation, :invalid_sensor_id}}

  defp validate_operation(fun) when is_function(fun, 0) or is_function(fun, 1), do: :ok
  defp validate_operation(_), do: {:error, {:validation, :invalid_operation}}

  defp validate_timeout(timeout) when is_integer(timeout) and timeout > 0, do: :ok
  defp validate_timeout(_), do: {:error, {:validation, :invalid_timeout}}

  defp ensure_sensor(sensor_id) do
    case Registry.lookup(SensorMonitor.Registry, sensor_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        ttl = SensorMonitor.TTL.get()

        case DynamicSupervisor.start_child(
               SensorMonitor.SensorSupervisor,
               {SensorMonitor.Sensor, {sensor_id, ttl}}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, reason} ->
            case Registry.lookup(SensorMonitor.Registry, sensor_id) do
              [{pid, _}] -> {:ok, pid}
              [] -> {:error, {:operational, reason}}
            end
        end
    end
  catch
    :exit, reason -> {:error, {:operational, reason}}
  end

  defp run_operation(sensor_id, operation, timeout) do
    task =
      Task.Supervisor.async_nolink(SensorMonitor.TaskSupervisor, fn ->
        evaluate_operation(sensor_id, operation)
      end)

    try do
      Task.await(task, timeout)
    catch
      :exit, reason -> {:error, {:operational, {:task_exit, reason}}}
    end
  rescue
    exception ->
      {:error, {:operational, {:task_start_failure, Exception.message(exception)}}}
  catch
    kind, reason -> {:error, {:operational, {:task_start_failure, kind, reason}}}
  end

  defp evaluate_operation(sensor_id, operation) do
    value =
      case :erlang.fun_info(operation, :arity) do
        {:arity, 0} -> operation.()
        {:arity, 1} -> operation.(sensor_id)
      end

    case value do
      {:ok, reading} when is_number(reading) ->
        {:ok, reading}

      reading when is_number(reading) ->
        {:ok, reading}

      {:error, reason} ->
        {:error, {:operational, reason}}

      other ->
        {:error, {:operational, {:invalid_result, other}}}
    end
  rescue
    exception ->
      {:error, {:operational, {:exception, exception.__struct__, Exception.message(exception)}}}
  catch
    kind, reason ->
      {:error, {:operational, {:caught, kind, reason}}}
  end

  defp append_result(pid, result) do
    try do
      GenServer.call(pid, {:append, result})
    catch
      :exit, reason -> {:error, reason}
    end
  end
end

defmodule SensorMonitor.TTL do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :ttl), name: __MODULE__)
  end

  def get do
    GenServer.call(__MODULE__, :get)
  end

  @impl true
  def init(ttl), do: {:ok, ttl}

  @impl true
  def handle_call(:get, _from, ttl), do: {:reply, ttl, ttl}
end

defmodule SensorMonitor.Sensor do
  use GenServer

  def start_link({sensor_id, ttl}) do
    GenServer.start_link(__MODULE__, {sensor_id, ttl},
      name: {:via, Registry, {SensorMonitor.Registry, sensor_id}}
    )
  end

  @impl true
  def init({_sensor_id, ttl}) do
    {:ok, %{ttl: ttl, history: [], next_sequence: 1, timer: nil}}
  end

  @impl true
  def handle_call({:append, result}, _from, state) do
    now = System.monotonic_time(:millisecond)

    entry = %{
      sequence: state.next_sequence,
      timestamp: DateTime.utc_now(),
      result: result,
      expires_at: now + state.ttl
    }

    state = %{
      state
      | history: [entry | state.history],
        next_sequence: state.next_sequence + 1
    }

    state =
      if state.timer do
        state
      else
        schedule_expiration(state, now)
      end

    {:reply, :ok, state}
  end

  def handle_call(:history, _from, state) do
    state = remove_expired(state, System.monotonic_time(:millisecond))

    entries =
      Enum.map(state.history, fn entry ->
        Map.drop(entry, [:expires_at])
      end)

    {:reply, entries, state}
  end

  @impl true
  def handle_info(:expire, state) do
    state = %{state | timer: nil}
    now = System.monotonic_time(:millisecond)
    state = remove_expired(state, now)

    state =
      if state.history == [] do
        state
      else
        schedule_expiration(state, now)
      end

    {:noreply, state}
  end

  defp remove_expired(state, now) do
    %{state | history: Enum.reject(state.history, &(&1.expires_at <= now))}
  end

  defp schedule_expiration(state, now) do
    next_expiration = state.history |> Enum.map(& &1.expires_at) |> Enum.min()
    delay = max(next_expiration - now, 0)
    %{state | timer: Process.send_after(self(), :expire, delay)}
  end
end
