# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @registry SensorMonitor.Registry
  @task_supervisor SensorMonitor.TaskSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(
      SensorMonitor.Supervisor,
      opts,
      name: Keyword.get(opts, :supervisor_name, SensorMonitor.Supervisor)
    )
  end

  def submit(sensor_id, read_fun, opts \\ []) do
    submit(__MODULE__, sensor_id, read_fun, opts)
  end

  def submit(server, sensor_id, read_fun, opts) do
    with :ok <- validate_submission(sensor_id, read_fun, opts) do
      GenServer.call(server, {:submit, sensor_id, read_fun, opts})
    end
  end

  def readings(server \\ __MODULE__) do
    GenServer.call(server, :readings)
  end

  def subscribe do
    Registry.register(@registry, :notifications, nil)
  end

  def unsubscribe do
    Registry.unregister(@registry, :notifications)
  end

  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    path = Keyword.get(opts, :path, "sensor_monitor.term")

    if is_integer(max_concurrency) and max_concurrency > 0 and is_binary(path) do
      {:ok,
       %{
         max_concurrency: max_concurrency,
         path: path,
         readings: load_readings(path),
         active: %{},
         queue: :queue.new()
       }}
    else
      {:stop, {:validation, :invalid_monitor_options}}
    end
  end

  def handle_call({:submit, sensor_id, read_fun, opts}, _from, state) do
    job = %{
      id: sensor_id,
      ref: make_ref(),
      read_fun: read_fun,
      condition: Keyword.get(opts, :condition),
      attempts: 0,
      max_retries: Keyword.get(opts, :max_retries, 3),
      retry_delay: Keyword.get(opts, :retry_delay, 100)
    }

    state = %{state | queue: :queue.in(job, state.queue)}
    {:reply, {:ok, job.ref}, dispatch(state)}
  end

  def handle_call(:readings, _from, state) do
    {:reply, state.readings, state}
  end

  def handle_info({ref, result}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{task, job}, active} ->
        Process.demonitor(task.ref, [:flush])
        state = %{state | active: active}
        {:noreply, state |> handle_result(job, result) |> dispatch()}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {{_task, job}, active} ->
        state = %{state | active: active}
        {:noreply, state |> retry_or_finish(job, {:temporary, {:task_exit, reason}}) |> dispatch()}
    end
  end

  def handle_info({:retry, job}, state) do
    state = %{state | queue: :queue.in(job, state.queue)}
    {:noreply, dispatch(state)}
  end

  defp dispatch(state) do
    if map_size(state.active) < state.max_concurrency do
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, job}, queue} ->
          task =
            Task.Supervisor.async_nolink(@task_supervisor, fn ->
              execute(job.read_fun)
            end)

          state = %{state | queue: queue, active: Map.put(state.active, task.ref, {task, job})}
          dispatch(state)
      end
    else
      state
    end
  end

  defp execute(read_fun) do
    try do
      read_fun.()
    rescue
      error ->
        {:error, {:temporary, {:exception, error.__struct__, Exception.message(error)}}}
    catch
      kind, reason ->
        {:error, {:temporary, {:caught, kind, reason}}}
    end
  end

  defp handle_result(state, job, {:ok, value}) do
    reading = %{value: value, updated_at: System.system_time(:millisecond)}
    readings = Map.put(state.readings, job.id, reading)
    state = %{state | readings: readings}

    case persist(state.path, readings) do
      :ok ->
        notify({:sensor_result, job.id, {:ok, value}, job.ref})

      {:error, reason} ->
        notify({:sensor_result, job.id, {:error, {:operational, {:persistence, reason}}}, job.ref})
    end

    if condition_met?(job.condition, value) do
      notify({:sensor_alert, job.id, value, job.ref})
    end

    state
  end

  defp handle_result(state, job, {:error, {:validation, reason}}) do
    notify({:sensor_result, job.id, {:error, {:validation, reason}}, job.ref})
    state
  end

  defp handle_result(state, job, {:error, {:temporary, reason}}) do
    retry_or_finish(state, job, {:temporary, reason})
  end

  defp handle_result(state, job, {:error, reason}) do
    notify({:sensor_result, job.id, {:error, {:operational, reason}}, job.ref})
    state
  end

  defp handle_result(state, job, other) do
    notify({:sensor_result, job.id, {:error, {:operational, {:invalid_result, other}}}, job.ref})
    state
  end

  defp retry_or_finish(state, job, {:temporary, reason}) do
    if job.attempts < job.max_retries do
      attempt = job.attempts + 1
      delay = min(job.retry_delay * trunc(:math.pow(2, job.attempts)), 60_000)
      retry_job = %{job | attempts: attempt}

      notify({:sensor_retry, job.id, attempt, reason, job.ref})
      Process.send_after(self(), {:retry, retry_job}, delay)
      state
    else
      notify({:sensor_result, job.id, {:error, {:operational, reason}}, job.ref})
      state
    end
  end

  defp condition_met?(nil, _value), do: false

  defp condition_met?(condition, value) do
    try do
      condition.(value) == true
    rescue
      _ -> false
    catch
      _, _ -> false
    end
  end

  defp notify(message) do
    Registry.dispatch(@registry, :notifications, fn subscribers ->
      Enum.each(subscribers, fn {pid, _value} -> send(pid, message) end)
    end)
  end

  defp validate_submission(sensor_id, read_fun, opts) do
    valid_id = not is_nil(sensor_id)
    valid_fun = is_function(read_fun, 0)
    valid_opts = Keyword.keyword?(opts)

    if valid_id and valid_fun and valid_opts do
      max_retries = Keyword.get(opts, :max_retries, 3)
      retry_delay = Keyword.get(opts, :retry_delay, 100)
      condition = Keyword.get(opts, :condition)

      if is_integer(max_retries) and max_retries >= 0 and
           is_integer(retry_delay) and retry_delay >= 0 and
           (is_nil(condition) or is_function(condition, 1)) do
        :ok
      else
        {:error, {:validation, :invalid_options}}
      end
    else
      {:error, {:validation, :invalid_submission}}
    end
  end

  defp load_readings(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            readings when is_map(readings) -> readings
            _ -> %{}
          end
        rescue
          _ -> %{}
        end

      {:error, :enoent} ->
        %{}

      {:error, _reason} ->
        %{}
    end
  end

  defp persist(path, readings) do
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, :erlang.term_to_binary(readings, [:compressed])),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end
end

defmodule SensorMonitor.Supervisor do
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: SensorMonitor.Registry},
      {Task.Supervisor, name: SensorMonitor.TaskSupervisor},
      {SensorMonitor, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
