# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir comunicação entre diferentes processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @moduledoc """
  Executa tarefas de monitoramento de sensores com limite de concorrência,
  detecta encerramentos inesperados e permite comunicação entre processos.
  """

  def start_link(sensors, sensor_fun, opts \\ [])
      when is_list(sensors) and is_function(sensor_fun, 2) do
    GenServer.start_link(__MODULE__, {sensors, sensor_fun, opts}, Keyword.take(opts, [:name]))
  end

  def send_to_sensor(server, sensor_id, message) do
    GenServer.call(server, {:send_to_sensor, sensor_id, message})
  end

  def broadcast(server, message) do
    GenServer.cast(server, {:broadcast, message})
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  def messages(server) do
    GenServer.call(server, :messages)
  end

  @impl true
  def init({sensors, sensor_fun, opts}) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    {:ok, task_supervisor} = Task.Supervisor.start_link()

    state = %{
      task_supervisor: task_supervisor,
      sensor_fun: sensor_fun,
      max_concurrency: max_concurrency,
      queue: :queue.from_list(sensors),
      tasks: %{},
      results: [],
      failures: [],
      messages: []
    }

    {:ok, dispatch(state)}
  end

  @impl true
  def handle_call(:status, _from, state) do
    reply = %{
      active: map_size(state.tasks),
      pending: :queue.len(state.queue),
      results: Enum.reverse(state.results),
      failures: Enum.reverse(state.failures)
    }

    {:reply, reply, state}
  end

  def handle_call(:messages, _from, state) do
    {:reply, Enum.reverse(state.messages), state}
  end

  def handle_call({:send_to_sensor, sensor_id, message}, _from, state) do
    case Enum.find_value(state.tasks, fn {_ref, task} ->
           if sensor_id(task.sensor) == sensor_id, do: task.pid
         end) do
      nil ->
        {:reply, :not_found, state}

      pid ->
        send(pid, {:sensor_monitor, message})
        {:reply, :sent, state}
    end
  end

  @impl true
  def handle_cast({:broadcast, message}, state) do
    Enum.each(state.tasks, fn {_ref, task} ->
      send(task.pid, {:sensor_monitor, message})
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:sensor_message, sensor_id, payload}, state) do
    {:noreply, %{state | messages: [{sensor_id, payload} | state.messages]}}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {task, tasks} ->
        Process.demonitor(ref, [:flush])

        state = %{
          state
          | tasks: tasks,
            results: [{task.sensor, result} | state.results]
        }

        {:noreply, dispatch(state)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {task, tasks} ->
        failures =
          if reason == :normal do
            state.failures
          else
            [{task.sensor, reason} | state.failures]
          end

        state = %{state | tasks: tasks, failures: failures}
        {:noreply, dispatch(state)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Supervisor.stop(state.task_supervisor, :shutdown)
    :ok
  end

  defp dispatch(state) when map_size(state.tasks) >= state.max_concurrency, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, sensor}, queue} ->
        coordinator = self()
        sensor_fun = state.sensor_fun

        task =
          Task.Supervisor.async_nolink(state.task_supervisor, fn ->
            sensor_fun.(sensor, coordinator)
          end)

        task_info = %{sensor: sensor, pid: task.pid}
        state = %{state | queue: queue, tasks: Map.put(state.tasks, task.ref, task_info)}

        dispatch(state)
    end
  end

  defp sensor(%{id: id}), do: id
  defp sensor(sensor), do: sensor
end

# A tarefa pode enviar dados ao monitor com:
# send(monitor_pid, {:sensor_message, sensor_id, payload})
#
# Ela recebe mensagens do monitor com:
# receive do
#   {:sensor_monitor, message} -> process_message(message)
# end
