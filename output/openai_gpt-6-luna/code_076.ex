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
  Executa funções de sensores como Tasks, limita sua concorrência e notifica
  o processo que iniciou o monitor sobre leituras e encerramentos inesperados.

  Cada função de sensor recebe uma função `report/1`, que pode ser chamada
  para enviar leituras ao processo monitor.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def monitor_sensor(monitor, sensor_id, sensor_fun)
      when is_function(sensor_fun, 1) do
    GenServer.call(monitor, {:monitor_sensor, sensor_id, sensor_fun})
  end

  def report(monitor, sensor_id, reading) do
    GenServer.cast(monitor, {:report, sensor_id, reading})
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    owner = Keyword.get(opts, :owner, self())

    if is_integer(max_concurrency) and max_concurrency > 0 and is_pid(owner) do
      case Task.Supervisor.start_link(max_children: max_concurrency) do
        {:ok, task_supervisor} ->
          {:ok,
           %{
             task_supervisor: task_supervisor,
             max_concurrency: max_concurrency,
             owner: owner,
             active: %{},
             queue: :queue.new()
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:monitor_sensor, sensor_id, sensor_fun}, _from, state) do
    queue = :queue.in({sensor_id, sensor_fun}, state.queue)
    {:reply, :ok, start_queued_tasks(%{state | queue: queue})}
  end

  @impl true
  def handle_cast({:report, sensor_id, reading}, state) do
    send(state.owner, {:sensor_reading, sensor_id, reading})
    {:noreply, state}
  end

  @impl true
  def handle_info({:sensor_reading, sensor_id, reading}, state) do
    send(state.owner, {:sensor_reading, sensor_id, reading})
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {%{pid: ^pid, sensor_id: sensor_id}, active} ->
        if reason != :normal do
          send(state.owner, {:sensor_down, sensor_id, reason})
        end

        {:noreply, start_queued_tasks(%{state | active: active})}
    end
  end

  defp start_queued_tasks(state) do
    if map_size(state.active) < state.max_concurrency do
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {sensor_id, sensor_fun}}, queue} ->
          state = %{state | queue: queue}

          reporter = fn reading ->
            send(self_monitor_pid(state), {:sensor_reading, sensor_id, reading})
          end

          case Task.Supervisor.start_child(state.task_supervisor, fn ->
                 sensor_fun.(reporter)
               end) do
            {:ok, pid} ->
              ref = Process.monitor(pid)
              active = Map.put(state.active, ref, %{pid: pid, sensor_id: sensor_id})
              start_queued_tasks(%{state | active: active})

            {:error, reason} ->
              send(state.owner, {:sensor_start_error, sensor_id, reason})
              start_queued_tasks(state)
          end
      end
    else
      state
    end
  end

  defp self_monitor_pid(state) do
    # O processo monitor é armazenado no estado para permitir comunicação
    # entre as Tasks e o GenServer.
    state.monitor_pid
  end
end
