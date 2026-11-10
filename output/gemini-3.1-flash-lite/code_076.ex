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
  @moduledoc """
  Monitor de sensores utilizando Task, GenServer para controle de concorrência
  e mensagens para comunicação entre processos.
  """

  use GenServer

  # --- API Pública ---

  def start_link(max_concurrency) do
    GenServer.start_link(__MODULE__, max_concurrency, name: __MODULE__)
  end

  @doc "Executa a leitura de um sensor respeitando o limite de concorrência"
  def read_sensor(sensor_id, work_fun) do
    GenServer.call(__MODULE__, {:run_task, sensor_id, work_fun})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(max_concurrency) do
    {:ok, %{max_concurrency: max_concurrency, running: 0, queue: :queue.new()}}
  end

  @impl true
  def handle_call({:run_task, id, fun}, from, state) do
    if state.running < state.max_concurrency do
      spawn_task(id, fun)
      {:reply, :ok, %{state | running: state.running + 1}}
    else
      {:noreply, %{state | queue: :queue.in({from, id, fun}, state.queue)}}
    end
  end

  @impl true
  def handle_info({_ref, :done}, state) do
    state = %{state | running: state.running - 1}
    process_next(state)
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    IO.puts("Alerta: Processo de sensor encerrou inesperadamente: #{inspect(reason)}")
    state = %{state | running: max(0, state.running - 1)}
    process_next(state)
  end

  # --- Funções Privadas ---

  defp spawn_task(id, fun) do
    parent = self()
    # Task.async garante o monitoramento do processo
    task = Task.async(fn ->
      result = fun.()
      send(parent, {nil, :done})
      result
    end)
    
    # Process.monitor detecta encerramentos inesperados
    Process.monitor(task.pid)
  end

  defp process_next(state) do
    case :queue.out(state.queue) do
      {{:value, {from, id, fun}}, rest} ->
        spawn_task(id, fun)
        GenServer.reply(from, :ok)
        {:noreply, %{state | running: state.running + 1, queue: rest}}
      {:empty, _} ->
        {:noreply, state}
    end
  end
end
