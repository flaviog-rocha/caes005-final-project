# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Implementar expiração automática de registros.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores com suporte a simultaneidade, expiração (TTL),
  limitação de workers e retry com backoff.
  """

  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def track_sensor(sensor_id, task_fn, ttl_ms \\ 5000) do
    GenServer.call(__MODULE__, {:track, sensor_id, task_fn, ttl_ms})
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  # --- Callbacks GenServer ---

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 10)
    {:ok, %{tasks: %{}, max_concurrency: max_concurrency, active_count: 0}}
  end

  @impl true
  def handle_call({:track, id, task_fn, ttl}, _from, state) do
    if state.active_count < state.max_concurrency do
      task = Task.async(fn -> execute_with_retry(task_fn, 3) end)
      
      # Agenda expiração automática
      Process.send_after(self(), {:expire, id}, ttl)
      
      new_tasks = Map.put(state.tasks, id, %{task: task, status: :running})
      {:reply, {:ok, id}, %{state | tasks: new_tasks, active_count: state.active_count + 1}}
    else
      {:reply, {:error, :limit_reached}, state}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state.tasks, state}
  end

  @impl true
  def handle_info({:expire, id}, state) do
    new_tasks = Map.delete(state.tasks, id)
    {:noreply, %{state | tasks: new_tasks, active_count: max(0, state.active_count - 1)}}
  end

  # --- Lógica Privada ---

  defp execute_with_retry(func, retries) do
    try do
      func.()
    rescue
      e ->
        if retries > 0 do
          :timer.sleep(100) # Backoff simples
          execute_with_retry(func, retries - 1)
        else
          reraise e, __STACKTRACE__
        end
    end
  end
end
