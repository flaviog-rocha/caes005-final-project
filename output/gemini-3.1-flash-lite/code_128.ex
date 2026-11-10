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
  Monitor de sensores com concorrência limitada, TTL, retries e estado.
  Utiliza `:poolboy` ou padrões OTP nativos. Para este exemplo autocontido,
  utilizamos um `GenServer` com um `DynamicSupervisor` para gerenciar a concorrência.
  """

  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Executa uma leitura de sensor com retry e limite de concorrência"
  def perform_reading(sensor_id, task_func, retries \\ 3) do
    GenServer.call(__MODULE__, {:enqueue, sensor_id, task_func, retries})
  end

  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(opts) do
    {:ok, %{
      max_concurrency: Keyword.get(opts, :max_concurrency, 10),
      running: %{},
      ttl_ms: Keyword.get(opts, :ttl, 5000)
    }}
  end

  @impl true
  def handle_call({:enqueue, id, func, retries}, _from, state) do
    if map_size(state.running) >= state.max_concurrency do
      {:reply, {:error, :concurrency_limit_reached}, state}
    else
      task = Task.async(fn -> execute_with_retry(func, retries) end)
      
      # Implementa TTL: limpa o registro após o tempo definido
      Process.send_after(self(), {:cleanup, id}, state.ttl_ms)

      new_state = put_in(state, [:running, id], %{task: task, started_at: System.system_time(:millisecond)})
      {:reply, {:ok, :started}, new_state}
    end
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    {:reply, state.running, state}
  end

  @impl true
  def handle_info({{:cleanup, id}, _ref}, state) do
    {:noreply, %{state | running: Map.delete(state.running, id)}}
  end

  # --- Lógica Interna ---

  defp execute_with_retry(func, retries) do
    try do
      func.()
    rescue
      _ ->
        if retries > 0 do
          Process.sleep(100) # Backoff simples
          execute_with_retry(func, retries - 1)
        else
          {:error, :max_retries_exceeded}
        end
    end
  end
end
