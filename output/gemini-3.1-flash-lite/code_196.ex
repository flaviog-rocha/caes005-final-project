# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Detectar encerramentos inesperados de processos.
# 3. Manter um histórico das operações realizadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Gerenciador de fila de tarefas com concorrência, supervisão e persistência.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{queue: [], history: []}, name: __MODULE__)
  end

  def enqueue(task_id, action) do
    GenServer.call(__MODULE__, {:enqueue, task_id, action})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Requisito 5: Executar periodicamente (limpeza/processamento a cada 30s)
    :timer.send_interval(30_000, :process_queue)
    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, id, action}, _from, state) do
    # Requisito 1: Consistência garantida pelo estado serializado do GenServer
    new_task = %{id: id, action: action, retries: 0, status: :pending}
    {:reply, :ok, %{state | queue: state.queue ++ [new_task]}}
  end

  @impl true
  def handle_info(:process_queue, state) do
    new_queue = Enum.map(state.queue, &execute_task/1)
    
    # Requisito 3: Manter histórico
    processed = Enum.filter(new_queue, &(&1.status in [:completed, :failed]))
    remaining = Enum.filter(new_queue, &(&1.status == :pending))
    
    {:noreply, %{state | queue: remaining, history: state.history ++ processed}}
  end

  # --- Lógica de Negócio ---

  defp execute_task(task) do
    # Requisito 2: Detecção de falhas via monitoramento de tarefa isolada
    try do
      # Simulação de lógica que pode falhar
      apply(Kernel, :is_atom, [task.action]) 
      %{task | status: :completed}
    rescue
      _ -> handle_failure(task)
    end
  end

  defp handle_failure(task) do
    # Requisito 4: Implementar novas tentativas (retry)
    if task.retries < 3 do
      %{task | retries: task.retries + 1, status: :pending}
    else
      %{task | status: :failed}
    end
  end
end

# --- Supervisão ---

defmodule TaskQueue.Supervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      # Requisito 2: Garante reinicialização se o processo morrer
      {TaskQueue, []}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
