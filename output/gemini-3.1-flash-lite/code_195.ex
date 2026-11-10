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
  Gerenciador de fila de tarefas com concorrência, supervisão, 
  histórico, retentativas e execução periódica.
  """
  use GenServer

  # --- Interface Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{queue: [], history: []}, name: __MODULE__)

  def enqueue(task_id, action, opts \\ []) do
    GenServer.cast(__MODULE__, {:enqueue, task_id, action, opts})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Agendamento de execução periódica (ex: a cada 60 segundos)
    schedule_periodic_cleanup()
    {:ok, state}
  end

  @impl true
  def handle_cast({:enqueue, id, action, opts}, state) do
    # Garantia de consistência: Estado atualizado atomicamente no processo do GenServer
    new_task = %{
      id: id,
      action: action,
      retries: 0,
      max_retries: Keyword.get(opts, :max_retries, 3),
      status: :pending
    }
    
    {:noreply, %{state | queue: [new_task | state.queue]}}
  end

  @impl true
  def handle_info(:periodic_run, state) do
    # Execução periódica e processamento da fila
    new_state = process_queue(state)
    schedule_periodic_cleanup()
    {:noreply, new_state}
  end

  # --- Lógica Interna ---

  defp process_queue(state) do
    Enum.reduce(state.queue, %{state | queue: []}, fn task, acc ->
      case execute_task(task) do
        :ok ->
          # Manutenção de histórico
          log_op(acc, task, :success)
        {:error, _reason} when task.retries < task.max_retries ->
          # Retentativa após falha temporária
          retried_task = %{task | retries: task.retries + 1}
          %{acc | queue: [retried_task | acc.queue]}
        {:error, reason} ->
          log_op(acc, task, {:failed, reason})
      end
    end)
  end

  defp execute_task(task) do
    # Detecção de encerramentos inesperados via monitoramento local ou link
    # Uso de Task.async/await para isolar falhas de processo
    try do
      Task.async(fn -> task.action.() end) |> Task.await(5000)
      :ok
    rescue
      _ -> {:error, :process_crashed}
    catch
      :exit, _ -> {:error, :process_crashed}
    end
  end

  defp log_op(state, task, status) do
    entry = %{task_id: task.id, status: status, timestamp: System.system_time(:second)}
    %{state | history: [entry | state.history]}
  end

  defp schedule_periodic_cleanup do
    Process.send_after(self(), :periodic_run, 60_000)
  end
end
