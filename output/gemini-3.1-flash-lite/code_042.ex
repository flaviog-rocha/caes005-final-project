# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir comunicação entre diferentes processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskManager do
  @moduledoc """
  Gerenciador de tarefas utilizando GenServer para estado e DynamicSupervisor para execução.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{history: [], running: %{}}, name: __MODULE__)

  def enqueue(task_id, task_fun) do
    GenServer.call(__MODULE__, {:enqueue, task_id, task_fun})
  end

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:enqueue, id, fun}, _from, state) do
    cond do
      Map.has_key?(state.running, id) ->
        {:reply, {:error, :already_running}, state}

      Enum.any?(state.history, &(&1.id == id and &1.status == :completed)) ->
        {:reply, {:error, :already_executed}, state}

      true ->
        case DynamicSupervisor.start_child(TaskSupervisor, {TaskWorker, [id, fun]}) do
          {:ok, pid} ->
            new_state = put_in(state.running[id], pid)
            {:reply, :ok, new_state}
          {:error, reason} ->
            {:reply, {:error, {:operational_failure, reason}}, state}
        end
    end
  end

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_info({:task_finished, id, result}, state) do
    new_history = [%{id: id, status: :completed, result: result, ts: System.system_time()}] ++ state.history
    {:noreply, %{state | history: new_history, running: Map.delete(state.running, id)}}
  end

  @impl true
  def handle_info({:task_failed, id, error}, state) do
    new_history = [%{id: id, status: :failed, error: error, ts: System.system_time()}] ++ state.history
    {:noreply, %{state | history: new_history, running: Map.delete(state.running, id)}}
  end
end

defmodule TaskWorker do
  use Task, restart: :temporary

  def start_link([id, fun]) do
    Task.start_link(fn ->
      try do
        # Validação simples
        unless is_function(fun, 0), do: raise(ArgumentError, "Task must be a 0-arity function")
        
        result = fun.()
        send(TaskManager, {:task_finished, id, result})
      rescue
        e in [ArgumentError] ->
          send(TaskManager, {:task_failed, id, {:validation_error, Exception.message(e)}})
        e ->
          send(TaskManager, {:task_failed, id, {:operational_failure, Exception.message(e)}})
      end
    end)
  end
end

defmodule ApplicationSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [])

  @impl true
  def init(_) do
    children = [
      {DynamicSupervisor, name: TaskSupervisor, strategy: :one_for_one},
      TaskManager
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end
end
