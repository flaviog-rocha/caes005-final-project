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

  # --- Tipos ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{history: [], running: %{}}, name: __MODULE__)

  def enqueue(id, task_func) do
    GenServer.call(__MODULE__, {:enqueue, id, task_func})
  end

  def get_status, do: GenServer.call(__MODULE__, :get_status)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:enqueue, id, task_func}, _from, state) do
    cond do
      Map.has_key?(state.running, id) ->
        {:reply, {:error, :already_running}, state}

      Enum.any?(state.history, &(&1.id == id and &1.status == :completed)) ->
        {:reply, {:error, :already_executed}, state}

      true ->
        # Inicia o worker via DynamicSupervisor
        child_spec = {TaskWorker, {id, task_func}}
        case DynamicSupervisor.start_child(TaskManager.Supervisor, child_spec) do
          {:ok, _pid} ->
            new_state = put_in(state.running[id], :started)
            {:reply, :ok, new_state}
          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info({:task_finished, id, result}, state) do
    new_history = [%{id: id, status: :completed, result: result} | state.history]
    {:noreply, %{state | history: new_history, running: Map.delete(state.running, id)}}
  end

  @impl true
  def handle_info({:task_failed, id, error}, state) do
    new_history = [%{id: id, status: :failed, error: error} | state.history]
    {:noreply, %{state | history: new_history, running: Map.delete(state.running, id)}}
  end
end

defmodule TaskWorker do
  use Task, restart: :temporary

  def start_link({id, task_func}) do
    Task.start_link(fn ->
      try do
        result = task_func.()
        send(TaskManager, {:task_finished, id, result})
      rescue
        e in [TaskManager.ValidationError, TaskManager.OperationalError] ->
          send(TaskManager, {:task_failed, id, e})
        e ->
          send(TaskManager, {:task_failed, id, %TaskManager.OperationalError{message: inspect(e)}})
      end
    end)
  end
end

defmodule TaskManager.Supervisor do
  use DynamicSupervisor

  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)
end

# --- Inicialização para demonstração ---
# {TaskManager.Supervisor, []} |> Supervisor.start_link(strategy: :one_for_one)
# TaskManager.start_link([])
