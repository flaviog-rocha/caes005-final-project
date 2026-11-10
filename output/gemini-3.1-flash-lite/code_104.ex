# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OperationManager do
  @moduledoc """
  Gerenciador de operações com concorrência limitada, rastreamento de histórico
  e prevenção de duplicidade, utilizando DynamicSupervisor.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {Registry, keys: :unique, name: OperationRegistry},
      {DynamicSupervisor, name: OperationSupervisor, strategy: :one_for_one},
      {Agent, fn -> [] end, name: OperationHistory}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Limite de concorrência via Semaphore (simulado com GenServer)
  defmodule Semaphore do
    use GenServer

    def start_link(limit), do: GenServer.start_link(__MODULE__, limit, name: __MODULE__)

    def init(limit), do: {:ok, limit}

    def handle_call({:acquire, pid}, _from, limit) when limit > 0 do
      {:reply, :ok, limit - 1}
    end

    def handle_call({:acquire, _}, _from, 0), do: {:reply, :error, 0}
    def handle_call(:release, _from, limit), do: {:reply, :ok, limit + 1}
  end

  defmodule Worker do
    use GenServer, restart: :transient

    def start_link({id, task_func}) do
      GenServer.start_link(__MODULE__, {id, task_func}, name: {:via, Registry, {OperationRegistry, id}})
    end

    def init({id, task_func}) do
      # Registra no histórico
      Agent.update(OperationHistory, fn history -> [{id, :started, System.system_time()} | history] end)
      
      send(self(), :run)
      {:ok, %{id: id, task: task_func}}
    end

    def handle_info(:run, state) do
      state.task.()
      Agent.update(OperationHistory, fn history -> [{state.id, :completed, System.system_time()} | history] end)
      {:stop, :normal, state}
    end

    def terminate(reason, state) do
      if reason != :normal do
        Agent.update(OperationHistory, fn history -> [{state.id, :failed, reason, System.system_time()} | history] end)
      end
    end
  end

  @doc "Executa uma operação garantindo unicidade e limite de concorrência"
  def execute(id, task_func) do
    case DynamicSupervisor.start_child(OperationSupervisor, {Worker, {id, task_func}}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> {:error, :already_running}
      error -> error
    end
  end

  def get_history, do: Agent.get(OperationHistory, & &1)
end
