# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheManager do
  @moduledoc """
  Sistema de Cache resiliente com limitação de concorrência.
  Utiliza GenServer para estado e poolboy-like (via Task.Supervisor) para concorrência.
  """
  use GenServer

  @name __MODULE__
  @max_concurrency 10

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{cache: %{}, history: []}, name: @name)

  def put(key, value) do
    # Garante a limitação de concorrência usando o Supervisor de tarefas
    Task.Supervisor.async_nolink(CacheManager.TaskSupervisor, fn ->
      GenServer.call(@name, {:put, key, value})
    end)
    |> Task.await()
  end

  def get(key), do: GenServer.call(@name, {:get, key})

  def get_history, do: GenServer.call(@name, :get_history)

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Garante a recuperação de falhas através de um Supervisor que gerencia as tarefas
    {:ok, _} = Task.Supervisor.start_link(name: CacheManager.TaskSupervisor, max_children: @max_concurrency)
    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    new_cache = Map.put(state.cache, key, value)
    new_history = [{key, value, System.system_time(:second)} | state.history] |> Enum.take(100)
    
    {:reply, :ok, %{state | cache: new_cache, history: new_history}}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    {:reply, Map.get(state.cache, key), state}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  # --- Supervisão ---
  # Para atender ao requisito de recuperação, este módulo pode ser incluído 
  # em uma árvore de supervisão padrão.
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end
end
