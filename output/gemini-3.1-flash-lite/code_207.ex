# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente usando GenServer e um Pool de semáforos.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def create_vote(id, candidate), do: GenServer.call(__MODULE__, {:create, id, candidate})

  def get_vote(id), do: GenServer.call(__MODULE__, {:get, id})

  def update_vote(id, candidate), do: GenServer.call(__MODULE__, {:update, id, candidate})

  def delete_vote(id), do: GenServer.call(__MODULE__, {:delete, id})

  def get_all_votes, do: GenServer.call(__MODULE__, :get_all)

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_call({:create, id, candidate}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, {:error, :already_exists}, state}
    else
      {:reply, :ok, Map.put(state, id, candidate)}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state), do: {:reply, Map.fetch(state, id), state}

  @impl true
  def handle_call({:update, id, candidate}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, :ok, Map.put(state, id, candidate)}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state), do: {:reply, :ok, Map.delete(state, id)}

  @impl true
  def handle_call(:get_all, _from, state), do: {:reply, state, state}

  # --- Controle de Concorrência ---

  @doc """
  Executa uma operação limitada pelo número de processos concorrentes (Semáforo).
  """
  def with_concurrency_limit(max_concurrent, task_fun) do
    # Cria uma tabela ETS para atuar como contador de semáforo
    :ets.new(:semaphore, [:named_table, :public, :set])
    :ets.insert(:semaphore, {:active, 0})

    fn ->
      wait_for_slot(max_concurrent)
      result = task_fun.()
      release_slot()
      result
    end
  end

  defp wait_for_slot(max) do
    [{:active, count}] = :ets.lookup(:semaphore, :active)
    if count < max do
      :ets.insert(:semaphore, {:active, count + 1})
    else
      Process.sleep(10)
      wait_for_slot(max)
    end
  end

  defp release_slot do
    [{:active, count}] = :ets.lookup(:semaphore, :active)
    :ets.insert(:semaphore, {:active, max(0, count - 1)})
  end
end
