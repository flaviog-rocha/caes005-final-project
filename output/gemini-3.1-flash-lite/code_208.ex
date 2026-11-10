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
  Sistema de votação concorrente utilizando GenServer para gerenciamento de estado
  e um pool de semáforos (via Process Registry/Registry) para limitação de operações.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def register_candidate(id), do: GenServer.call(__MODULE__, {:register, id})
  def remove_candidate(id), do: GenServer.call(__MODULE__, {:remove, id})
  def get_candidates, do: GenServer.call(__MODULE__, :get_all)

  def vote(id) do
    # Limita concorrência: permite apenas 5 processos de votação simultâneos
    # usando um simples "gatekeeper" via Registry ou um worker pool.
    # Aqui utilizamos uma estratégia de semáforo simples.
    :poolboy.transaction(:vote_pool, fn pid ->
      GenServer.call(__MODULE__, {:vote, id})
    end, 5000)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, id}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, {:error, :already_exists}, state}
    else
      {:reply, :ok, Map.put(state, id, 0)}
    end
  end

  @impl true
  def handle_call({:remove, id}, _from, state) do
    {:reply, :ok, Map.delete(state, id)}
  end

  @impl true
  def handle_call({:vote, id}, _from, state) do
    case Map.get(state, id) do
      nil -> {:reply, {:error, :not_found}, state}
      count -> {:reply, :ok, Map.put(state, id, count + 1)}
    end
  end

  @impl true
  def handle_call(:get_all, _from, state) do
    {:reply, state, state}
  end
end

# --- Configuração do Pool de Limitação (Exemplo de infraestrutura) ---

defmodule VotingSystem.Application do
  use Application

  def start(_type, _args) do
    pool_options = [
      name: {:local, :vote_pool},
      worker_module: VotingSystem.Worker,
      size: 5, # Limite de 5 operações simultâneas
      max_overflow: 0
    ]

    children = [
      :poolboy.child_spec(:vote_pool, pool_options),
      VotingSystem
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule VotingSystem.Worker do
  use GenServer
  def start_link(_), do: GenServer.start_link(__MODULE__, nil)
  def init(_), do: {:ok, nil}
end
