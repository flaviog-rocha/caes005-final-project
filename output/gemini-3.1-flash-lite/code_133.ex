# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação resiliente utilizando GenServer e DETS (Disk Based Term Storage).
  Atende aos requisitos de persistência, tolerância a falhas e isolamento.
  """

  use GenServer

  # --- API Pública ---

  def start_link(opts) do
    entity_id = Keyword.get(opts, :entity_id)
    GenServer.start_link(__MODULE__, entity_id, name: via_tuple(entity_id))
  end

  def vote(entity_id, candidate) do
    GenServer.call(via_tuple(entity_id), {:vote, candidate})
  end

  def get_votes(entity_id) do
    GenServer.call(via_tuple(entity_id), :get_votes)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(entity_id) do
    # Requisito 2: Persistência via DETS
    table = String.to_atom("votes_#{entity_id}")
    {:ok, _} = :dets.open_file(table, type: :set)
    {:ok, %{table: table, entity_id: entity_id}}
  end

  @impl true
  def handle_call({:vote, candidate}, _from, state) do
    # Requisito 5: Diferenciação de erros
    cond do
      !is_binary(candidate) or String.trim(candidate) == "" ->
        {:reply, {:error, :validation_failed}, state}

      true ->
        # Requisito 4: Operações simultâneas permitidas pelo modelo de atores
        current = get_count(state.table, candidate)
        :dets.insert(state.table, {candidate, current + 1})
        :dets.sync(state.table)
        {:reply, :ok, state}
    end
  rescue
    e ->
      # Requisito 1: Tratamento de falhas operacionais (Crash/Recovery)
      {:reply, {:error, :operational_failure}, state}
  end

  @impl true
  def handle_call(:get_votes, _from, state) do
    votes = :dets.foldl(fn {k, v}, acc -> [{k, v} | acc] end, [], state.table)
    {:reply, {:ok, votes}, state}
  end

  # --- Funções Auxiliares ---

  defp get_count(table, candidate) do
    case :dets.lookup(table, candidate) do
      [{^candidate, count}] -> count
      [] -> 0
    end
  end

  defp via_tuple(entity_id), do: {:via, Registry, {VotingRegistry, entity_id}}
end

# --- Supervisão e Inicialização ---

defmodule VotingSystem.Supervisor do
  use Supervisor

  def start_link(_) do
    # Requisito 1: Registro para recuperação de processos
    {:ok, _} = Registry.start_link(keys: :unique, name: VotingRegistry)
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      # Requisito 3: Cada entidade roda em seu próprio processo (isolamento)
      {DynamicSupervisor, name: VotingSystem.EntitySupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def start_entity(entity_id) do
    DynamicSupervisor.start_child(VotingSystem.EntitySupervisor, {VotingSystem, [entity_id: entity_id]})
  end
end
