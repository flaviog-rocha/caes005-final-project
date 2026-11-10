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

# Para rodar este exemplo, salve como voting_system.exs e execute: elixir voting_system.exs
# O sistema utiliza um Registry para isolar entidades e um GenServer supervisionado por uma
# árvore de supervisão (DynamicSupervisor) para garantir persistência e recuperação.

defmodule VotingSystem do
  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: VotingRegistry},
      {DynamicSupervisor, name: VotingSupervisor, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_all)
  end
end

defmodule VotingSystem.Entity do
  use GenServer, restart: :permanent

  # Definição de Erros
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # API Pública
  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id, name: via_tuple(entity_id))
  end

  def vote(entity_id, candidate) do
    GenServer.call(via_tuple(entity_id), {:vote, candidate})
  end

  # Callbacks
  @impl true
  def init(entity_id) do
    # Recupera estado persistido do arquivo (simulado)
    data = load_from_disk(entity_id)
    {:ok, %{id: entity_id, votes: data}}
  end

  @impl true
  def handle_call({:vote, candidate}, _from, state) do
    cond do
      !is_binary(candidate) ->
        {:reply, {:error, %ValidationError{message: "Candidato inválido"}}, state}

      Map.get(state.votes, candidate, 0) < 0 ->
        {:reply, {:error, %OperationalError{message: "Falha de banco de dados"}}, state}

      true ->
        new_votes = Map.update(state.votes, candidate, 1, &(&1 + 1))
        new_state = %{state | votes: new_votes}
        persist_to_disk(state.id, new_votes)
        {:reply, {:ok, new_votes}, new_state}
    end
  end

  # Helpers para persistência (Simulando persistência local simples)
  defp persist_to_disk(id, data), do: File.write("vote_#{id}.dets", :erlang.term_to_binary(data))

  defp load_from_disk(id) do
    case File.read("vote_#{id}.dets") do
      {:ok, binary} -> :erlang.binary_to_term(binary)
      _ -> %{}
    end
  end

  defp via_tuple(id), do: {:via, Registry, {VotingRegistry, id}}
end

# Demonstração de uso
VotingSystem.start(nil, nil)

# Criar um processo supervisionado para uma entidade
DynamicSupervisor.start_child(VotingSupervisor, {VotingSystem.Entity, "entidade_1"})

# Operações simultâneas e validação
IO.inspect(VotingSystem.Entity.vote("entidade_1", "Alice"))
IO.inspect(VotingSystem.Entity.vote("entidade_1", 123)) # Retorna ValidationError
