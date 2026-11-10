# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Fazer uso de macros.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem.VoteServer do
  use GenServer

  # Macros para definir operações de estado de forma declarativa
  defmacro def_vote_operation(name, operation) do
    quote do
      def unquote(name)(pid, entity_id) do
        GenServer.call(pid, {:operate, unquote(operation), entity_id})
      end
    end
  end

  # Interface pública via macros
  def_vote_operation(:cast_vote, :increment)
  def_vote_operation(:reset_votes, :reset)

  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, %{votes: 0}, name: via_tuple(entity_id))
  end

  def init(state), do: {:ok, state}

  def handle_call({:operate, :increment, _}, _from, state) do
    new_state = %{state | votes: state.votes + 1}
    {:reply, new_state.votes, new_state}
  end

  def handle_call({:operate, :reset, _}, _from, _state) do
    {:reply, 0, %{votes: 0}}
  end

  defp via_tuple(entity_id), do: {:via, Registry, {VotingSystem.Registry, entity_id}}
end

defmodule VotingSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: VotingSystem.Registry},
      {DynamicSupervisor, name: VotingSystem.EntitySupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Garante independência entre entidades criando processos dinamicamente
  def start_entity(entity_id) do
    spec = {VotingSystem.VoteServer, entity_id}
    DynamicSupervisor.start_child(VotingSystem.EntitySupervisor, spec)
  end
end

# Exemplo de uso:
# 1. VotingSystem.Supervisor.start_link([])
# 2. VotingSystem.Supervisor.start_entity("candidato_1")
# 3. VotingSystem.VoteServer.cast_vote({:via, Registry, {VotingSystem.Registry, "candidato_1"}}, "candidato_1")
