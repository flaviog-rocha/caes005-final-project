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

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente utilizando GenServer e Registry para isolamento.
  """

  defmodule VoteServer do
    use GenServer

    # Macro para definir estados iniciais de forma declarativa
    defmacro __using__(_opts) do
      quote do
        def start_link(entity_id) do
          GenServer.start_link(__MODULE__, 0, name: via_tuple(entity_id))
        end
      end
    end

    defp via_tuple(entity_id), do: {:via, Registry, {VotingRegistry, entity_id}}

    def init(initial_votes), do: {:ok, initial_votes}

    def handle_call(:get_votes, _from, state), do: {:reply, state, state}

    def handle_cast(:increment, state), do: {:noreply, state + 1}

    # API Pública
    def vote(entity_id) do
      GenServer.cast(via_tuple(entity_id), :increment)
    end

    def get_count(entity_id) do
      GenServer.call(via_tuple(entity_id), :get_votes)
    end
  end

  defmodule Supervisor do
    use Supervisor

    def start_link(_) do
      Supervisor.start_link(__MODULE__, [], name: __MODULE__)
    end

    def init(_) do
      children = [
        {Registry, keys: :unique, name: VotingRegistry},
        {DynamicSupervisor, name: VotingSupervisor, strategy: :one_for_one}
      ]

      Supervisor.init(children, strategy: :one_for_all)
    end

    # Método para iniciar uma nova entidade de votação dinamicamente
    def start_entity(entity_id) do
      DynamicSupervisor.start_child(VotingSupervisor, {VoteServer, entity_id})
    end
  end
end

# Exemplo de uso:
# 1. Iniciar o Supervisor
# VotingSystem.Supervisor.start_link(nil)
#
# 2. Registrar entidades independentes (ex: "enquete_1", "enquete_2")
# VotingSystem.Supervisor.start_entity("enquete_1")
# VotingSystem.Supervisor.start_entity("enquete_2")
#
# 3. Votar de forma concorrente (garantindo consistência via serialização do GenServer)
# VotingSystem.VoteServer.vote("enquete_1")
# VotingSystem.VoteServer.vote("enquete_1")
#
# 4. Consultar resultados
# IO.inspect(VotingSystem.VoteServer.get_count("enquete_1")) # => 2
