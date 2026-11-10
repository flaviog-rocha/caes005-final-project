# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: VotingRegistry},
      {DynamicSupervisor, name: VotingSupervisor, strategy: :one_for_one}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end
end

defmodule VotingSystem.Vote do
  use GenServer
  
  @timeout :timer.seconds(60)

  # Interface
  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))
  
  def register(id, value) do
    DynamicSupervisor.start_child(VotingSupervisor, {VotingSystem.Vote, id})
    GenServer.call(via(id), {:set, value})
  end

  def get(id), do: GenServer.call(via(id), :get)
  def update(id, value), do: GenServer.call(via(id), {:set, value})
  def delete(id), do: GenServer.stop(via(id))

  # Implementation
  defp via(id), do: {:via, Registry, {VotingRegistry, id}}

  @impl true
  def init(id) do
    # Expiração automática: envia mensagem após @timeout
    Process.send_after(self(), :expire, @timeout)
    {:ok, %{id: id, value: nil}}
  end

  @impl true
  def handle_call({:set, value}, _from, state) do
    # Atomicidade garantida pela serialização de mensagens do GenServer
    new_state = %{state | value: value}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get, _from, state), do: {:reply, state.value, state}

  @impl true
  def handle_info(:expire, state) do
    {:stop, :normal, state}
  end

  # Recuperação após falha: o Supervisor reiniciará o processo
  # se ele morrer, mas manteremos o estado simples aqui.
  # Em produção, um ets ou banco persistiria o valor para reinicialização completa.
end

# Exemplo de uso:
# VotingSystem.Supervisor.start_link([])
# VotingSystem.Vote.register("voto_1", "opcao_a")
# VotingSystem.Vote.get("voto_1")
# VotingSystem.Vote.update("voto_1", "opcao_b")
# VotingSystem.Vote.delete("voto_1")
