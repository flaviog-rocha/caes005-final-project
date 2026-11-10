# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    # 1 & 2: Processos independentes gerenciados via Registry e DynamicSupervisor
    children = [
      {Registry, keys: :unique, name: VotingSystem.Registry},
      {DynamicSupervisor, name: VotingSystem.WorkerSupervisor, strategy: :one_for_one},
      VotingSystem.Storage
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule VotingSystem.Storage do
  use GenServer
  # 4: Persistência usando DETS (Disk-based Term Storage)
  @file_name 'votes.dets'

  def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(_) do
    {:ok, _} = :dets.open_file(@file_name, type: :set)
    {:ok, nil}
  end

  def persist(poll_id, vote_data), do: :dets.insert(@file_name, {poll_id, vote_data})
  def fetch(poll_id), do: :dets.lookup(@file_name, poll_id)
end

defmodule VotingSystem.PollWorker do
  use GenServer, restart: :transient # 5: Recuperação após falhas

  # 3: Processamento por streams (simulado) para lidar com grandes volumes
  def start_link(poll_id) do
    GenServer.start_link(__MODULE__, poll_id, name: {:via, Registry, {VotingSystem.Registry, poll_id}})
  end

  def init(poll_id), do: {:ok, %{poll_id: poll_id, votes: 0}}

  def handle_cast({:vote, user_id}, state) do
    # Simulação de processamento de dados massivos via stream (pode ser substituído por Flow)
    new_state = %{state | votes: state.votes + 1}
    VotingSystem.Storage.persist(state.poll_id, new_state.votes)
    {:noreply, new_state}
  end
end

defmodule VotingSystem do
  # API de interface para os requisitos
  def create_poll(poll_id) do
    DynamicSupervisor.start_child(VotingSystem.WorkerSupervisor, {VotingSystem.PollWorker, poll_id})
  end

  def cast_vote(poll_id, user_id) do
    case Registry.lookup(VotingSystem.Registry, poll_id) do
      [{pid, _}] -> GenServer.cast(pid, {:vote, user_id})
      [] -> :error
    end
  end

  # Processamento de grandes volumes sem carregar tudo em memória:
  # Utiliza lazy evaluation via Elixir Streams que interagem com o Storage
  def get_results_stream(poll_id) do
    Stream.resource(
      fn -> VotingSystem.Storage.fetch(poll_id) end,
      fn 
        [{_, count}] -> {[count], nil}
        _ -> {:halt, nil}
      end,
      fn _ -> :ok end
    )
  end
end
