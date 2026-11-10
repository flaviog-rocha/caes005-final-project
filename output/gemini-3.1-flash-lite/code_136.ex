# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Recuperar processos após falhas inesperadas.
# 3. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação resiliente utilizando GenServer e processamento via Streams.
  
  1. Processamento de grandes volumes: Utiliza `Stream` para leitura preguiçosa.
  2. Recuperação de falhas: Implementado através de uma `Supervisor` tree.
  3. Detecção de encerramentos: Utiliza `Process.monitor/1` para rastreamento.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {VotingSystem.Counter, name: :vote_counter},
      {VotingSystem.Manager, name: :vote_manager}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule VotingSystem.Counter do
  use GenServer

  # API
  def start_link(opts), do: GenServer.start_link(__MODULE__, %{}, opts)
  def add_vote(server, candidate), do: GenServer.cast(server, {:vote, candidate})

  # Callbacks
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:vote, candidate}, state) do
    new_state = Map.update(state, candidate, 1, &(&1 + 1))
    {:noreply, new_state}
  end
end

defmodule VotingSystem.Manager do
  use GenServer
  require Logger

  def start_link(opts), do: GenServer.start_link(__MODULE__, nil, opts)

  @impl true
  def init(_) do
    # Monitora o contador para detectar falhas inesperadas
    Process.monitor(Process.whereis(:vote_counter))
    {:ok, %{}}
  end

  @doc """
  Processa um volume massivo de votos via Stream, garantindo que o arquivo
  não seja carregado inteiramente na memória.
  """
  def process_votes(file_path) do
    File.stream!(file_path)
    |> Stream.map(&String.trim/1)
    |> Stream.each(fn candidate -> 
      VotingSystem.Counter.add_vote(:vote_counter, candidate) 
    end)
    |> Stream.run()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.error("Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    # O Supervisor reiniciará o processo automaticamente devido à estratégia :one_for_one
    {:noreply, state}
  end
end
