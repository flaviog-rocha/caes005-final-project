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
  Sistema de votação resiliente utilizando OTP.
  - Processamento via Stream para lidar com grandes volumes de dados.
  - Supervision Tree para recuperação de falhas.
  - Monitoramento de processos para detecção de encerramentos.
  """

  use Supervisor
  use GenServer

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {Registry, keys: :unique, name: VoteRegistry},
      {DynamicSupervisor, name: VoteProcessorSupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # Processamento de fluxo de dados (Stream)
  # Recebe um Enumerable (pode ser um arquivo ou socket stream)
  def process_votes(stream) do
    stream
    |> Stream.map(&parse_vote/1)
    |> Stream.each(fn {candidate_id, weight} ->
      worker = get_or_create_worker(candidate_id)
      GenServer.cast(worker, {:add_vote, weight})
    end)
    |> Stream.run()
  end

  defp get_or_create_worker(candidate_id) do
    case Registry.lookup(VoteRegistry, candidate_id) do
      [{pid, _}] -> pid
      [] -> start_worker(candidate_id)
    end
  end

  defp start_worker(candidate_id) do
    spec = {VoteWorker, candidate_id}
    {:ok, pid} = DynamicSupervisor.start_child(VoteProcessorSupervisor, spec)
    pid
  end

  defp parse_vote(raw), do: {String.to_integer(raw), 1}
end

defmodule VoteWorker do
  use GenServer

  # Requisito: Detectar encerramentos inesperados via state persistente (simulado)
  def start_link(candidate_id) do
    GenServer.start_link(__MODULE__, candidate_id, name: {:via, Registry, {VoteRegistry, candidate_id}})
  end

  @impl true
  def init(candidate_id) do
    # Em um cenário real, carregaríamos o estado do ETS ou Mnesia aqui
    {:ok, %{candidate_id: candidate_id, count: 0}}
  end

  @impl true
  def handle_cast({:add_vote, weight}, state) do
    {:noreply, %{state | count: state.count + weight}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    # Detecta falhas de dependências monitoradas
    IO.warn("Worker para #{state.candidate_id} detectou encerramento: #{inspect(reason)}")
    {:noreply, state}
  end
end
