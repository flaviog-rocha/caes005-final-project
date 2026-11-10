# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente utilizando GenServer para consistência de estado,
  Task para processamento assíncrono e controle de concorrência com Task.Supervisor.
  """

  use GenServer

  # --- Configurações ---
  @max_concurrency 5
  @retry_limit 3

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{votes: %{}, threshold: 10}, name: __MODULE__)
  end

  def cast_vote(candidate) do
    # Limita operações simultâneas usando um pool de tarefas supervisionado
    Task.Supervisor.async_nolink(VotingTaskSupervisor, fn ->
      perform_vote_with_retry(candidate, @retry_limit)
    end)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:increment, candidate}, _from, state) do
    new_votes = Map.update(state.votes, candidate, 1, &(&1 + 1))
    new_state = %{state | votes: new_votes}
    
    # 1. Notificação se atingir threshold
    if new_votes[candidate] == state.threshold do
      spawn(fn -> IO.puts("Notificação: Candidato #{candidate} atingiu o limite de #{state.threshold} votos!") end)
    end

    {:reply, new_votes[candidate], new_state}
  end

  # --- Lógica Interna ---

  defp perform_vote_with_retry(candidate, retries) do
    try do
      GenServer.call(__MODULE__, {:increment, candidate})
    rescue
      # 4. Implementar novas tentativas após falhas temporárias
      _ ->
        if retries > 0 do
          :timer.sleep(50)
          perform_vote_with_retry(candidate, retries - 1)
        else
          {:error, :max_retries_reached}
        end
    end
  end
end

# --- Inicialização do Sistema ---

# Estrutura de supervisão simplificada para os requisitos solicitados
children = [
  {Task.Supervisor, name: VotingTaskSupervisor, max_children: 5},
  VotingSystem
]

{:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)

# Exemplo de uso:
# VotingSystem.cast_vote("Alice")
# VotingSystem.cast_vote("Bob")
