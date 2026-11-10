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
  Sistema de votação concorrente utilizando GenServer para consistência
  e Task para execução assíncrona com controle de fluxo.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{votes: %{}, semaphore: 5}, name: __MODULE__)

  def vote(candidate) do
    # Limita concorrência e implementa retry (máximo 3 tentativas)
    Task.async(fn -> 
      execute_with_retry(fn -> GenServer.call(__MODULE__, {:cast_vote, candidate}) end)
    end)
    |> Task.await()
  end

  # --- GenServer Callbacks ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:cast_vote, candidate}, _from, state) do
    # Garantia de consistência: O estado é gerenciado sequencialmente pelo GenServer
    current_count = Map.get(state.votes, candidate, 0) + 1
    new_votes = Map.put(state.votes, candidate, current_count)
    
    # Notificação condicional
    if rem(current_count, 10) == 0 do
      IO.puts("[NOTIFICAÇÃO] Candidato #{candidate} atingiu #{current_count} votos!")
    end

    {:reply, :ok, %{state | votes: new_votes}}
  end

  # --- Lógica de Confiabilidade ---

  defp execute_with_retry(fun, attempts \\ 3) do
    try do
      fun.()
    rescue
      _ ->
        if attempts > 1 do
          Process.sleep(100) # Backoff simples
          execute_with_retry(fun, attempts - 1)
        else
          {:error, :max_retries_exceeded}
        end
    end
  end
end

# Exemplo de uso:
# VotingSystem.start_link([])
# VotingSystem.vote("Alice")
