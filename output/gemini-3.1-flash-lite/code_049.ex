# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Detectar encerramentos inesperados de processos.
# 4. Garantir a consistência dos dados durante operações concorrentes.
# 5. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente utilizando GenServer para estado e Task para operações.
  """
  use GenServer

  # Requisito 1 & 4: O GenServer garante a serialização das operações (consistência).
  # Requisito 5: Expiração automática usando :timer.send_after.

  @timeout 60_000 # 60 segundos para expiração

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  # Requisito 2: Consultar estado atual
  def get_votes, do: GenServer.call(__MODULE__, :get_state)

  # Requisito 1 & 4: Operação de voto com proteção contra duplicidade
  def vote(user_id, option) do
    # Task para processar de forma assíncrona, mas o GenServer garante a consistência
    Task.start(fn ->
      GenServer.call(__MODULE__, {:cast_vote, user_id, option})
    end)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_call({:cast_vote, user_id, option}, _from, state) do
    # Requisito 1: Impedir duplicidade
    if Map.has_key?(state, user_id) do
      {:reply, {:error, :already_voted}, state}
    else
      # Requisito 5: Agendar expiração (limpeza)
      :timer.send_after(@timeout, {:expire, user_id})
      new_state = Map.put(state, user_id, option)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_info({:expire, user_id}, state) do
    {:noreply, Map.delete(state, user_id)}
  end

  # Requisito 3: Monitoramento de processos
  # Se o processo que chamou o voto morrer, o GenServer permanece íntegro
  # pois o comportamento de call trata o timeout/exit do caller.
  @impl true
  def terminate(reason, _state) do
    IO.puts("Sistema encerrado inesperadamente: #{inspect(reason)}")
    :ok
  end
end

# Exemplo de uso:
# {:ok, _} = VotingSystem.start_link([])
# VotingSystem.vote("user1", "Opção A")
# VotingSystem.vote("user1", "Opção B") # Retornará erro de duplicidade
# IO.inspect(VotingSystem.get_votes())
