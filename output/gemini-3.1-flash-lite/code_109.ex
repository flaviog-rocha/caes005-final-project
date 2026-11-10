# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir a execução de operações simultâneas.
# 4. Garantir a consistência dos dados durante operações concorrentes.
# 5. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente usando Agent para persistência em memória
  e persistência em disco para recuperação após reinicializações.
  """

  use Agent

  @storage_file "votes.state"

  def start_link(_opts) do
    initial_state = load_from_disk()
    Agent.start_link(fn -> initial_state end, name: __MODULE__)
  end

  @doc """
  Incrementa o voto de um candidato de forma atômica.
  Garante consistência usando a natureza serializada do Agent.
  """
  def vote_for(candidate_id) do
    Agent.update(__MODULE__, fn state ->
      new_state = Map.update(state, candidate_id, 1, &(&1 + 1))
      save_to_disk(new_state)
      new_state
    end)
  end

  @doc """
  Retorna a contagem atual de votos.
  """
  def get_votes(candidate_id) do
    Agent.get(__MODULE__, fn state -> Map.get(state, candidate_id, 0) end)
  end

  @doc """
  Retorna todos os votos.
  """
  def get_all_votes do
    Agent.get(__MODULE__, & &1)
  end

  # Persistência básica para garantir que dados sobrevivam a reinicializações.
  # Em um cenário real, usaria-se Ecto + Postgre ou Mnesia.
  defp save_to_disk(state) do
    binary = :erlang.term_to_binary(state)
    File.write!(@storage_file, binary)
  end

  defp load_from_disk do
    if File.exists?(@storage_file) do
      @storage_file
      |> File.read!()
      |> :erlang.binary_to_term()
    else
      %{}
    end
  end
end

# Exemplo de uso concorrente:
# {:ok, _pid} = VotingSystem.start_link([])
#
# # Simula múltiplos usuários votando simultaneamente
# tasks = for i <- 1..100 do
#   Task.async(fn -> VotingSystem.vote_for("candidato_a") end)
# end
# Enum.each(tasks, &Task.await/1)
#
# IO.inspect(VotingSystem.get_votes("candidato_a"))
