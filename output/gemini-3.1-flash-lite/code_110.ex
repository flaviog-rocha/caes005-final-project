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
  Sistema de votação concorrente usando Agent para gerenciamento de estado
  e File IO para persistência atômica.
  """
  use Agent

  @storage_file "votes.json"

  def start_link(_opts) do
    initial_state = load_from_disk()
    Agent.start_link(fn -> initial_state end, name: __MODULE__)
  end

  @doc """
  Registra um voto de forma atômica.
  Utiliza Agent.update para garantir que a operação ocorra em série,
  mantendo a consistência dos dados sob alta concorrência.
  """
  def cast_vote(candidate_id) do
    Agent.update(__MODULE__, fn state ->
      new_state = Map.update(state, candidate_id, 1, &(&1 + 1))
      
      # Persistência para recuperação após reinicialização
      persist_to_disk(new_state)
      
      new_state
    end)
  end

  def get_results do
    Agent.get(__MODULE__, & &1)
  end

  @doc """
  Garante a atomicidade da persistência (impedindo atualizações parciais).
  Escreve em um arquivo temporário e faz o rename (operação atômica no SO).
  """
  defp persist_to_disk(state) do
    temp_file = @storage_file <> ".tmp"
    data = :erlang.term_to_binary(state)
    
    File.write!(temp_file, data)
    File.rename!(temp_file, @storage_file)
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
# # Simulação de concorrência com Task.async_stream
# 1..100 
# |> Task.async_stream(fn _ -> VotingSystem.cast_vote("candidate_a") end) 
# |> Stream.run()
#
# IO.inspect(VotingSystem.get_results())
