# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação utilizando Agent para estado, 
  Stream para processamento de memória eficiente e 
  validação rigorosa.
  """

  use Agent

  @storage_file "votes.log"

  # 1. Impedir duplicidade: Usamos um MapSet para rastrear IDs de votos únicos
  def start_link(_opts) do
    initial_state = %{processed_ids: MapSet.new()}
    Agent.start_link(fn -> initial_state end, name: __MODULE__)
  end

  # 2. Processar grandes volumes via Stream (Lazy evaluation)
  # Recebe um enumerable de votos (ex: linhas de um arquivo ou socket)
  def process_votes(vote_stream) do
    vote_stream
    |> Stream.map(&validate_vote/1)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, vote} -> register_vote(vote) end)
    |> Stream.run()
  end

  # 4. Validação de dados
  defp validate_vote(vote) do
    cond do
      is_map(vote) and Map.has_key?(vote, :id) and Map.has_key?(vote, :candidate) ->
        {:ok, vote}
      true ->
        {:error, :invalid_data}
    end
  end

  # 3. Persistência + Impedir duplicidade (Idempotência)
  defp register_vote(%{id: id, candidate: candidate} = vote) do
    Agent.get_and_update(__MODULE__, fn state ->
      if MapSet.member?(state.processed_ids, id) do
        {:already_processed, state}
      else
        persist_to_disk(vote)
        new_state = %{state | processed_ids: MapSet.put(state.processed_ids, id)}
        {:ok, new_state}
      end
    end)
  end

  defp persist_to_disk(vote) do
    File.write(@storage_file, :erlang.term_to_binary(vote) <> "\n", [:append])
  end

  # Recuperação de estado após reinicialização
  def recover_state do
    if File.exists?(@storage_file) do
      File.stream!(@storage_file)
      |> Stream.map(&:erlang.binary_to_term/1)
      |> Enum.reduce(MapSet.new(), fn vote, acc -> 
        MapSet.put(acc, vote.id) 
      end)
    else
      MapSet.new()
    end
  end
end
