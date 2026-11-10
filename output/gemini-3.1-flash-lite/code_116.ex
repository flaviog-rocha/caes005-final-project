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
  Sistema de votação utilizando Agents para estado e streams para processamento eficiente.
  """

  use Agent

  @doc "Inicia o sistema com um arquivo de log para persistência."
  def start_link(file_path) do
    # O estado é um MapSet de IDs de transação para garantir idempotência
    initial_state = load_from_disk(file_path)
    Agent.start_link(fn -> %{processed: initial_state, path: file_path} end, name: __MODULE__)
  end

  @doc "Processa um voto validando unicidade e persistindo em append-only."
  def cast_vote(vote_id, candidate_id) when is_binary(vote_id) and is_binary(candidate_id) do
    Agent.get_and_update(__MODULE__, fn state ->
      if MapSet.member?(state.processed, vote_id) do
        {:error, state}
      else
        # Persistência síncrona simples em arquivo (Append-only)
        File.write(state.path, "#{vote_id}:#{candidate_id}\n", [:append])
        new_state = %{state | processed: MapSet.put(state.processed, vote_id)}
        {:ok, new_state}
      end
    end)
  end

  @doc "Processa grandes volumes de dados via Stream para evitar carga na memória."
  def process_batch(file_path) do
    file_path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.filter(&valid_format?/1)
    |> Stream.each(fn line ->
      [id, _] = String.split(line, ":")
      # Processamento lógico de cada linha sem carregar tudo
      IO.puts("Processando voto ID: #{id}")
    end)
    |> Stream.run()
  end

  defp valid_format?(line), do: String.contains?(line, ":")

  defp load_from_disk(path) do
    if File.exists?(path) do
      path
      |> File.stream!()
      |> Stream.map(fn line -> List.first(String.split(line, ":")) end)
      |> Enum.into(MapSet.new())
    else
      MapSet.new()
    end
  end
end
