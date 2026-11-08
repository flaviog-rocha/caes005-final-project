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

defmodule OnlineVoting do
  @moduledoc """
  Sistema de votação com operações idempotentes, processamento em fluxo e
  persistência em disco.
  """

  @table :online_voting_dets
  @max_field_size 256

  @doc """
  Inicia o sistema usando `path` como arquivo persistente.
  """
  def start(path) when is_binary(path) do
    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} ->
        case Agent.start_link(fn -> @table end) do
          {:ok, server} ->
            {:ok, server}

          {:error, reason} ->
            :dets.close(@table)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Para o processo e fecha o armazenamento persistente.
  """
  def stop(server) do
    with :ok <- Agent.stop(server),
         :ok <- :dets.close(@table) do
      :ok
    end
  end

  @doc """
  Registra um voto. Repetir uma operação com o mesmo `operation_id` não
  altera novamente os dados.
  """
  def vote(server, operation_id, voter_id, candidate) do
    with :ok <- validate_field(operation_id, :invalid_operation_id),
         :ok <- validate_field(voter_id, :invalid_voter_id),
         :ok <- validate_field(candidate, :invalid_candidate) do
      Agent.get_and_update(server, fn table ->
        case :dets.lookup(table, operation_id) do
          [_existing_vote] ->
            { {:ok, :already_processed}, table }

          [] ->
            case :dets.insert(table, {operation_id, voter_id, candidate}) do
              :ok ->
                case :dets.sync(table) do
                  :ok ->
                    {{:ok, :recorded}, table}

                  {:error, reason} ->
                    {{:error, {:persistence_failed, reason}}, table}
                end

              {:error, reason} ->
                {{:error, {:persistence_failed, reason}}, table}
            end
        end
      end)
    end
  end

  @doc """
  Conta os votos de um candidato sem carregar todos os registros em memória.
  """
  def count_votes(server, candidate) do
    with :ok <- validate_field(candidate, :invalid_candidate) do
      Agent.get(server, fn table ->
        :dets.foldl(
          fn {_operation_id, _voter_id, stored_candidate}, count ->
            if stored_candidate == candidate, do: count + 1, else: count
          end,
          0,
          table
        )
      end)
    end
  end

  @doc """
  Processa um arquivo TSV em fluxo. Cada linha deve conter:
  `operation_id`, `voter_id` e `candidate`, separados por tabulações.
  """
  def vote_file(server, path) when is_binary(path) do
    case File.open(path, [:read], fn io ->
           File.stream!(io, [], :line)
           |> Stream.with_index(1)
           |> Enum.reduce_while({:ok, %{recorded: 0, already_processed: 0}}, fn
             {line, line_number}, {:ok, counts} ->
               case parse_line(line) do
                 {:ok, operation_id, voter_id, candidate} ->
                   case vote(server, operation_id, voter_id, candidate) do
                     {:ok, :recorded} ->
                       {:cont, {:ok, %{counts | recorded: counts.recorded + 1}}}

                     {:ok, :already_processed} ->
                       {:cont,
                        {:ok,
                         %{counts | already_processed: counts.already_processed + 1}}}

                     {:error, reason} ->
                       {:halt, {:error, {:line, line_number, reason}}}
                   end

                 {:error, reason} ->
                   {:halt, {:error, {:line, line_number, reason}}}
               end
           end)
         end) do
      {:ok, result} -> result
      {:error, reason} -> {:error, {:file, reason}}
    end
  end

  defp parse_line(line) do
    line =
      line
      |> String.trim_trailing("\n")
      |> String.trim_trailing("\r")

    case String.split(line, "\t", parts: 3) do
      [operation_id, voter_id, candidate] ->
        with :ok <- validate_field(operation_id, :invalid_operation_id),
             :ok <- validate_field(voter_id, :invalid_voter_id),
             :ok <- validate_field(candidate, :invalid_candidate) do
          {:ok, operation_id, voter_id, candidate}
        end

      _ ->
        {:error, :invalid_line}
    end
  end

  defp validate_field(value, error)
       when is_binary(value) and byte_size(value) > 0 and
              byte_size(value) <= @max_field_size do
    if String.trim(value) != "" and
         not String.contains?(value, ["\t", "\n", "\r"]) do
      :ok
    else
      {:error, error}
    end
  end

  defp validate_field(_value, error), do: {:error, error}
end
