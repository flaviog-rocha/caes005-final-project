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
  Sistema de votação com deduplicação persistente e processamento por fluxo.
  """

  @table :online_voting_dets
  @max_operation_id_bytes 256
  @max_candidate_bytes 128

  @doc """
  Inicia o sistema. O arquivo DETS mantém os votos após reinicializações.
  """
  def start_link(path, opts \\ []) when is_binary(path) do
    name = Keyword.get(opts, :name, __MODULE__)

    Agent.start_link(fn -> open_table!(path) end, name: name)
  end

  @doc """
  Registra um voto uma única vez para cada identificador de operação.
  """
  def cast_vote(server \\ __MODULE__, operation_id, candidate) do
    with :ok <- validate_text(operation_id, @max_operation_id_bytes),
         :ok <- validate_text(candidate, @max_candidate_bytes) do
      Agent.get_and_update(server, fn table ->
        key = {:vote, operation_id}

        case :dets.lookup(table, key) do
          [_existing] ->
            {{:ok, :duplicate}, table}

          [] ->
            case :dets.insert(table, {key, candidate}) do
              :ok ->
                case :dets.sync(table) do
                  :ok -> {{:ok, :accepted}, table}
                  {:error, reason} -> {{:error, {:persistence_failed, reason}}, table}
                end

              {:error, reason} ->
                {{:error, {:persistence_failed, reason}}, table}
            end
        end
      end)
    end
  end

  @doc """
  Retorna um fluxo que registra cada item somente quando o fluxo é consumido.

  Cada item deve ser uma tupla `{operation_id, candidate}`. Itens inválidos
  produzem um erro sem executar uma operação de votação.
  """
  def vote_stream(server \\ __MODULE__, enumerable) do
    Stream.map(enumerable, fn
      {operation_id, candidate} ->
        cast_vote(server, operation_id, candidate)

      _invalid_item ->
        {:error, :invalid_input}
    end)
  end

  @doc """
  Conta os votos de um candidato sem carregar todos os registros na memória.
  """
  def count_votes(server \\ __MODULE__, candidate) do
    with :ok <- validate_text(candidate, @max_candidate_bytes) do
      Agent.get(server, fn table ->
        :dets.foldl(
          fn
            {{:vote, _operation_id}, stored_candidate}, count
            when stored_candidate == candidate ->
              count + 1

            _entry, count ->
              count
          end,
          0,
          table
        )
      end)
    end
  end

  defp open_table!(path) do
    case :dets.open_file(@table,
           file: String.to_charlist(path),
           type: :set,
           repair: true
         ) do
      {:ok, table} ->
        table

      {:error, reason} ->
        raise "Não foi possível abrir o armazenamento de votos: #{inspect(reason)}"
    end
  end

  defp validate_text(value, max_bytes) when is_binary(value) do
    cond do
      byte_size(value) == 0 ->
        {:error, :empty_value}

      byte_size(value) > max_bytes ->
        {:error, :value_too_long}

      not String.valid?(value) ->
        {:error, :invalid_encoding}

      String.trim(value) != value ->
        {:error, :surrounding_whitespace}

      true ->
        :ok
    end
  end

  defp validate_text(_value, _max_bytes), do: {:error, :invalid_value}
end
