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

defmodule OnlineVoting do
  @moduledoc """
  Sistema de votação persistente em arquivo.

  Cada arquivo deve ser usado por uma única instância do sistema. As operações
  sobre uma instância são serializadas pelo Agent.
  """

  @version 1

  @doc "Inicia o Agent e recupera as votações do arquivo indicado."
  def start_link(path) when is_binary(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, polls} <- load_store(path) do
      Agent.start_link(fn -> %{path: path, polls: polls} end)
    end
  end

  def start_link(_), do: {:error, :invalid_path}

  @doc "Cria uma votação com pelo menos duas opções distintas."
  def create_poll(agent, id, question, options) do
    if valid_text?(id) and valid_text?(question) and valid_options?(options) do
      poll = %{id: id, question: question, options: options, votes: %{}}

      transact(agent, fn polls ->
        if Map.has_key?(polls, id) do
          {:error, :poll_already_exists}
        else
          {:ok, poll, Map.put(polls, id, poll)}
        end
      end)
    else
      {:error, :invalid_poll}
    end
  end

  @doc "Registra um voto. Cada eleitor pode votar uma única vez por votação."
  def cast_vote(agent, poll_id, voter_id, option)
      when is_binary(poll_id) and is_binary(voter_id) and is_binary(option) do
    if valid_text?(voter_id) do
      transact(agent, fn polls ->
        case Map.fetch(polls, poll_id) do
          :error ->
            {:error, :poll_not_found}

          {:ok, poll} ->
            cond do
              not (option in poll.options) ->
                {:error, :invalid_option}

              Map.has_key?(poll.votes, voter_id) ->
                {:error, :already_voted}

              true ->
                updated_poll = %{poll | votes: Map.put(poll.votes, voter_id, option)}
                {:ok, :vote_recorded, Map.put(polls, poll_id, updated_poll)}
            end
        end
      end)
    else
      {:error, :invalid_voter_id}
    end
  end

  def cast_vote(_agent, _poll_id, _voter_id, _option), do: {:error, :invalid_vote}

  @doc "Retorna os resultados da votação."
  def results(agent, poll_id) when is_binary(poll_id) do
    Agent.get(agent, fn %{polls: polls} ->
      case Map.fetch(polls, poll_id) do
        :error ->
          {:error, :poll_not_found}

        {:ok, poll} ->
          options =
            Enum.map(poll.options, fn option ->
              %{option: option, votes: Enum.count(Map.values(poll.votes), &(&1 == option))}
            end)

          {:ok,
           %{
             id: poll.id,
             question: poll.question,
             options: options,
             total_votes: map_size(poll.votes)
           }}
      end
    end)
  end

  def results(_agent, _poll_id), do: {:error, :invalid_poll_id}

  # A persistência acontece antes da troca do estado do Agent. Assim, uma falha
  # de gravação não deixa uma atualização parcial em memória.
  defp transact(agent, operation) do
    Agent.get_and_update(agent, fn %{path: path, polls: polls} = state ->
      case operation.(polls) do
        {:error, reason} ->
          {{:error, reason}, state}

        {:ok, result, updated_polls} ->
          case persist(path, updated_polls) do
            :ok ->
              {{:ok, result}, %{state | polls: updated_polls}}

            {:error, reason} ->
              {{:error, {:persistence, reason}}, state}
          end
      end
    end)
  end

  defp valid_options?(options) when is_list(options) and length(options) >= 2 do
    Enum.all?(options, &valid_text?/1) and length(Enum.uniq(options)) == length(options)
  end

  defp valid_options?(_), do: false

  defp valid_text?(value) when is_binary(value) do
    String.valid?(value) and String.trim(value) != ""
  end

  defp valid_text?(_), do: false

  defp load_store(path) do
    case File.read(path) do
      {:ok, contents} ->
        try do
          case :erlang.binary_to_term(contents, [:safe]) do
            %{version: @version, polls: polls} when is_map(polls) ->
              {:ok, polls}

            _ ->
              {:error, :invalid_store_format}
          end
        rescue
          _ -> {:error, :invalid_store_format}
        end

      {:error, :enoent} ->
        case persist(path, %{}) do
          :ok -> {:ok, %{}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # O arquivo temporário fica no mesmo diretório para que rename seja atômico.
  defp persist(path, polls) do
    try do
      contents = :erlang.term_to_binary(%{version: @version, polls: polls}, [:compressed])
      suffix = "#{System.system_time(:nanosecond)}.#{:erlang.unique_integer([:positive])}"
      temporary_path = path <> ".tmp." <> suffix

      result =
        case File.open(temporary_path, [:write, :binary, :exclusive]) do
          {:ok, file} ->
            write_result =
              with :ok <- IO.binwrite(file, contents),
                   :ok <- :file.sync(file) do
                :ok
              end

            close_result = File.close(file)

            case {write_result, close_result} do
              {:ok, :ok} -> File.rename(temporary_path, path)
              {{:error, _} = error, _} -> error
              {:ok, {:error, _} = error} -> error
            end

          {:error, reason} ->
            {:error, reason}
        end

      case result do
        :ok -> :ok
        {:error, reason} ->
          File.rm(temporary_path)
          {:error, reason}
      end
    rescue
      exception -> {:error, Exception.message(exception)}
    end
  end
end
