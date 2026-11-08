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
  Sistema de votação concorrente com persistência em arquivo.

  Cada operação de escrita é serializada pelo Agent e só altera o estado
  em memória depois que a nova versão foi gravada atomicamente em disco.
  """

  use Agent

  @type poll_id :: binary()
  @type option :: binary()
  @type voter_id :: binary()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, "voting_store.term")
    agent_opts = Keyword.take(opts, [:name])

    with true <- is_binary(path) and path != "",
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, data} <- load_store(path) do
      Agent.start_link(fn -> %{path: path, data: data} end, agent_opts)
    else
      false -> {:error, :invalid_path}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec create_poll(GenServer.server(), poll_id(), [option()]) :: {:ok, poll_id()} | {:error, term()}
  def create_poll(server, poll_id, options) do
    cond do
      not valid_id?(poll_id) ->
        {:error, :invalid_poll_id}

      not valid_options?(options) ->
        {:error, :invalid_options}

      true ->
        transact(server, fn data ->
          if Map.has_key?(data.polls, poll_id) do
            {:error, :poll_already_exists}
          else
            poll = %{options: options, votes: %{}}
            new_data = put_in(data, [:polls, poll_id], poll)
            {:ok, poll_id, new_data}
          end
        end)
    end
  end

  @spec vote(GenServer.server(), poll_id(), voter_id(), option()) :: {:ok, map()} | {:error, term()}
  def vote(server, poll_id, voter_id, option) do
    cond do
      not valid_id?(poll_id) ->
        {:error, :invalid_poll_id}

      not valid_id?(voter_id) ->
        {:error, :invalid_voter_id}

      not valid_id?(option) ->
        {:error, :invalid_option}

      true ->
        transact(server, fn data ->
          case Map.fetch(data.polls, poll_id) do
            :error ->
              {:error, :poll_not_found}

            {:ok, poll} ->
              cond do
                option not in poll.options ->
                  {:error, :invalid_option}

                Map.has_key?(poll.votes, voter_id) ->
                  {:error, :voter_already_voted}

                true ->
                  updated_poll = put_in(poll, [:votes, voter_id], option)
                  new_data = put_in(data, [:polls, poll_id], updated_poll)

                  {:ok,
                   %{poll_id: poll_id, voter_id: voter_id, option: option},
                   new_data}
              end
          end
        end)
    end
  end

  @spec results(GenServer.server(), poll_id()) :: {:ok, map()} | {:error, term()}
  def results(server, poll_id) do
    if valid_id?(poll_id) do
      Agent.get(server, fn %{data: data} ->
        case Map.fetch(data.polls, poll_id) do
          :error ->
            {:error, :poll_not_found}

          {:ok, poll} ->
            counts =
              Enum.reduce(poll.votes, Map.new(poll.options, &{&1, 0}), fn {_voter, option}, acc ->
                Map.update!(acc, option, &(&1 + 1))
              end)

            {:ok,
             %{
               poll_id: poll_id,
               options: poll.options,
               counts: counts,
               total_votes: map_size(poll.votes)
             }}
        end
      end)
    else
      {:error, :invalid_poll_id}
    end
  end

  @spec list_polls(GenServer.server()) :: [poll_id()]
  def list_polls(server) do
    Agent.get(server, fn %{data: data} -> Map.keys(data.polls) end)
  end

  defp transact(server, operation) do
    Agent.get_and_update(server, fn %{path: path, data: data} = state ->
      case operation.(data) do
        {:error, reason} ->
          {{:error, reason}, state}

        {:ok, result, new_data} ->
          case persist_store(path, new_data) do
            :ok -> {{:ok, result}, %{state | data: new_data}}
            {:error, reason} -> {{:error, {:persistence_failed, reason}}, state}
          end
      end
    end)
  end

  defp load_store(path) do
    case File.read(path) do
      {:ok, binary} ->
        decode_store(binary)

      {:error, :enoent} ->
        {:ok, %{polls: %{}}}

      {:error, reason} ->
        {:error, {:read_failed, reason}}
    end
  end

  defp decode_store(binary) do
    try do
      case :erlang.binary_to_term(binary, [:safe]) do
        %{polls: polls} = data when is_map(polls) ->
          if valid_store?(data), do: {:ok, data}, else: {:error, :invalid_store}

        _ ->
          {:error, :invalid_store}
      end
    rescue
      ArgumentError -> {:error, :invalid_store}
    catch
      _, _ -> {:error, :invalid_store}
    end
  end

  defp valid_store?(%{polls: polls}) do
    Enum.all?(polls, fn
      {poll_id, %{options: options, votes: votes}} ->
        valid_id?(poll_id) and valid_options?(options) and is_map(votes) and
          Enum.all?(votes, fn {voter_id, option} ->
            valid_id?(voter_id) and option in options
          end)

      _ ->
        false
    end)
  end

  defp persist_store(path, data) do
    directory = Path.dirname(path)
    temporary_path =
      Path.join(
        directory,
        ".#{Path.basename(path)}.#{System.unique_integer([:positive, :monotonic])}.tmp"
      )

    result =
      with :ok <- File.write(temporary_path, :erlang.term_to_binary(data), [:binary]),
           :ok <- sync_file(temporary_path),
           :ok <- File.rename(temporary_path, path) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end

  defp sync_file(path) do
    with {:ok, file} <- File.open(path, [:read, :write]) do
      try do
        :file.sync(file)
      after
        File.close(file)
      end
    end
  end

  defp valid_id?(value), do: is_binary(value) and byte_size(value) > 0

  defp valid_options?(options) do
    is_list(options) and options != [] and
      Enum.all?(options, &valid_id?/1) and
      length(options) == length(Enum.uniq(options))
  end
end
