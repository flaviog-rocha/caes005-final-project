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

# Sistema de votação online com Agent, persistência em disco e atualizações atômicas.
# Arquivo único, usando apenas a biblioteca padrão do Elixir/OTP.
#
# Garantias:
# - Atualizações atômicas: cada operação calcula o novo estado por completo antes de
#   persistir. Se a validação falhar ou a persistência falhar, o estado anterior é mantido.
# - Operações concorrentes: o Agent serializa as mutações, então dois processos nunca
#   veem ou produzem um estado intermediário.
# - Consistência: um eleitor vota uma única vez por enquete, apenas em enquetes abertas
#   e apenas em opções válidas.
# - Persistência: o estado é gravado em arquivo temporário, sincronizado com o disco e
#   então renomeado sobre o arquivo final (rename é atômico em sistemas POSIX). Na
#   inicialização, o estado é recarregado do disco.

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação com Agent como estado concorrente e persistência em arquivo,
  funcionando como um "banco de dados" fictício.

  Exemplo de uso:

      {:ok, _pid} = VotingSystem.start_link(name: :votacao, path: "votacao.state")

      :ok = VotingSystem.create_poll(:votacao, "eleicao_1", "Qual linguagem?", ["Elixir", "Erlang"])
      :ok = VotingSystem.vote(:votacao, "eleicao_1", "eleitor_42", "Elixir")
      {:ok, resultado} = VotingSystem.results(:votacao, "eleicao_1")
  """

  use Agent

  @format_version 1
  @default_path "voting_system.state"

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o Agent. Opções:
    * `:name` - nome registrado do processo (padrão: `#{inspect(__MODULE__)}`)
    * `:path` - caminho do arquivo de persistência (padrão: `#{@default_path}`)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    path = Keyword.get(opts, :path, @default_path)

    # O estado é carregado do disco dentro do processo do Agent, na inicialização.
    Agent.start_link(fn -> load(path) end, name: name)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cria uma enquete aberta com ao menos duas opções distintas."
  def create_poll(server, poll_id, title, options)
      when is_binary(poll_id) and is_binary(title) and is_list(options) do
    transact(server, fn polls ->
      cond do
        Map.has_key?(polls, poll_id) ->
          {:error, :poll_already_exists}

        not valid_options?(options) ->
          {:error, :invalid_options}

        true ->
          poll = %{title: title, options: options, status: :open, votes: %{}}
          {:ok, Map.put(polls, poll_id, poll)}
      end
    end)
  end

  @doc "Registra um voto. Um eleitor só pode votar uma vez por enquete."
  def vote(server, poll_id, voter_id, option) do
    transact(server, fn polls ->
      apply_vote(polls, poll_id, voter_id, option)
    end)
  end

  @doc """
  Registra vários votos em uma única operação atômica (tudo ou nada).
  Recebe uma lista de tuplas `{poll_id, voter_id, option}`.
  """
  def cast_votes(server, votes) when is_list(votes) do
    transact(server, fn polls ->
      Enum.reduce_while(votes, {:ok, polls}, fn {poll_id, voter_id, option}, {:ok, acc} ->
        case apply_vote(acc, poll_id, voter_id, option) do
          {:ok, new_acc} -> {:cont, {:ok, new_acc}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end)
  end

  @doc "Encerra uma enquete, impedindo novos votos."
  def close_poll(server, poll_id) do
    transact(server, fn polls ->
      with {:ok, poll} <- fetch_poll(polls, poll_id),
           :ok <- ensure_open(poll) do
        {:ok, Map.put(polls, poll_id, %{poll | status: :closed})}
      end
    end)
  end

  @doc "Retorna o resultado (contagem por opção) de uma enquete."
  def results(server, poll_id) do
    # Leitura feita dentro do Agent: é um snapshot consistente do estado.
    Agent.get(server, fn %{polls: polls} ->
      with {:ok, poll} <- fetch_poll(polls, poll_id) do
        frequencies = Enum.frequencies(Map.values(poll.votes))

        counts =
          Enum.map(poll.options, fn option -> {option, Map.get(frequencies, option, 0)} end)

        {:ok,
         %{
           title: poll.title,
           status: poll.status,
           total_votes: map_size(poll.votes),
           counts: counts
         }}
      end
    end)
  end

  @doc "Lista os identificadores de todas as enquetes."
  def list_polls(server) do
    Agent.get(server, fn %{polls: polls} -> Map.keys(polls) end)
  end

  # ---------------------------------------------------------------------------
  # Núcleo transacional
  # ---------------------------------------------------------------------------

  # Executa `fun` sobre as enquetes atuais dentro do Agent.
  # `fun` deve ser puro: recebe os `polls` e retorna {:ok, novos_polls} ou {:error, motivo}.
  # O novo estado só substitui o antigo se a persistência em disco for bem-sucedida.
  defp transact(server, fun) do
    Agent.get_and_update(server, fn state ->
      case fun.(state.polls) do
        {:ok, new_polls} ->
          new_state = %{state | polls: new_polls}

          case persist(new_state) do
            :ok -> {:ok, new_state}
            # Persistência falhou: mantém o estado anterior (sem atualização parcial).
            {:error, reason} -> {{:error, {:persistence_failed, reason}}, state}
          end

        {:error, _reason} = error ->
          {error, state}
      end
    end)
  end

  defp apply_vote(polls, poll_id, voter_id, option) do
    with {:ok, poll} <- fetch_poll(polls, poll_id),
         :ok <- ensure_open(poll),
         :ok <- ensure_valid_option(poll, option),
         :ok <- ensure_not_voted(poll, voter_id) do
      updated = %{poll | votes: Map.put(poll.votes, voter_id, option)}
      {:ok, Map.put(polls, poll_id, updated)}
    end
  end

  # ---------------------------------------------------------------------------
  # Validações
  # ---------------------------------------------------------------------------

  defp fetch_poll(polls, poll_id) do
    case Map.fetch(polls, poll_id) do
      {:ok, poll} -> {:ok, poll}
      :error -> {:error, :poll_not_found}
    end
  end

  defp ensure_open(%{status: :open}), do: :ok
  defp ensure_open(_poll), do: {:error, :poll_closed}

  defp ensure_valid_option(%{options: options}, option) do
    if option in options, do: :ok, else: {:error, :invalid_option}
  end

  defp ensure_not_voted(%{votes: votes}, voter_id) do
    if Map.has_key?(votes, voter_id), do: {:error, :already_voted}, else: :ok
  end

  defp valid_options?(options) do
    length(options) >= 2 and
      Enum.all?(options, &(is_binary(&1) and &1 != "")) and
      length(Enum.uniq(options)) == length(options)
  end

  # ---------------------------------------------------------------------------
  # Persistência (banco de dados fictício baseado em arquivo)
  # ---------------------------------------------------------------------------

  # Carrega o estado do disco. Se o arquivo não existir, começa vazio.
  # Se estiver corrompido, falha explicitamente para evitar perda silenciosa de dados.
  defp load(path) do
    polls =
      case File.read(path) do
        {:ok, binary} ->
          case safe_decode(binary) do
            {:ok, %{version: @format_version, polls: polls}} ->
              polls

            _ ->
              raise ArgumentError,
                    "arquivo de estado corrompido ou versão incompatível: #{path}"
          end

        {:error, :enoent} ->
          %{}

        {:error, reason} ->
          raise "não foi possível ler #{path}: #{inspect(reason)}"
      end

    %{path: path, polls: polls}
  end

  defp safe_decode(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # Escrita atômica: grava em arquivo temporário, sincroniza com o disco e
  # renomeia sobre o arquivo definitivo. Uma queda no meio não corrompe o original.
  defp persist(%{path: path, polls: polls}) do
    tmp_path = path <> ".tmp"
    binary = :erlang.term_to_binary(%{version: @format_version, polls: polls})

    with {:ok, :ok} <- File.open(tmp_path, [:write, :binary], &write_and_sync(&1, binary)),
         :ok <- File.rename(tmp_path, path) do
      :ok
    else
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_and_sync(device, binary) do
    with :ok <- IO.binwrite(device, binary) do
      :file.sync(device)
    end
  end
end
