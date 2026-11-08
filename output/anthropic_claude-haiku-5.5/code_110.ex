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
# Executar com: elixir voting_system.exs

defmodule VotingSystem.Storage do
  @moduledoc false
  # Persistência em arquivo binário. A escrita é atômica: grava-se em um
  # arquivo temporário, força a sincronização com o disco e só então
  # renomeia para o arquivo definitivo. Uma falha no meio do caminho
  # nunca deixa o arquivo principal corrompido.

  @doc "Carrega o mapa de enquetes do disco. Retorna %{} se o arquivo não existir."
  def load(path) do
    case File.read(path) do
      {:ok, bin} -> {:ok, :erlang.binary_to_term(bin, [:safe])}
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Persiste de forma atômica o mapa de enquetes."
  def save(path, polls) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(polls)

    with {:ok, io} <- File.open(tmp, [:write, :binary]) do
      # Garante o fechamento do descritor mesmo em caso de erro na escrita.
      written =
        with :ok <- IO.binwrite(io, bin),
             :ok <- :file.sync(io),
             do: :ok

      File.close(io)

      with :ok <- written, do: File.rename(tmp, path)
    end
  end
end

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação concorrente.

  * Todo o estado vive em um `Agent`, que serializa as mutações.
  * Cada operação de escrita é transacional: a função pura calcula o novo
    estado completo; só se ele for válido e persistido com sucesso é que
    substitui o estado anterior. Caso contrário, nada muda (sem
    atualizações parciais).
  * Leituras (`results/2`, `list_polls/1`) são snapshots consistentes.
  * Os dados são gravados em disco a cada mutação e recarregados na
    inicialização, sobrevivendo a reinicializações.
  * Não há dependência de Ecto: o "banco" é um arquivo binário atômico.
  """

  alias VotingSystem.Storage

  @default_path "voting_state.bin"

  # ---------------------------------------------------------------------------
  # Ciclo de vida
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o sistema.

  Opções:
    * `:name` - nome registrado do Agent (padrão: `VotingSystem`)
    * `:path` - caminho do arquivo de persistência
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    path = Keyword.get(opts, :path, @default_path)

    Agent.start_link(fn -> init_state(path) end, name: name)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  # Carrega o estado persistido. Se o arquivo estiver ilegível, o processo
  # falha em vez de iniciar com dados vazios (evita sobrescrever dados reais).
  defp init_state(path) do
    case Storage.load(path) do
      {:ok, polls} -> %{path: path, polls: polls}
      {:error, reason} -> raise "falha ao carregar estado: #{inspect(reason)}"
    end
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cria uma enquete com pelo menos duas opções distintas."
  def create_poll(server \\ __MODULE__, poll_id, title, options) do
    transact(server, fn polls ->
      cond do
        Map.has_key?(polls, poll_id) ->
          {:error, :already_exists}

        not valid_options?(options) ->
          {:error, :invalid_options}

        true ->
          poll = %{
            id: poll_id,
            title: title,
            options: options,
            status: :open,
            votes: %{}
          }

          {:ok, poll_id, Map.put(polls, poll_id, poll)}
      end
    end)
  end

  @doc """
  Registra o voto de um eleitor. Um eleitor só pode votar uma vez por
  enquete, em uma opção válida, enquanto a enquete estiver aberta.
  """
  def vote(server \\ __MODULE__, poll_id, voter_id, option) do
    transact(server, fn polls ->
      with {:ok, poll} <- fetch_poll(polls, poll_id),
           :ok <- ensure_open(poll),
           :ok <- ensure_option(poll, option),
           :ok <- ensure_not_voted(poll, voter_id) do
        updated = %{poll | votes: Map.put(poll.votes, voter_id, option)}
        {:ok, :registered, Map.put(polls, poll_id, updated)}
      end
    end)
  end

  @doc "Encerra uma enquete. Após encerrada, não aceita mais votos."
  def close_poll(server \\ __MODULE__, poll_id) do
    transact(server, fn polls ->
      with {:ok, poll} <- fetch_poll(polls, poll_id),
           :ok <- ensure_open(poll) do
        {:ok, :closed, Map.put(polls, poll_id, %{poll | status: :closed})}
      end
    end)
  end

  @doc "Retorna a contagem de votos por opção. Leitura consistente (snapshot)."
  def results(server \\ __MODULE__, poll_id) do
    Agent.get(server, fn %{polls: polls} ->
      with {:ok, poll} <- fetch_poll(polls, poll_id) do
        {:ok, tally(poll)}
      end
    end)
  end

  @doc "Lista as enquetes existentes com seu status."
  def list_polls(server \\ __MODULE__) do
    Agent.get(server, fn %{polls: polls} ->
      polls
      |> Map.values()
      |> Enum.map(&Map.take(&1, [:id, :title, :status]))
    end)
  end

  # ---------------------------------------------------------------------------
  # Núcleo transacional
  # ---------------------------------------------------------------------------

  # Executa `fun` dentro do Agent, de forma atômica.
  #   * `fun` recebe o mapa de enquetes atual e deve retornar
  #     `{:ok, reply, novo_estado}` ou `{:error, reason}`.
  #   * O novo estado só é aplicado se a persistência em disco tiver sucesso.
  #   * Como o Agent processa uma mensagem por vez, não há intercalação
  #     entre transações (consistência sob concorrência).
  defp transact(server, fun) do
    Agent.get_and_update(
      server,
      fn %{path: path, polls: polls} = state ->
        case fun.(polls) do
          {:ok, reply, new_polls} ->
            case Storage.save(path, new_polls) do
              :ok ->
                {{:ok, reply}, %{state | polls: new_polls}}

              {:error, reason} ->
                # Falha de persistência: mantém o estado anterior intacto.
                {{:error, {:persist_failed, reason}}, state}
            end

          {:error, _reason} = error ->
            {error, state}
        end
      end,
      :infinity
    )
  end

  # ---------------------------------------------------------------------------
  # Validações e funções puras
  # ---------------------------------------------------------------------------

  defp fetch_poll(polls, poll_id) do
    case Map.fetch(polls, poll_id) do
      {:ok, poll} -> {:ok, poll}
      :error -> {:error, :not_found}
    end
  end

  defp ensure_open(%{status: :open}), do: :ok
  defp ensure_open(_poll), do: {:error, :poll_closed}

  defp ensure_option(%{options: options}, option) do
    if option in options, do: :ok, else: {:error, :invalid_option}
  end

  defp ensure_not_voted(%{votes: votes}, voter_id) do
    if Map.has_key?(votes, voter_id), do: {:error, :already_voted}, else: :ok
  end

  defp valid_options?(options) do
    is_list(options) and length(options) >= 2 and
      length(Enum.uniq(options)) == length(options)
  end

  # Contagem derivada dos votos: não existe um contador separado que
  # possa ficar inconsistente com a lista de votos.
  defp tally(%{options: options, votes: votes}) do
    frequencies = Enum.frequencies(Map.values(votes))
    Map.new(options, fn opt -> {opt, Map.get(frequencies, opt, 0)} end)
  end
end

defmodule VotingSystem.Demo do
  @moduledoc false
  # Demonstração: operações concorrentes, rejeição de duplicatas,
  # encerramento de enquete e recuperação após reinicialização.

  def run do
    path = Path.join(System.tmp_dir!(), "voting_demo_#{System.unique_integer([:positive])}.bin")

    {:ok, _pid} = VotingSystem.start_link(path: path)
    {:ok, "eleicao-1"} = VotingSystem.create_poll("eleicao-1", "Melhor linguagem", ["Elixir", "Erlang", "Go"])

    # 300 eleitores votando simultaneamente.
    options = ["Elixir", "Erlang", "Go"]

    1..300
    |> Enum.map(fn i ->
      Task.async(fn ->
        VotingSystem.vote("eleicao-1", "eleitor-#{i}", Enum.random(options))
      end)
    end)
    |> Task.await_many()

    # Tentativa de voto duplicado e em opção inválida.
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-1", "Go"), label: "duplicado")
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-999", "Rust"), label: "opção inválida")

    {:ok, tally} = VotingSystem.results("eleicao-1")
    IO.inspect(tally, label: "apuração antes do reinício")
    IO.inspect(Enum.sum(Map.values(tally)), label: "total de votos válidos")

    # Encerramento e tentativa de voto posterior.
    {:ok, :closed} = VotingSystem.close_poll("eleicao-1")
    IO.inspect(VotingSystem.vote("eleicao-1", "eleitor-atrasado", "Elixir"), label: "após encerramento")

    # Simula reinicialização: para o Agent e sobe novamente a partir do disco.
    :ok = Agent.stop(VotingSystem)
    {:ok, _pid} = VotingSystem.start_link(path: path)

    {:ok, recovered} = VotingSystem.results("eleicao-1")
    IO.inspect(recovered, label: "apuração após reinício")
    IO.inspect(recovered == tally, label: "dados preservados")

    IO.inspect(VotingSystem.list_polls(), label: "enquetes")

    Agent.stop(VotingSystem)
    File.rm(path)
    File.rm(path <> ".tmp")
    :ok
  end
end

VotingSystem.Demo.run()
