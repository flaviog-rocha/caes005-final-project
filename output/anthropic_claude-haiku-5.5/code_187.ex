# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# voting.exs — Sistema de votação online em Elixir (arquivo único, autocontido)
#
# Execução: elixir voting.exs
#
# Requisitos atendidos:
# 1. Comunicação entre processos: GenServer.call/send, Registry (localização por nome)
#    e pub/sub de eventos baseado em Registry com chaves duplicadas.
# 2. Entidades independentes: cada votação é um processo próprio, com tabela DETS
#    própria, supervisionado por um DynamicSupervisor e localizado via Registry.
# 3. Grandes volumes: ingestão em streaming (File.stream! + Stream.chunk_every).
#    Os votos individuais ficam em disco (DETS); em memória há apenas os contadores,
#    cujo tamanho é proporcional ao número de opções, não de eleitores.
# 4. Persistência: cada votação grava metadados e votos em um arquivo DETS.
# 5. Recuperação: supervisão com reinício automático; ao reiniciar, o estado é
#    reconstruído a partir do DETS (streaming via foldl) e as votações existentes
#    no disco são reativadas na inicialização do sistema.

defmodule Voting do
  @moduledoc """
  API pública do sistema de votação.
  """

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @doc "Sobe a árvore de supervisão e reativa todas as votações persistidas em disco."
  def start(opts \\ []) do
    dir = Keyword.get(opts, :data_dir, "data")
    File.mkdir_p!(dir)
    Application.put_env(:voting, :data_dir, dir)

    with {:ok, root} <- Voting.Root.start_link([]) do
      recovered = recover_polls()
      IO.puts("Votações recuperadas do disco: #{recovered}")
      {:ok, root}
    end
  end

  @doc "Reativa, a partir do disco, todas as votações que possuem arquivo DETS."
  def recover_polls do
    data_dir()
    |> Path.join("*.dets")
    |> Path.wildcard()
    |> Enum.map(&Path.basename(&1, ".dets"))
    |> Enum.filter(&valid_id?/1)
    |> Enum.count(fn id -> match?({:ok, _}, ensure_started(id)) end)
  end

  # ---------------------------------------------------------------------------
  # API de votação
  # ---------------------------------------------------------------------------

  @doc "Cria uma nova votação com as opções informadas."
  def create_poll(id, options) when is_binary(id) and is_list(options) do
    cond do
      not valid_id?(id) ->
        {:error, :invalid_id}

      options == [] or not Enum.all?(options, &is_binary/1) ->
        {:error, :invalid_options}

      File.exists?(poll_file(id)) ->
        {:error, :already_exists}

      true ->
        spec = {Voting.Poll, {id, {:create, options}}}

        case DynamicSupervisor.start_child(Voting.PollSupervisor, spec) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> {:error, :already_exists}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "Registra um voto de `voter` na opção `option`."
  def vote(id, voter, option), do: call(id, {:vote, voter, option})

  @doc "Registra um lote de votos `[{voter, option}, ...]` em uma única mensagem."
  def vote_batch(id, pairs) when is_list(pairs), do: call(id, {:vote_batch, pairs})

  @doc "Retorna a contagem atual de votos por opção."
  def results(id), do: call(id, :results)

  @doc "Inscreve o processo chamador para receber eventos da votação."
  def subscribe(id), do: Voting.Events.subscribe(id)

  # ---------------------------------------------------------------------------
  # Funções auxiliares internas (públicas apenas para os demais módulos)
  # ---------------------------------------------------------------------------

  @doc false
  def poll_file(id), do: Path.join(data_dir(), id <> ".dets")

  @doc false
  def ensure_started(id) when is_binary(id) do
    case Registry.lookup(Voting.Registry, {:poll, id}) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(Voting.PollSupervisor, {Voting.Poll, {id, :load}}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp call(id, msg) do
    if valid_id?(id) do
      with {:ok, pid} <- ensure_started(id) do
        GenServer.call(pid, msg)
      end
    else
      {:error, :invalid_id}
    end
  catch
    # Se o processo cair durante a chamada, o supervisor o reinicia; o chamador recebe erro.
    :exit, _ -> {:error, :unavailable}
  end

  defp valid_id?(id), do: is_binary(id) and Regex.match?(~r/^[A-Za-z0-9_-]{1,64}$/, id)

  defp data_dir, do: Application.get_env(:voting, :data_dir, "data")
end

# ---------------------------------------------------------------------------
# Árvore de supervisão raiz
# ---------------------------------------------------------------------------

defmodule Voting.Root do
  @moduledoc """
  Supervisor raiz: Registries (localização e pub/sub) e o supervisor dinâmico
  que hospeda um processo independente por votação.
  """
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Voting.Registry},
      {Registry, keys: :duplicate, name: Voting.PubSub},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor}
    ]

    # rest_for_one: se um Registry cair, os processos que dependem dele são reiniciados.
    Supervisor.init(children, strategy: :rest_for_one)
  end
end

# ---------------------------------------------------------------------------
# Processo de uma votação (entidade independente)
# ---------------------------------------------------------------------------

defmodule Voting.Poll do
  @moduledoc """
  Processo de uma votação. Cada votação é isolada: possui seu próprio processo,
  sua própria tabela DETS e seus próprios contadores. Os votos individuais
  permanecem em disco; a memória guarda somente as opções e os contadores.
  """
  use GenServer

  def start_link({id, mode}) do
    GenServer.start_link(__MODULE__, {id, mode}, name: via(id))
  end

  defp via(id), do: {:via, Registry, {Voting.Registry, {:poll, id}}}

  # -------------------------------------------------------------------------
  # Inicialização e recuperação
  # -------------------------------------------------------------------------

  @impl true
  def init({id, mode}) do
    # Permite que terminate/2 seja chamado para fechar o DETS de forma ordenada.
    Process.flag(:trap_exit, true)
    path = Voting.poll_file(id)

    if mode == :load and not File.exists?(path) do
      {:stop, :not_found}
    else
      open(id, path, mode)
    end
  end

  defp open(id, path, mode) do
    # O nome da tabela DETS precisa ser átomo; há um átomo por votação.
    table = :"poll_#{id}"

    case :dets.open_file(table, file: String.to_charlist(path), type: :set) do
      {:ok, tab} -> setup(id, tab, mode)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp setup(id, tab, mode) do
    case {:dets.lookup(tab, :meta), mode} do
      # Já existe votação persistida (criação original, reinício ou recuperação).
      {[{:meta, meta}], _} ->
        rebuild(id, tab, meta)

      # Criação de nova votação.
      {[], {:create, [_ | _] = options}} ->
        :ok = :dets.insert(tab, {:meta, %{options: options}})
        rebuild(id, tab, %{options: options})

      {[], _} ->
        :dets.close(tab)
        {:stop, :not_found}
    end
  end

  # Reconstrói os contadores varrendo os votos no disco em streaming (foldl),
  # sem carregar a tabela inteira na memória.
  defp rebuild(id, tab, %{options: options}) do
    zero = Map.new(options, &{&1, 0})

    counts =
      :dets.foldl(
        fn
          {{:voter, _voter}, option}, acc -> Map.update(acc, option, 1, &(&1 + 1))
          _other, acc -> acc
        end,
        zero,
        tab
      )

    {:ok, %{id: id, table: tab, options: options, counts: counts}}
  end

  # -------------------------------------------------------------------------
  # Chamadas
  # -------------------------------------------------------------------------

  @impl true
  def handle_call({:vote, voter, option}, _from, state) do
    case cast_vote(state, voter, option) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:vote_batch, pairs}, _from, state) do
    {new_state, result} =
      Enum.reduce(pairs, {state, %{accepted: 0, rejected: 0}}, fn {voter, option}, {st, acc} ->
        case cast_vote(st, voter, option) do
          {:ok, new_st} -> {new_st, %{acc | accepted: acc.accepted + 1}}
          {:error, _reason} -> {st, %{acc | rejected: acc.rejected + 1}}
        end
      end)

    {:reply, {:ok, result}, new_state}
  end

  def handle_call(:results, _from, state) do
    {:reply, {:ok, state.counts}, state}
  end

  # -------------------------------------------------------------------------
  # Regra de negócio
  # -------------------------------------------------------------------------

  defp cast_vote(state, voter, option) do
    cond do
      not is_binary(voter) or voter == "" ->
        {:error, :invalid_voter}

      option not in state.options ->
        {:error, :invalid_option}

      # insert_new é atômico: falha se o eleitor já votou.
      :dets.insert_new(state.table, {{:voter, voter}, option}) ->
        {:ok, bump(state, option)}

      true ->
        {:error, :already_voted}
    end
  end

  defp bump(state, option) do
    Voting.Events.publish(state.id, {:vote_counted, state.id, option})
    %{state | counts: Map.update!(state.counts, option, &(&1 + 1))}
  end

  # -------------------------------------------------------------------------
  # Encerramento ordenado
  # -------------------------------------------------------------------------

  @impl true
  def terminate(_reason, %{table: tab}) do
    :dets.close(tab)
    :ok
  end

  def terminate(_reason, _state), do: :ok
end

# ---------------------------------------------------------------------------
# Pub/sub entre processos
# ---------------------------------------------------------------------------

defmodule Voting.Events do
  @moduledoc "Comunicação assíncrona entre processos via Registry com chaves duplicadas."

  def subscribe(poll_id) do
    Registry.register(Voting.PubSub, {:poll, poll_id}, nil)
  end

  def publish(poll_id, event) do
    Registry.dispatch(Voting.PubSub, {:poll, poll_id}, fn subscribers ->
      for {pid, _} <- subscribers, do: send(pid, event)
    end)
  end
end

# ---------------------------------------------------------------------------
# Ingestão de grandes volumes em streaming
# ---------------------------------------------------------------------------

defmodule Voting.Ingest do
  @moduledoc """
  Importação de grandes volumes de votos a partir de um CSV no formato `eleitor,opcao`.
  O arquivo é lido linha a linha e enviado em lotes: nunca é carregado por inteiro.
  """

  def import_file(poll_id, path, chunk_size \\ 1_000) do
    path
    |> File.stream!()
    |> Stream.map(&parse_line/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.chunk_every(chunk_size)
    |> Enum.reduce(%{accepted: 0, rejected: 0}, fn batch, acc ->
      case Voting.vote_batch(poll_id, batch) do
        {:ok, %{accepted: a, rejected: r}} ->
          %{accepted: acc.accepted + a, rejected: acc.rejected + r}

        {:error, reason} ->
          raise "falha ao importar lote: #{inspect(reason)}"
      end
    end)
  end

  defp parse_line(line) do
    case line |> String.trim() |> String.split(",", parts: 2) do
      [voter, option] when voter != "" -> {voter, option}
      _ -> nil
    end
  end
end

# ---------------------------------------------------------------------------
# Demonstração: comunicação, ingestão, falha e recuperação
# ---------------------------------------------------------------------------

defmodule Voting.Demo do
  @moduledoc false

  @poll "eleicao-2025"
  @options ["Alice", "Bob", "Carol"]
  @dir "data_demo"

  def run do
    File.rm_rf!(@dir)
    {:ok, root} = Voting.start(data_dir: @dir)

    # 1. Criação de entidade independente
    :ok = Voting.create_poll(@poll, @options)

    # 2. Comunicação entre processos (pub/sub)
    {:ok, _} = Voting.subscribe(@poll)
    :ok = Voting.vote(@poll, "eleitor-0", "Alice")

    receive do
      {:vote_counted, id, option} -> IO.puts("Evento recebido: #{id} -> #{option}")
    after
      1_000 -> IO.puts("Nenhum evento recebido")
    end

    # 3. Ingestão de grande volume em streaming
    path = Path.join(@dir, "votos.csv")

    1..300_000
    |> Stream.map(fn i -> "eleitor-#{i},#{Enum.random(@options)}\n" end)
    |> Stream.into(File.stream!(path))
    |> Stream.run()

    IO.inspect(Voting.Ingest.import_file(@poll, path, 1_000), label: "importação")
    IO.inspect(Voting.results(@poll), label: "resultados")

    # 4. Falha inesperada do processo da votação
    [{pid, _}] = Registry.lookup(Voting.Registry, {:poll, @poll})
    Process.exit(pid, :kill)
    Process.sleep(100)
    IO.inspect(Voting.results(@poll), label: "após falha forçada")

    # 5. Reinicialização completa do sistema (estado reconstruído do disco)
    Supervisor.stop(root)
    {:ok, _root} = Voting.start(data_dir: @dir)
    IO.inspect(Voting.results(@poll), label: "após reinicialização")
  end
end

Voting.Demo.run()
