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

# voting.ex
# Sistema de votação online com:
#   1. Comunicação entre processos: mensagens GenServer e Registry (pub/sub) para assinantes.
#   2. Entidades independentes: cada enquete é um processo próprio, supervisionado.
#   3. Grandes volumes: log append-only lido por Stream (registro a registro) e
#      índice de eleitores em DETS (armazenamento em disco), sem carregar tudo na memória.
#   4. Persistência: log binário em disco, com fsync a cada voto.
#   5. Recuperação: supervisores reiniciam processos que falham; o estado é reconstruído
#      a partir do log. O Manager também reidrata todas as enquetes ao iniciar.
#
# Execute com: elixir voting.ex

defmodule Voting.Store do
  @moduledoc """
  Log append-only com registros enquadrados: <<tamanho::32, termo_binario::binary>>.
  A leitura é feita via Stream.resource, um registro por vez, sem carregar o arquivo inteiro.
  """

  # Serializa um termo Erlang em um quadro com cabeçalho de tamanho.
  def frame(term) do
    bin = :erlang.term_to_binary(term)
    <<byte_size(bin)::32, bin::binary>>
  end

  # Anexa um registro ao log e força a escrita em disco (fsync).
  def append(path, term) do
    {:ok, io} = File.open(path, [:append, :binary])

    try do
      :ok = IO.binwrite(io, frame(term))
      :ok = :file.sync(io)
    after
      File.close(io)
    end
  end

  # Emite {termo, bytes_ocupados} para cada registro válido.
  # Um registro parcial no fim do arquivo (queda durante escrita) encerra o stream.
  def stream(path) do
    Stream.resource(
      fn -> if File.exists?(path), do: File.open!(path, [:read, :binary]), else: nil end,
      fn
        nil ->
          {:halt, nil}

        io ->
          with <<len::32>> <- IO.binread(io, 4),
               bin when is_binary(bin) and byte_size(bin) == len <- IO.binread(io, len) do
            {[{:erlang.binary_to_term(bin, [:safe]), 4 + len}], io}
          else
            _ -> {:halt, io}
          end
      end,
      fn
        nil -> :ok
        io -> File.close(io)
      end
    )
  end
end

defmodule Voting.Poll do
  @moduledoc """
  Processo de uma enquete. Cada enquete é independente: falhas ou carga em uma não
  afetam as demais. O estado (título, opções e contagens) é reconstruído do log na
  inicialização, o que torna a recuperação após falhas automática.
  """
  use GenServer

  @registry Voting.PollRegistry
  @pubsub Voting.PubSub

  # API pública

  def start_link({id, dir}) do
    GenServer.start_link(__MODULE__, {id, dir}, name: via(id))
  end

  def vote(id, voter, option), do: GenServer.call(via(id), {:vote, voter, option})

  def results(id), do: GenServer.call(via(id), :results)

  # Assinantes recebem {:voting, id, contagens} a cada voto aceito.
  def subscribe(id), do: Registry.register(@pubsub, id, nil)

  defp via(id), do: {:via, Registry, {@registry, id}}

  # Inicialização e recuperação

  @impl true
  def init({id, dir}) do
    path = Path.join(dir, "poll_#{id}.log")
    tab = table_name(id)

    # O índice DETS fica em disco, então o conjunto de eleitores não consome memória.
    # Ele é reconstruído a partir do log, que é a fonte da verdade.
    {:ok, ^tab} =
      :dets.open_file(tab,
        file: String.to_charlist(Path.join(dir, "poll_#{id}.dets")),
        type: :set
      )

    :ok = :dets.delete_all_objects(tab)

    {meta, counts, valid_bytes} = replay(tab, path)

    if meta == nil, do: raise(ArgumentError, "log sem definição da enquete: #{path}")

    # Remove eventual registro parcial deixado por uma queda, para não corromper novos anexos.
    if File.stat!(path).size > valid_bytes do
      {:ok, fd} = File.open(path, [:read, :write, :binary])
      {:ok, _} = :file.position(fd, valid_bytes)
      :ok = :file.truncate(fd)
      File.close(fd)
    end

    {:ok, io} = File.open(path, [:append, :binary])

    state = %{
      id: id,
      tab: tab,
      io: io,
      title: meta.title,
      options: meta.options,
      counts: Map.merge(Map.new(meta.options, &{&1, 0}), counts)
    }

    {:ok, state}
  end

  # Varre o log em streaming, acumulando apenas o necessário para o estado.
  defp replay(tab, path) do
    path
    |> Voting.Store.stream()
    |> Enum.reduce({nil, %{}, 0}, fn {record, size}, {meta, counts, offset} ->
      offset = offset + size

      case record do
        {:created, title, options} ->
          {%{title: title, options: options}, counts, offset}

        {:vote, voter, option} ->
          :ok = :dets.insert(tab, {voter})
          {meta, Map.update(counts, option, 1, &(&1 + 1)), offset}
      end
    end)
  end

  defp table_name(id), do: :"voters_#{id}"

  # Callbacks

  @impl true
  def handle_call({:vote, voter, option}, _from, s) do
    cond do
      option not in s.options ->
        {:reply, {:error, :invalid_option}, s}

      :dets.member(s.tab, voter) ->
        {:reply, {:error, :already_voted}, s}

      true ->
        # 1) Persiste no log antes de qualquer efeito colateral.
        :ok = IO.binwrite(s.io, Voting.Store.frame({:vote, voter, option}))
        :ok = :file.sync(s.io)

        # 2) Atualiza o índice de eleitores em disco.
        :ok = :dets.insert(s.tab, {voter})

        # 3) Atualiza o estado e notifica os assinantes.
        counts = Map.update!(s.counts, option, &(&1 + 1))

        Registry.dispatch(@pubsub, s.id, fn subs ->
          for {pid, _} <- subs, do: send(pid, {:voting, s.id, counts})
        end)

        {:reply, :ok, %{s | counts: counts}}
    end
  end

  def handle_call(:results, _from, s) do
    {:reply, {:ok, %{title: s.title, counts: s.counts}}, s}
  end
end

defmodule Voting.Manager do
  @moduledoc """
  Cria enquetes, persiste sua definição e reidrata todas as enquetes existentes
  em disco ao iniciar, o que cobre reinicializações completas do sistema.
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :data_dir), name: __MODULE__)
  end

  def create_poll(title, options), do: GenServer.call(__MODULE__, {:create, title, options})

  @impl true
  def init(dir) do
    File.mkdir_p!(dir)
    restore(dir)
    {:ok, %{dir: dir}}
  end

  @impl true
  def handle_call({:create, title, options}, _from, %{dir: dir} = s) do
    id = new_id()
    # A definição é gravada primeiro, para que a enquete sobreviva a qualquer queda.
    Voting.Store.append(Path.join(dir, "poll_#{id}.log"), {:created, title, options})

    case DynamicSupervisor.start_child(Voting.PollSupervisor, {Voting.Poll, {id, dir}}) do
      {:ok, _pid} -> {:reply, {:ok, id}, s}
      error -> {:reply, error, s}
    end
  end

  # Reinicia um processo para cada log encontrado. Processos já ativos são ignorados.
  defp restore(dir) do
    dir
    |> Path.join("poll_*.log")
    |> Path.wildcard()
    |> Enum.each(fn path ->
      id = path |> Path.basename(".log") |> String.replace_prefix("poll_", "")
      DynamicSupervisor.start_child(Voting.PollSupervisor, {Voting.Poll, {id, dir}})
    end)
  end

  defp new_id do
    "#{System.system_time(:microsecond)}_#{System.unique_integer([:positive])}"
  end
end

defmodule Voting do
  @moduledoc """
  Árvore de supervisão e API pública do sistema de votação.
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: Voting.PollRegistry},
      {Registry, keys: :duplicate, name: Voting.PubSub},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor},
      {Voting.Manager, data_dir: Keyword.get(opts, :data_dir, "dados_votacao")}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defdelegate create_poll(title, options), to: Voting.Manager
  defdelegate vote(id, voter, option), to: Voting.Poll
  defdelegate results(id), to: Voting.Poll
  defdelegate subscribe(id), to: Voting.Poll
end

# ---------------------------------------------------------------------------
# Demonstração (executada ao rodar: elixir voting.ex)
# ---------------------------------------------------------------------------

{:ok, _sup} = Voting.start_link(data_dir: "dados_votacao")

{:ok, poll} = Voting.create_poll("Linguagem favorita", ["Elixir", "Erlang", "Gleam"])
{:ok, _} = Voting.subscribe(poll)

:ok = Voting.vote(poll, "eleitor-1", "Elixir")
{:error, :already_voted} = Voting.vote(poll, "eleitor-1", "Erlang")
:ok = Voting.vote(poll, "eleitor-2", "Gleam")

receive do
  {:voting, _id, counts} -> IO.inspect(counts, label: "notificação em tempo real")
after
  1_000 -> IO.puts("sem notificação")
end

# Simula uma falha inesperada do processo da enquete.
[{pid, _}] = Registry.lookup(Voting.PollRegistry, poll)
Process.exit(pid, :kill)
Process.sleep(100)

# O supervisor reiniciou o processo, que reconstruiu o estado a partir do log.
IO.inspect(Voting.results(poll), label: "resultados após recuperação")
