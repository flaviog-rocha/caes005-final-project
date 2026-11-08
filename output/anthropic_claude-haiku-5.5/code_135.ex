# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Recuperar processos após falhas inesperadas.
# 3. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online com processamento em streaming, recuperação e monitoramento.
# Somente biblioteca padrão do Elixir. Execução: elixir voting_system.exs

defmodule Voting.Store do
  @moduledoc """
  Armazena os votos de forma durável e idempotente.

  - Votos e progresso de importação ficam em :dets (disco), não em memória.
  - Apenas a contagem por candidato (poucos valores) fica em ETS. Ela é
    reconstruída por varredura incremental (`:dets.foldl/3`) ao reiniciar.
  - Um voto por eleitor é garantido pela chave do :dets.
  """
  use GenServer

  @name __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  @doc "Registra um voto individual."
  def vote(voter, candidate), do: GenServer.call(@name, {:vote, voter, candidate})

  @doc "Retorna a apuração atual como mapa %{candidato => votos}."
  def tally, do: GenServer.call(@name, :tally)

  @doc "Retorna o próximo índice de linha a ser processado para a fonte informada."
  def progress(source), do: GenServer.call(@name, {:progress, source})

  @doc "Confirma um lote de votos e o progresso da fonte em uma única operação."
  def commit_batch(source, next_offset, votes) do
    GenServer.call(@name, {:commit_batch, source, next_offset, votes}, :infinity)
  end

  # Callbacks

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :dir, ".")
    db_path = dir |> Path.join("voters.dets") |> String.to_charlist()

    # Reabre o arquivo. O :dets executa reparo automático após falhas.
    {:ok, db} = :dets.open_file(:voters_db, file: db_path, type: :set)
    tally = :ets.new(:tally, [:set, :private])

    # Reconstrói a apuração sem carregar todos os registros na memória
    :dets.foldl(
      fn
        {{:voter, _}, cand}, acc ->
          inc(tally, cand)
          acc

        _other, acc ->
          acc
      end,
      :ok,
      db
    )

    {:ok, %{db: db, tally: tally}}
  end

  @impl true
  def handle_call({:vote, voter, cand}, _from, s) do
    reply =
      case persist(s, [{voter, cand}], nil) do
        {:ok, 1} -> :ok
        {:ok, 0} -> {:error, :already_voted}
        {:error, _} = err -> err
      end

    {:reply, reply, s}
  end

  def handle_call({:commit_batch, source, next, votes}, _from, s) do
    {:reply, persist(s, votes, {{:progress, source}, next}), s}
  end

  def handle_call(:tally, _from, s) do
    {:reply, s.tally |> :ets.tab2list() |> Map.new(), s}
  end

  def handle_call({:progress, source}, _from, s) do
    value =
      case :dets.lookup(s.db, {:progress, source}) do
        [{_, offset}] -> offset
        [] -> 0
      end

    {:reply, value, s}
  end

  # Persiste somente votos novos (idempotência) e o progresso, de forma atômica no :dets
  defp persist(%{db: db, tally: tally}, votes, progress) do
    {new, _seen} =
      Enum.reduce(votes, {[], MapSet.new()}, fn {voter, cand}, {acc, seen} ->
        if MapSet.member?(seen, voter) or :dets.member(db, {:voter, voter}) do
          {acc, seen}
        else
          {[{{:voter, voter}, cand} | acc], MapSet.put(seen, voter)}
        end
      end)

    entries = if progress, do: [progress | new], else: new

    case :dets.insert(db, entries) do
      :ok ->
        :ok = :dets.sync(db)
        Enum.each(new, fn {_key, cand} -> inc(tally, cand) end)
        {:ok, length(new)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp inc(tally, cand), do: :ets.update_counter(tally, cand, 1, {cand, 0})
end

defmodule Voting.Watchdog do
  @moduledoc """
  Monitora o processo Voting.Store com Process.monitor/2 e registra
  encerramentos inesperados (motivo diferente de :normal/:shutdown).
  Após uma queda, o Supervisor reinicia o processo e o Watchdog passa
  a monitorar a nova instância assim que ela estiver registrada.
  """
  use GenServer

  @target Voting.Store
  @interval 100

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    send(self(), :watch)
    {:ok, %{pid: nil, ref: nil}}
  end

  @impl true
  def handle_info(:watch, s) do
    Process.send_after(self(), :watch, @interval)
    current = Process.whereis(@target)

    if current != nil and current != s.pid do
      ref = Process.monitor(current)
      {:noreply, %{s | pid: current, ref: ref}}
    else
      {:noreply, s}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %{ref: ref} = s) do
    if reason in [:normal, :shutdown] or match?({:shutdown, _}, reason) do
      IO.puts(:stderr, "[watchdog] #{inspect(@target)} encerrou normalmente")
    else
      IO.puts(:stderr, "[watchdog] ENCERRAMENTO INESPERADO de #{inspect(pid)}: #{inspect(reason)}")
    end

    {:noreply, %{s | pid: nil, ref: nil}}
  end

  def handle_info(_msg, s), do: {:noreply, s}
end

defmodule Voting.Importer do
  @moduledoc """
  Importa arquivos de votos ("eleitor,candidato") em streaming.

  - Lê linha a linha com File.stream!/1 e processa em lotes (chunk_every).
  - Cada lote é confirmado junto com o progresso; em caso de falha do worker,
    a importação é retomada da última linha confirmada.
  - O worker é monitorado; falhas inesperadas disparam nova tentativa com backoff.
  """
  @chunk_size 1_000
  @max_retries 5

  def import_file(path, opts \\ []) do
    source = Keyword.get(opts, :source, Path.basename(path))
    run(path, source, 0)
  end

  defp run(path, source, attempt) do
    {pid, ref} = spawn_monitor(fn -> process(path, source) end)

    receive do
      {:DOWN, ^ref, :process, ^pid, :normal} ->
        {:ok, :done}

      {:DOWN, ^ref, :process, ^pid, reason} when attempt < @max_retries ->
        IO.puts(
          :stderr,
          "[importer] worker #{inspect(pid)} terminou (#{inspect(reason)}); retomando..."
        )

        Process.sleep(100 * Integer.pow(2, attempt))
        run(path, source, attempt + 1)

      {:DOWN, ^ref, :process, ^pid, reason} ->
        {:error, reason}
    end
  end

  defp process(path, source) do
    start = Voting.Store.progress(source)

    path
    |> File.stream!()
    |> Stream.drop(start)
    |> Stream.with_index(start)
    |> Stream.chunk_every(@chunk_size)
    |> Enum.each(fn chunk ->
      votes = Enum.flat_map(chunk, fn {line, _idx} -> parse(line) end)
      {_line, last_idx} = List.last(chunk)
      {:ok, _accepted} = Voting.Store.commit_batch(source, last_idx + 1, votes)
    end)
  end

  defp parse(line) do
    case line |> String.trim() |> String.split(",", parts: 2) do
      [voter, cand] when voter != "" and cand != "" -> [{voter, cand}]
      _ -> []
    end
  end
end

defmodule Voting.Supervisor do
  @moduledoc """
  Supervisor :one_for_one: reinicia o Store em caso de falha e mantém o Watchdog ativo.
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Voting.Store, opts},
      Voting.Watchdog
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 30)
  end
end

defmodule Voting.Demo do
  @moduledoc "Demonstração: geração de arquivo grande, importação, falha simulada e reimportação."

  def run do
    dir = "voting_data"
    File.mkdir_p!(dir)
    csv = Path.join(dir, "votos.csv")

    {:ok, _sup} = Voting.Supervisor.start_link(dir: dir)

    generate(csv, 200_000)

    IO.puts("Importando votos em streaming...")
    {:ok, :done} = Voting.Importer.import_file(csv)
    IO.inspect(Voting.Store.tally(), label: "Apuração após importação")

    IO.puts("Simulando falha inesperada do Store...")
    Process.exit(Process.whereis(Voting.Store), :kill)
    Process.sleep(300)
    IO.inspect(Voting.Store.vote("eleitor_extra", "alice"), label: "Voto após recuperação")

    IO.puts("Reimportando com outra fonte (deduplicação deve impedir votos repetidos)...")
    {:ok, :done} = Voting.Importer.import_file(csv, source: "reimportacao")
    IO.inspect(Voting.Store.tally(), label: "Apuração final")
  end

  # Gera o arquivo de forma lazy, sem montar a lista inteira em memória
  defp generate(path, n) do
    candidates = ["alice", "bob", "carol", "dave"]

    1..n
    |> Stream.map(fn _ -> "eleitor#{:rand.uniform(n)},#{Enum.random(candidates)}\n" end)
    |> Stream.into(File.stream!(path))
    |> Stream.run()
  end
end

Voting.Demo.run()
