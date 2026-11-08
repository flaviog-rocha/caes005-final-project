# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Recuperar processos após falhas inesperadas.
# 3. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de apuração de votos online (Elixir, somente biblioteca padrão)
#
# Requisitos atendidos:
#   1. Processamento em streaming: o arquivo de votos é lido linha a linha
#      (File.stream!), agrupado em lotes (Stream.chunk_every) e descartado após
#      a contagem. Em memória ficam apenas o lote atual e o mapa de contagens
#      (um contador por candidato).
#   2. Recuperação após falhas: o estado de apuração (contagens + offset) é
#      persistido em checkpoint atômico (escrita em arquivo temporário + rename).
#      Um supervisor :rest_for_one reinicia os processos afetados, que retomam
#      do último checkpoint sem contar votos duas vezes.
#   3. Detecção de encerramentos inesperados: workers são monitorados com
#      spawn_monitor (com timeout), e um Watchdog monitora os processos principais
#      e registra motivo de cada encerramento.

defmodule VotingSystem do
  @moduledoc """
  API principal. Inicia a árvore de supervisão da apuração.
  """

  @doc """
  Inicia a árvore de supervisão.

  Opções:
    * `:votes_path` - arquivo com um voto (candidato) por linha (obrigatório)
    * `:checkpoint_path` - arquivo de checkpoint (padrão: "tally_checkpoint.bin")
    * `:batch_size` - tamanho do lote processado por vez (padrão: 10_000)
    * `:max_attempts` - tentativas por lote antes de falhar (padrão: 3)
    * `:batch_timeout` - tempo máximo por lote em ms (padrão: 30_000)
    * `:fail_probability` - probabilidade de falha injetada por lote, para testes
    * `:notify` - pid que recebe `{:ingestion_complete, counts, total}` ao final
  """
  def start(opts) do
    children = [
      # Ordem importa: Tally primeiro, pois o Ingestor depende dele.
      {VotingSystem.Tally, Keyword.take(opts, [:checkpoint_path])},
      {VotingSystem.Ingestor, opts},
      VotingSystem.Watchdog
    ]

    # rest_for_one: se o Tally cair, o Ingestor (que vem depois) também é reiniciado,
    # garantindo que ambos sejam reconciliados a partir do checkpoint.
    Supervisor.start_link(children,
      strategy: :rest_for_one,
      max_restarts: 20,
      max_seconds: 10,
      name: VotingSystem.RootSupervisor
    )
  end

  @doc "Retorna `{contagens, offset}` da apuração atual."
  def result, do: VotingSystem.Tally.snapshot()
end

defmodule VotingSystem.Tally do
  @moduledoc """
  Mantém as contagens por candidato e o offset (quantidade de linhas já processadas).
  Persiste o estado a cada lote, de forma atômica, para permitir recuperação.
  """
  use GenServer
  require Logger

  @default_path "tally_checkpoint.bin"

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Offset atual (número de votos já contabilizados)."
  def offset, do: GenServer.call(__MODULE__, :offset)

  @doc "Incorpora as contagens de um lote e persiste o checkpoint."
  def merge(batch_counts, new_offset) do
    GenServer.call(__MODULE__, {:merge, batch_counts, new_offset})
  end

  @doc "Retorna `{contagens, offset}`."
  def snapshot, do: GenServer.call(__MODULE__, :snapshot)

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :checkpoint_path, @default_path)
    {counts, offset} = load_checkpoint(path)
    Logger.info("Tally iniciado a partir do offset #{offset}")
    {:ok, %{path: path, counts: counts, offset: offset}}
  end

  @impl true
  def handle_call(:offset, _from, state), do: {:reply, state.offset, state}

  def handle_call(:snapshot, _from, state),
    do: {:reply, {state.counts, state.offset}, state}

  def handle_call({:merge, batch_counts, new_offset}, _from, state) do
    counts = Map.merge(state.counts, batch_counts, fn _candidate, a, b -> a + b end)
    new_state = %{state | counts: counts, offset: new_offset}
    :ok = persist(new_state)
    {:reply, :ok, new_state}
  end

  # Carrega o checkpoint; se inexistente ou corrompido, começa do zero.
  defp load_checkpoint(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          {counts, offset} = :erlang.binary_to_term(binary, [:safe])
          {counts, offset}
        rescue
          _ ->
            Logger.warning("Checkpoint corrompido, reiniciando apuração")
            {%{}, 0}
        end

      {:error, :enoent} ->
        {%{}, 0}
    end
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  # Uma falha no meio nunca deixa o checkpoint principal parcialmente escrito.
  defp persist(%{path: path, counts: counts, offset: offset}) do
    tmp = path <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary({counts, offset}))
    File.rename!(tmp, path)
    :ok
  end
end

defmodule VotingSystem.Ingestor do
  @moduledoc """
  Lê o arquivo de votos em streaming, processa lotes em workers monitorados
  e envia as contagens parciais ao Tally. Retoma do offset persistido.
  """
  use GenServer, restart: :transient
  require Logger

  @defaults [
    votes_path: nil,
    batch_size: 10_000,
    max_attempts: 3,
    batch_timeout: 30_000,
    fail_probability: 0.0,
    notify: nil
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    config = Keyword.merge(@defaults, opts) |> Map.new()
    {:ok, config, {:continue, :ingest}}
  end

  @impl true
  def handle_continue(:ingest, config) do
    # Retoma do ponto salvo. Stream.drop descarta as linhas já apuradas sem
    # carregá-las em memória.
    offset = VotingSystem.Tally.offset()
    Logger.info("Ingestão iniciada/retomada no offset #{offset}")

    final_offset =
      config.votes_path
      |> File.stream!()
      |> Stream.drop(offset)
      |> Stream.chunk_every(config.batch_size)
      |> Enum.reduce(offset, fn lines, acc ->
        counts = process_batch!(lines, config, 1)
        new_offset = acc + length(lines)
        :ok = VotingSystem.Tally.merge(counts, new_offset)
        new_offset
      end)

    {counts, _} = VotingSystem.Tally.snapshot()
    Logger.info("Ingestão concluída: #{final_offset} votos processados")

    if config.notify, do: send(config.notify, {:ingestion_complete, counts, final_offset})

    {:stop, :normal, config}
  end

  # Executa um lote em um processo monitorado. Em caso de falha ou timeout,
  # tenta novamente até `max_attempts`.
  defp process_batch!(lines, config, attempt) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        if config.fail_probability > 0 and :rand.uniform() < config.fail_probability do
          # Falha injetada para demonstrar recuperação.
          exit(:falha_injetada)
        end

        send(parent, {:batch_result, self(), count_votes(lines)})
      end)

    receive do
      {:batch_result, ^pid, counts} ->
        Process.demonitor(ref, [:flush])
        counts

      {:DOWN, ^ref, :process, ^pid, reason} ->
        retry_or_raise(lines, config, attempt, reason)
    after
      config.batch_timeout ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)
        retry_or_raise(lines, config, attempt, :timeout)
    end
  end

  defp retry_or_raise(lines, config, attempt, reason) do
    Logger.warning("Worker do lote falhou (tentativa #{attempt}): #{inspect(reason)}")

    if attempt < config.max_attempts do
      process_batch!(lines, config, attempt + 1)
    else
      raise "lote falhou após #{attempt} tentativas: #{inspect(reason)}"
    end
  end

  # Conta votos de um lote. Linhas vazias são ignoradas.
  defp count_votes(lines) do
    Enum.reduce(lines, %{}, fn line, acc ->
      case String.trim(line) do
        "" -> acc
        candidate -> Map.update(acc, candidate, 1, &(&1 + 1))
      end
    end)
  end
end

defmodule VotingSystem.Watchdog do
  @moduledoc """
  Monitora os processos principais e registra encerramentos inesperados,
  com o motivo do término. Reconecta-se aos processos após reinício pelo supervisor.
  """
  use GenServer
  require Logger

  @monitored [VotingSystem.Tally, VotingSystem.Ingestor]
  @retry_ms 100

  def start_link(_arg) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    refs = Enum.reduce(@monitored, %{}, &attach(&1, &2))
    {:ok, %{refs: refs}}
  end

  @impl true
  def handle_info({:attach, name}, state) do
    {:noreply, %{state | refs: attach(name, state.refs)}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    {name, refs} = Map.pop(state.refs, ref)

    cond do
      name == nil ->
        :ok

      reason == :normal ->
        Logger.info("#{inspect(name)} (#{inspect(pid)}) finalizou normalmente")

      true ->
        Logger.error(
          "#{inspect(name)} (#{inspect(pid)}) encerrou inesperadamente: #{inspect(reason)}. " <>
            "Aguardando reinício supervisionado."
        )

        Process.send_after(self(), {:attach, name}, @retry_ms)
    end

    {:noreply, %{state | refs: refs}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Monitora o processo registrado com `name`. Se ainda não existir, agenda nova tentativa.
  defp attach(name, refs) do
    case Process.whereis(name) do
      nil ->
        Process.send_after(self(), {:attach, name}, @retry_ms)
        refs

      pid ->
        ref = Process.monitor(pid)
        Map.put(refs, ref, name)
    end
  end
end

defmodule VotingSystem.Demo do
  @moduledoc """
  Demonstração: gera um arquivo grande de votos, injeta falhas nos workers
  e executa a apuração com recuperação automática.
  """

  @candidates ~w(alice bruno carla)

  def run(total_votes \\ 1_000_000) do
    path = "votos.txt"
    checkpoint = "tally_checkpoint.bin"

    # Começa do zero para a demonstração.
    File.rm(checkpoint)
    File.rm(checkpoint <> ".tmp")

    generate(path, total_votes)

    {:ok, _sup} =
      VotingSystem.start(
        votes_path: path,
        checkpoint_path: checkpoint,
        batch_size: 10_000,
        fail_probability: 0.05,
        notify: self()
      )

    receive do
      {:ingestion_complete, counts, total} ->
        IO.puts("\nApuração finalizada (#{total} votos):")

        counts
        |> Enum.sort_by(fn {_c, v} -> -v end)
        |> Enum.each(fn {candidate, votes} -> IO.puts("  #{candidate}: #{votes}") end)
    after
      600_000 -> IO.puts("Tempo esgotado aguardando a apuração")
    end
  end

  # Gera o arquivo de votos em streaming (não mantém a lista em memória).
  defp generate(path, total) do
    Stream.repeatedly(fn -> Enum.random(@candidates) <> "\n" end)
    |> Stream.take(total)
    |> Enum.into(File.stream!(path))
  end
end

VotingSystem.Demo.run()
