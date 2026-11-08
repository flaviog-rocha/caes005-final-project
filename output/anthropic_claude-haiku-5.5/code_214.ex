# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Permitir comunicação entre diferentes processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# csv_pipeline.exs
# Executar com: elixir csv_pipeline.exs
# Requisitos: apenas a biblioteca padrão do Elixir (OTP).

defmodule CsvPipeline.Parser do
  @moduledoc """
  Funções puras de parsing e validação de conteúdo CSV.

  Contrato de erros: qualquer problema nos DADOS do arquivo é retornado como
  `{:error, {:validation, detalhe}}`. Esses erros são determinísticos, portanto
  não são reexecutados.
  """

  @header ["id", "nome", "valor"]

  @spec parse(binary()) :: {:ok, [map()]} | {:error, {:validation, term()}}
  def parse(content) when is_binary(content) do
    # Aceita quebras de linha \n e \r\n e ignora linhas vazias
    case String.split(content, ["\r\n", "\n"], trim: true) do
      [] ->
        {:error, {:validation, :empty_file}}

      [header | lines] ->
        if split(header) == @header do
          validate_lines(lines)
        else
          {:error, {:validation, {:invalid_header, split(header)}}}
        end
    end
  end

  # Reduz as linhas interrompendo na primeira inválida
  defp validate_lines(lines) do
    lines
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, []}, fn {line, num}, {:ok, acc} ->
      case parse_row(split(line), num) do
        {:ok, row} -> {:cont, {:ok, [row | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp parse_row([id, nome, valor], num) do
    with {:ok, id} <- parse_id(id, num),
         {:ok, nome} <- parse_nome(nome, num),
         {:ok, valor} <- parse_valor(valor, num) do
      {:ok, %{id: id, nome: nome, valor: valor}}
    end
  end

  defp parse_row(cols, num) do
    {:error, {:validation, {:line, num, {:wrong_column_count, length(cols)}}}}
  end

  defp parse_id(raw, num) do
    case Integer.parse(raw) do
      {id, ""} when id > 0 -> {:ok, id}
      _ -> {:error, {:validation, {:line, num, {:invalid_id, raw}}}}
    end
  end

  defp parse_nome("", num), do: {:error, {:validation, {:line, num, :empty_nome}}}
  defp parse_nome(nome, _num), do: {:ok, nome}

  defp parse_valor(raw, num) do
    case Float.parse(raw) do
      {valor, ""} -> {:ok, valor}
      _ -> {:error, {:validation, {:line, num, {:invalid_valor, raw}}}}
    end
  end

  defp split(line) do
    line
    |> String.split(",")
    |> Enum.map(&String.trim/1)
  end
end

defmodule CsvPipeline.Processor do
  @moduledoc """
  Executa o processamento de um arquivo. Roda dentro de uma Task supervisionada.

  Contrato de retorno:
    * `{:ok, resumo}`                          - sucesso
    * `{:error, {:validation, detalhe}}`       - erro de validação (dados inválidos)
    * `{:error, {:operational, motivo}}`       - falha operacional (I/O etc.), passível de retry
  """

  alias CsvPipeline.Parser

  @spec run(Path.t()) :: {:ok, map()} | {:error, term()}
  def run(path) do
    with {:ok, content} <- read(path),
         {:ok, rows} <- Parser.parse(content) do
      {:ok, summarize(rows)}
    end
  end

  # Falhas de leitura são operacionais, não de validação
  defp read(path) do
    case File.read(path) do
      {:ok, content} -> {:ok, content}
      {:error, reason} -> {:error, {:operational, {:read_failed, reason}}}
    end
  end

  defp summarize(rows) do
    %{
      registros: length(rows),
      soma: rows |> Enum.map(& &1.valor) |> Enum.sum()
    }
  end
end

defmodule CsvPipeline.Events do
  @moduledoc """
  Barramento de eventos baseado em Registry (duplicate keys).
  Processos interessados se inscrevem e recebem `{:csv_event, evento}`.
  """

  @registry __MODULE__

  @doc "Inscreve o processo chamador para receber eventos."
  def subscribe do
    {:ok, _owner} = Registry.register(@registry, :events, nil)
    :ok
  end

  @doc "Publica um evento para todos os inscritos."
  def publish(event) do
    Registry.dispatch(@registry, :events, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:csv_event, event})
    end)
  end
end

defmodule CsvPipeline.Scheduler do
  @moduledoc """
  Orquestrador periódico.

  * Executa varredura da caixa de entrada a cada `interval` ms (tick periódico).
  * Inicia cada processamento em uma Task supervisionada, sem link (`async_nolink`),
    para que a falha de uma task não derrube o orquestrador.
  * Detecta encerramentos inesperados via monitor (`{:DOWN, ...}`) e os trata
    como falha operacional.
  * Se o próprio orquestrador morrer, o Supervisor o reinicia (one_for_one).
  """

  use GenServer

  @task_supervisor CsvPipeline.TaskSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Retorna estatísticas e estado atual do orquestrador."
  def status, do: GenServer.call(__MODULE__, :status)

  @impl true
  def init(opts) do
    base = Keyword.fetch!(opts, :base_dir)

    dirs =
      Map.new([:inbox, :processed, :rejected, :failed], fn key ->
        {key, Path.join(base, Atom.to_string(key))}
      end)

    Enum.each(Map.values(dirs), &File.mkdir_p!/1)

    state = %{
      dirs: dirs,
      interval: Keyword.get(opts, :interval, 1_000),
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      # ref do Task => caminho do arquivo
      in_flight: %{},
      # caminho => número de tentativas operacionais falhas
      attempts: %{},
      stats: %{processed: 0, rejected: 0, failed: 0, crashes: 0}
    }

    # Primeiro tick imediato; os demais são agendados ao final de cada tick
    send(self(), :tick)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    busy = state.in_flight |> Map.values() |> MapSet.new()

    new_state =
      state.dirs.inbox
      |> Path.join("*.csv")
      |> Path.wildcard()
      |> Enum.reject(&MapSet.member?(busy, &1))
      |> Enum.reduce(state, &start_job/2)

    Process.send_after(self(), :tick, state.interval)
    {:noreply, new_state}
  end

  # Resultado normal devolvido pela task
  def handle_info({ref, result}, %{in_flight: in_flight} = state)
      when is_map_key(in_flight, ref) do
    # Remove o monitor e descarta eventual DOWN pendente
    Process.demonitor(ref, [:flush])
    {path, in_flight} = Map.pop!(in_flight, ref)

    {:noreply, finish(%{state | in_flight: in_flight}, path, result)}
  end

  # Encerramento inesperado da task (exceção não tratada, exit, kill etc.)
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{in_flight: in_flight} = state)
      when is_map_key(in_flight, ref) do
    {path, in_flight} = Map.pop!(in_flight, ref)
    state = %{state | in_flight: in_flight} |> bump(:crashes)

    result = {:error, {:operational, {:crashed, reason}}}
    {:noreply, finish(state, path, result)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:status, _from, state) do
    reply = %{
      stats: state.stats,
      em_execucao: map_size(state.in_flight),
      tentativas_pendentes: state.attempts
    }

    {:reply, reply, state}
  end

  # Inicia o processamento de um arquivo em uma task supervisionada e não ligada
  defp start_job(path, state) do
    task =
      Task.Supervisor.async_nolink(@task_supervisor, fn ->
        CsvPipeline.Processor.run(path)
      end)

    %{state | in_flight: Map.put(state.in_flight, task.ref, path)}
  end

  # Sucesso: move para processed
  defp finish(state, path, {:ok, summary}) do
    move(path, state.dirs.processed)
    CsvPipeline.Events.publish({:file_processed, Path.basename(path), summary})

    state
    |> bump(:processed)
    |> forget(path)
  end

  # Erro de validação: não há retry, pois os dados não mudarão sozinhos
  defp finish(state, path, {:error, {:validation, detail}}) do
    move(path, state.dirs.rejected)
    CsvPipeline.Events.publish({:file_rejected, Path.basename(path), detail})

    state
    |> bump(:rejected)
    |> forget(path)
  end

  # Falha operacional: reagenda até max_attempts, depois move para failed
  defp finish(state, path, {:error, {:operational, reason}}) do
    attempts = Map.get(state.attempts, path, 0) + 1
    name = Path.basename(path)

    if attempts < state.max_attempts do
      CsvPipeline.Events.publish({:retry_scheduled, name, attempts, reason})
      %{state | attempts: Map.put(state.attempts, path, attempts)}
    else
      move(path, state.dirs.failed)
      CsvPipeline.Events.publish({:file_failed, name, reason})

      state
      |> bump(:failed)
      |> forget(path)
    end
  end

  defp move(path, dest_dir) do
    target = Path.join(dest_dir, Path.basename(path))

    case File.rename(path, target) do
      :ok -> :ok
      {:error, reason} -> CsvPipeline.Events.publish({:move_failed, path, reason})
    end
  end

  defp bump(state, key) do
    %{state | stats: Map.update!(state.stats, key, &(&1 + 1))}
  end

  defp forget(state, path) do
    %{state | attempts: Map.delete(state.attempts, path)}
  end
end

defmodule CsvPipeline.Demo do
  @moduledoc """
  Demonstração autocontida: cria arquivos de exemplo, sobe a árvore de
  supervisão, escuta eventos por alguns segundos e encerra.
  """

  def run do
    base = Path.join(System.tmp_dir!(), "csv_pipeline_#{System.unique_integer([:positive])}")
    inbox = Path.join(base, "inbox")
    File.mkdir_p!(inbox)

    # Arquivo válido
    File.write!(Path.join(inbox, "ok.csv"), "id,nome,valor\n1,Ana,10.5\n2,Bruno,20\n")

    # Erro de validação (valor inválido na linha 2)
    File.write!(Path.join(inbox, "invalido.csv"), "id,nome,valor\n1,Ana,abc\n")

    # Falha operacional: diretório com extensão .csv faz File.read falhar (:eisdir)
    File.mkdir_p!(Path.join(inbox, "quebrado.csv"))

    children = [
      {Registry, keys: :duplicate, name: CsvPipeline.Events},
      {Task.Supervisor, name: CsvPipeline.TaskSupervisor},
      {CsvPipeline.Scheduler, base_dir: base, interval: 200, max_attempts: 3}
    ]

    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)

    # Comunicação: este processo se inscreve e recebe os eventos do orquestrador
    :ok = CsvPipeline.Events.subscribe()

    listen(System.monotonic_time(:millisecond) + 2_000)

    IO.inspect(CsvPipeline.Scheduler.status(), label: "status final")

    Supervisor.stop(sup)
    File.rm_rf!(base)
  end

  defp listen(deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:csv_event, event} ->
        IO.inspect(event, label: "evento")
        listen(deadline)
    after
      timeout -> :ok
    end
  end
end

CsvPipeline.Demo.run()
