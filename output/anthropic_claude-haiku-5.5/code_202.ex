# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único autocontido: processamento de CSV com retentativas, limite de
# concorrência, consulta de estado e recuperação após falhas.
#
# Uso (exemplo em comentário):
#
#   {:ok, _pid} =
#     CsvProcessor.start_link(
#       handler: fn row -> if length(row) > 0, do: :ok, else: {:error, :permanent, :empty} end,
#       max_concurrency: 4,
#       max_attempts: 5,
#       state_dir: "/tmp/csv_state"
#     )
#
#   {:ok, id} = CsvProcessor.enqueue("dados.csv")
#   CsvProcessor.status()
#   CsvProcessor.status(id)

defmodule CsvProcessor do
  @moduledoc """
  Supervisor raiz do processador de CSV.

  Estrutura:
    * `Task.Supervisor` (`CsvProcessor.TaskSupervisor`) executa os workers.
    * `CsvProcessor.Manager` (GenServer) controla fila, concorrência, retentativas
      e persistência do estado em disco (journal).

  Estratégia `:rest_for_one`: se o supervisor de tarefas cair, o Manager também
  é reiniciado e, ao subir, recupera o estado a partir do journal.
  """

  use Supervisor

  @doc "Inicia o processador. Veja `CsvProcessor.Manager` para as opções."
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: CsvProcessor.TaskSupervisor},
      {CsvProcessor.Manager, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Enfileira um arquivo CSV para processamento. Retorna `{:ok, id}`."
  def enqueue(path), do: CsvProcessor.Manager.enqueue(path)

  @doc "Retorna o resumo do estado atual (em execução, aguardando, retentando, etc.)."
  def status, do: CsvProcessor.Manager.status()

  @doc "Retorna o estado de uma operação específica pelo id."
  def status(id), do: CsvProcessor.Manager.job_status(id)
end

defmodule CsvProcessor.Manager do
  @moduledoc """
  Gerencia as operações de processamento.

  Opções de inicialização:
    * `:handler` (obrigatório) - função de aridade 1 que recebe uma linha (lista de campos)
      e retorna `:ok`, `{:error, :transient, motivo}` ou `{:error, :permanent, motivo}`.
    * `:max_concurrency` - limite de operações simultâneas (padrão: schedulers online).
    * `:max_attempts` - número máximo de tentativas por operação (padrão: 5).
    * `:state_dir` - diretório onde o journal de recuperação é gravado.
    * `:chunk_size` - a cada quantas linhas o progresso é persistido.

  Semântica de recuperação: o progresso é gravado a cada `chunk_size` linhas,
  portanto, após uma falha, o processamento é retomado a partir do último
  checkpoint (semântica at-least-once dentro de um mesmo chunk).
  """

  use GenServer

  @default_max_attempts 5
  @base_backoff_ms 500
  @max_backoff_ms 30_000
  @default_chunk_size 1_000
  @journal_file "journal.bin"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def enqueue(path), do: GenServer.call(__MODULE__, {:enqueue, path})

  def status, do: GenServer.call(__MODULE__, :status)

  def job_status(id), do: GenServer.call(__MODULE__, {:job_status, id})

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    cfg = %{
      handler: Keyword.fetch!(opts, :handler),
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
      state_dir: Keyword.get(opts, :state_dir, "csv_processor_state"),
      chunk_size: Keyword.get(opts, :chunk_size, @default_chunk_size)
    }

    File.mkdir_p!(cfg.state_dir)

    {jobs, next_id} = load_journal(cfg)

    state =
      %{cfg: cfg, jobs: jobs, next_id: next_id, queue: :queue.new(), running: %{}}
      |> recover()

    {:ok, dispatch(state)}
  end

  @impl true
  def handle_call({:enqueue, path}, _from, state) do
    id = state.next_id

    job = %{
      id: id,
      path: path,
      status: :queued,
      attempts: 0,
      line: 0,
      pid: nil,
      last_error: nil,
      enqueued_at: now_ms(),
      started_at: nil,
      finished_at: nil,
      next_retry_at: nil
    }

    state =
      %{state | next_id: id + 1, queue: :queue.in(id, state.queue)}
      |> put_job(job)
      |> persist()
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call(:status, _from, state) do
    jobs = Map.values(state.jobs)
    by_status = fn s -> Enum.filter(jobs, &(&1.status == s)) end

    summary = %{
      max_concurrency: state.cfg.max_concurrency,
      running: Enum.map(by_status.(:running), &public/1),
      retrying: Enum.map(by_status.(:retrying), &public/1),
      queued: length(by_status.(:queued)),
      done: length(by_status.(:done)),
      failed: length(by_status.(:failed))
    }

    {:reply, summary, state}
  end

  def handle_call({:job_status, id}, _from, state) do
    reply =
      case Map.fetch(state.jobs, id) do
        {:ok, job} -> {:ok, public(job)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_cast({:progress, id, line}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :running} = job} ->
        {:noreply, state |> put_job(%{job | line: line}) |> persist()}

      _ ->
        {:noreply, state}
    end
  end

  # Resultado retornado pela Task (Task.Supervisor.async_nolink).
  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish(state, ref, result)}
  end

  # Task caiu inesperadamente (crash) - tratada como falha transitória.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {:noreply, finish(state, ref, {:error, :transient, {:crashed, reason}})}
  end

  # Timer de retentativa disparado.
  def handle_info({:retry, id}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :retrying} = job} ->
        state =
          %{state | queue: :queue.in(id, state.queue)}
          |> put_job(%{job | status: :queued})
          |> persist()
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Despacho e controle de concorrência
  # ---------------------------------------------------------------------------

  # Inicia operações da fila enquanto houver vagas no limite de concorrência.
  defp dispatch(state) do
    if map_size(state.running) < state.cfg.max_concurrency and not :queue.is_empty(state.queue) do
      {{:value, id}, queue} = :queue.out(state.queue)

      state
      |> Map.put(:queue, queue)
      |> start_job(id)
      |> dispatch()
    else
      state
    end
  end

  defp start_job(state, id) do
    job = Map.fetch!(state.jobs, id)
    cfg = state.cfg

    # Função usada pelo worker para reportar progresso ao Manager.
    report = fn line -> GenServer.cast(__MODULE__, {:progress, id, line}) end

    args = %{
      path: job.path,
      from: job.line,
      handler: cfg.handler,
      chunk_size: cfg.chunk_size,
      report: report
    }

    task =
      Task.Supervisor.async_nolink(CsvProcessor.TaskSupervisor, fn ->
        CsvProcessor.Worker.run(args)
      end)

    job = %{
      job
      | status: :running,
        attempts: job.attempts + 1,
        pid: task.pid,
        started_at: now_ms(),
        next_retry_at: nil
    }

    state
    |> put_job(job)
    |> Map.update!(:running, &Map.put(&1, task.ref, id))
    |> persist()
  end

  defp finish(state, ref, result) do
    case Map.pop(state.running, ref) do
      {nil, _} ->
        state

      {id, running} ->
        job = %{Map.fetch!(state.jobs, id) | pid: nil}

        job =
          case result do
            {:ok, line} ->
              %{job | status: :done, line: line, finished_at: now_ms()}

            {:error, :permanent, reason} ->
              %{job | status: :failed, last_error: reason, finished_at: now_ms()}

            {:error, :transient, reason} ->
              schedule_or_fail(job, reason, state.cfg)
          end

        %{state | running: running}
        |> put_job(job)
        |> persist()
        |> dispatch()
    end
  end

  # Decide entre nova tentativa (backoff exponencial) ou falha definitiva.
  defp schedule_or_fail(job, reason, cfg) do
    if job.attempts < cfg.max_attempts do
      delay = backoff(job.attempts, cfg)
      Process.send_after(self(), {:retry, job.id}, delay)
      %{job | status: :retrying, last_error: reason, next_retry_at: now_ms() + delay}
    else
      %{job | status: :failed, last_error: reason, finished_at: now_ms()}
    end
  end

  defp backoff(attempts, _cfg) do
    min(@base_backoff_ms * Integer.pow(2, attempts - 1), @max_backoff_ms)
  end

  # ---------------------------------------------------------------------------
  # Recuperação e persistência
  # ---------------------------------------------------------------------------

  # Reconstrói o estado após reinício do Manager a partir do journal.
  defp recover(state) do
    now = now_ms()

    state.jobs
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reduce(state, fn id, acc ->
      job = Map.fetch!(acc.jobs, id)

      case job.status do
        :running ->
          # Worker órfão ainda pode estar vivo: encerra antes de reagendar.
          if is_pid(job.pid) and Process.alive?(job.pid), do: Process.exit(job.pid, :kill)
          put_job(acc, schedule_or_fail(%{job | pid: nil}, :interrupted, acc.cfg))

        :retrying ->
          Process.send_after(self(), {:retry, id}, max(job.next_retry_at - now, 0))
          acc

        :queued ->
          %{acc | queue: :queue.in(id, acc.queue)}

        _finished ->
          acc
      end
    end)
    |> persist()
  end

  defp put_job(state, job), do: %{state | jobs: Map.put(state.jobs, job.id, job)}

  # Grava o journal de forma atômica (escreve em arquivo temporário e renomeia).
  defp persist(state) do
    path = Path.join(state.cfg.state_dir, @journal_file)
    tmp = path <> ".tmp"
    data = :erlang.term_to_binary(%{next_id: state.next_id, jobs: state.jobs})

    File.write!(tmp, data)
    File.rename!(tmp, path)
    state
  end

  # Carrega o journal. Journal corrompido é descartado (início limpo).
  defp load_journal(cfg) do
    path = Path.join(cfg.state_dir, @journal_file)

    case File.read(path) do
      {:ok, bin} ->
        try do
          %{next_id: next_id, jobs: jobs} = :erlang.binary_to_term(bin)
          {jobs, next_id}
        rescue
          _ -> {%{}, 1}
        end

      {:error, _} ->
        {%{}, 1}
    end
  end

  defp public(job) do
    Map.take(job, [
      :id,
      :path,
      :status,
      :attempts,
      :line,
      :last_error,
      :started_at,
      :finished_at,
      :next_retry_at
    ])
  end

  defp now_ms, do: System.system_time(:millisecond)
end

defmodule CsvProcessor.Worker do
  @moduledoc false
  # Executa o processamento de um arquivo CSV a partir de um checkpoint.

  def run(%{path: path, from: from, handler: handler, chunk_size: chunk, report: report}) do
    if File.regular?(path) do
      try do
        process(path, from, handler, chunk, report)
      rescue
        e in File.Error -> {:error, :transient, e.reason}
      end
    else
      {:error, :permanent, :file_not_found}
    end
  end

  defp process(path, from, handler, chunk, report) do
    result =
      path
      |> File.stream!()
      |> Stream.drop(from)
      |> Enum.reduce_while({:ok, from}, fn line, {:ok, done} ->
        case handler.(parse_line(line)) do
          :ok ->
            done = done + 1
            if rem(done, chunk) == 0, do: report.(done)
            {:cont, {:ok, done}}

          {:error, kind, reason} when kind in [:transient, :permanent] ->
            {:halt, {:error, kind, reason}}
        end
      end)

    result
  end

  # Parser CSV mínimo da biblioteca padrão: suporta campos entre aspas,
  # vírgulas internas e aspas escapadas ("").
  # Limitação: campos com quebra de linha dentro de aspas não são suportados.
  @doc false
  def parse_line(line) do
    line
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
    |> split_fields([], "", false)
  end

  # 34 = aspas duplas, 44 = vírgula
  defp split_fields("", acc, field, _in_quotes), do: Enum.reverse([field | acc])

  defp split_fields(<<34, 34, rest::binary>>, acc, field, true),
    do: split_fields(rest, acc, field <> "\"", true)

  defp split_fields(<<34, rest::binary>>, acc, field, in_quotes),
    do: split_fields(rest, acc, field, not in_quotes)

  defp split_fields(<<44, rest::binary>>, acc, field, false),
    do: split_fields(rest, [field | acc], "", false)

  defp split_fields(<<c, rest::binary>>, acc, field, in_quotes),
    do: split_fields(rest, acc, field <> <<c>>, in_quotes)
end
