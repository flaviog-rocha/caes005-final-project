# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use Supervisor

  @store CsvProcessor.Store
  @manager CsvProcessor.Manager

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def submit(path, opts \\ []) when is_binary(path) do
    GenServer.call(@manager, {:submit, path, opts})
  end

  def status(id) do
    GenServer.call(@manager, {:status, id})
  end

  def in_progress do
    GenServer.call(@manager, :in_progress)
  end

  @impl true
  def init(opts) do
    children = [
      {@store, Keyword.take(opts, [:store_path])},
      {@manager, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule CsvProcessor.Store do
  use GenServer

  @table __MODULE__
  @default_path "csv_processor.dets"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def all do
    GenServer.call(__MODULE__, :all)
  end

  def put(job) do
    GenServer.call(__MODULE__, {:put, job})
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :store_path, @default_path)

    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, @table} -> {:ok, @table}
      {:error, reason} -> {:stop, {:store_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(:all, _from, table) do
    jobs =
      :dets.foldl(
        fn {_id, job}, acc -> [job | acc] end,
        [],
        table
      )

    {:reply, jobs, table}
  end

  def handle_call({:put, job}, _from, table) do
    {:reply, :dets.insert(table, {job.id, job}), table}
  end

  @impl true
  def terminate(_reason, table) do
    :dets.close(table)
  end
end

defmodule CsvProcessor.Manager do
  use GenServer

  @store CsvProcessor.Store

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    max_attempts = Keyword.get(opts, :max_attempts, 3)
    retry_delay = Keyword.get(opts, :retry_delay, 1_000)
    retry_if = Keyword.get(opts, :retry_if, &default_retry?/1)
    parent = Process.get(:"$ancestors", []) |> List.first()

    jobs =
      @store.all()
      |> Enum.map(&recover_job/1)
      |> Map.new(fn job -> {job.id, job} end)

    Enum.each(Map.values(jobs), fn job ->
      if job.status == :queued, do: @store.put(job)
    end)

    state = %{
      jobs: jobs,
      queue: ordered_queued_ids(jobs),
      running: %{},
      max_concurrency: max_concurrency,
      max_attempts: max_attempts,
      retry_delay: retry_delay,
      retry_if: retry_if,
      parent: parent
    }

    send(self(), :dispatch)
    {:ok, state}
  end

  @impl true
  def handle_call({:submit, path, opts}, _from, state) do
    max_attempts = Keyword.get(opts, :max_attempts, state.max_attempts)

    if is_integer(max_attempts) and max_attempts > 0 do
      id = System.unique_integer([:positive, :monotonic])

      job = %{
        id: id,
        path: path,
        status: :queued,
        attempts: 0,
        max_attempts: max_attempts,
        inserted_at: System.system_time(:millisecond),
        started_at: nil,
        finished_at: nil,
        result: nil,
        error: nil
      }

      :ok = @store.put(job)
      state = %{state | jobs: Map.put(state.jobs, id, job), queue: state.queue ++ [id]}
      send(self(), :dispatch)
      {:reply, {:ok, id}, state}
    else
      {:reply, {:error, :invalid_max_attempts}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    {:reply, Map.get(state.jobs, id), state}
  end

  def handle_call(:in_progress, _from, state) do
    jobs =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running, :retrying]))
      |> Enum.sort_by(& &1.inserted_at)

    {:reply, jobs, state}
  end

  @impl true
  def handle_info(:dispatch, state) do
    {:noreply, dispatch_available(state)}
  end

  def handle_info({:retry, id, attempts}, state) do
    case Map.get(state.jobs, id) do
      %{status: :retrying, attempts: ^attempts} = job ->
        job = %{job | status: :queued}
        :ok = @store.put(job)

        state = %{
          state
          | jobs: Map.put(state.jobs, id, job),
            queue: state.queue ++ [id]
        }

        {:noreply, dispatch_available(state)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:csv_worker_result, pid, result}, state) do
    case Map.pop(state.running, pid) do
      {nil, _running} ->
        {:noreply, state}

      {id, running} ->
        state = %{state | running: running}
        job = Map.fetch!(state.jobs, id)
        {:noreply, result_job(job, result, state) |> dispatch_available()}
    end
  end

  def handle_info({:EXIT, pid, reason}, %{parent: pid} = state) do
    {:stop, reason, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    case Map.pop(state.running, pid) do
      {nil, _running} ->
        {:noreply, state}

      {id, running} ->
        state = %{state | running: running}
        job = Map.fetch!(state.jobs, id)
        error = {:worker_exit, reason}
        {:noreply, result_job(job, {:error, error}, state) |> dispatch_available()}
    end
  end

  defp dispatch_available(state) do
    if map_size(state.running) < state.max_concurrency do
      case take_queued_job(state) do
        {nil, state} ->
          state

        {job, state} ->
          start_worker(job, state)
          |> dispatch_available()
      end
    else
      state
    end
  end

  defp take_queued_job(%{queue: []} = state), do: {nil, state}

  defp take_queued_job(%{queue: [id | rest]} = state) do
    state = %{state | queue: rest}

    case Map.get(state.jobs, id) do
      %{status: :queued} = job -> {job, state}
      _ -> take_queued_job(state)
    end
  end

  defp start_worker(job, state) do
    attempts = job.attempts + 1

    job = %{
      job
      | status: :running,
        attempts: attempts,
        started_at: System.system_time(:millisecond),
        error: nil
    }

    :ok = @store.put(job)
    parent = self()

    pid =
      spawn_link(fn ->
        result = run_csv_job(job.path)
        send(parent, {:csv_worker_result, self(), result})
      end)

    %{
      state
      | jobs: Map.put(state.jobs, job.id, job),
        running: Map.put(state.running, pid, job.id)
    }
  end

  defp result_job(job, {:ok, result}, state) do
    job = %{
      job
      | status: :completed,
        result: result,
        error: nil,
        finished_at: System.system_time(:millisecond)
    }

    :ok = @store.put(job)
    %{state | jobs: Map.put(state.jobs, job.id, job)}
  end

  defp result_job(job, {:error, reason}, state) do
    if job.attempts < job.max_attempts and retry?(state.retry_if, reason) do
      delay = state.retry_delay * round(:math.pow(2, max(job.attempts - 1, 0)))
      job = %{job | status: :retrying, error: reason}
      :ok = @store.put(job)
      Process.send_after(self(), {:retry, job.id, job.attempts}, delay)
      %{state | jobs: Map.put(state.jobs, job.id, job)}
    else
      job = %{
        job
        | status: :failed,
          error: reason,
          finished_at: System.system_time(:millisecond)
      }

      :ok = @store.put(job)
      %{state | jobs: Map.put(state.jobs, job.id, job)}
    end
  end

  defp retry?(predicate, error) do
    try do
      predicate.(error)
    rescue
      _ -> false
    catch
      _, _ -> false
    end
  end

  defp default_retry?({:error, {:invalid_csv, _reason}}), do: false
  defp default_retry?(_error), do: true

  defp recover_job(%{status: status} = job) when status in [:running, :retrying] do
    if job.attempts < job.max_attempts do
      %{job | status: :queued}
    else
      %{
        job
        | status: :failed,
          error: :interrupted_after_max_attempts,
          finished_at: System.system_time(:millisecond)
      }
    end
  end

  defp recover_job(job), do: job

  defp ordered_queued_ids(jobs) do
    jobs
    |> Map.values()
    |> Enum.filter(&(&1.status == :queued))
    |> Enum.sort_by(&{&1.inserted_at, &1.id})
    |> Enum.map(& &1.id)
  end

  defp run_csv_job(path) do
    case File.read(path) do
      {:ok, contents} ->
        contents
        |> String.replace("\r\n", "\n")
        |> String.replace("\r", "\n")
        |> String.to_charlist()
        |> count_csv_rows()

      {:error, reason} ->
        {:error, {:file, reason}}
    end
  rescue
    exception -> {:error, {:exception, Exception.message(exception)}}
  catch
    kind, reason -> {:error, {:caught, kind, reason}}
  end

  defp count_csv_rows(chars) do
    scan_csv(chars, false, false, false, false, 0)
  end

  defp scan_csv([], true, _field_started, _closed_quote, _record_started, _count) do
    {:error, {:invalid_csv, :unclosed_quote}}
  end

  defp scan_csv([], false, _field_started, _closed_quote, false, count) do
    {:ok, %{rows: count}}
  end

  defp scan_csv([], false, _field_started, _closed_quote, true, count) do
    {:ok, %{rows: count + 1}}
  end

  defp scan_csv([?", ?" | rest], true, _field_started, _closed_quote, _record_started, count) do
    scan_csv(rest, true, true, false, true, count)
  end

  defp scan_csv([?" | rest], true, _field_started, _closed_quote, record_started, count) do
    scan_csv(rest, false, true, true, record_started, count)
  end

  defp scan_csv([?\n | rest], true, _field_started, _closed_quote, _record_started, count) do
    scan_csv(rest, true, true, false, true, count)
  end

  defp scan_csv([_char | rest], true, _field_started, _closed_quote, _record_started, count) do
    scan_csv(rest, true, true, false, true, count)
  end

  defp scan_csv([?" | rest], false, false, false, _record_started, count) do
    scan_csv(rest, true, true, false, true, count)
  end

  defp scan_csv([?" | _rest], false, _field_started, _closed_quote, _record_started, _count) do
    {:error, {:invalid_csv, :unexpected_quote}}
  end

  defp scan_csv([?, | rest], false, _field_started, _closed_quote, _record_started, count) do
    scan_csv(rest, false, false, false, true, count)
  end

  defp scan_csv([?\n | rest], false, _field_started, _closed_quote, _record_started, count) do
    scan_csv(rest, false, false, false, false, count + 1)
  end

  defp scan_csv([_char | _rest], false, _field_started, true, _record_started, _count) do
    {:error, {:invalid_csv, :characters_after_quote}}
  end

  defp scan_csv([_char | rest], false, _field_started, false, _record_started, count) do
    scan_csv(rest, false, true, false, true, count)
  end
end
