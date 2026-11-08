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

  @server CsvProcessor.Server

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {CsvProcessor.Server, opts},
      {Task.Supervisor, name: CsvProcessor.TaskSupervisor}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def submit(path) when is_binary(path) do
    GenServer.call(@server, {:submit, path})
  end

  def status(id) do
    GenServer.call(@server, {:status, id})
  end

  def statuses do
    GenServer.call(@server, :statuses)
  end
end

defmodule CsvProcessor.Server do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    journal =
      Keyword.get_lazy(opts, :journal, fn ->
        Path.join(System.tmp_dir!(), "csv_processor_#{node()}.term")
      end)

    {jobs, queue, next_id} = load_journal(journal)

    state = %{
      jobs: jobs,
      queue: queue,
      running: %{},
      limit: Keyword.get(opts, :max_concurrency, 4),
      max_retries: Keyword.get(opts, :max_retries, 3),
      journal: journal,
      next_id: next_id
    }

    send(self(), :dispatch)
    {:ok, state}
  end

  @impl true
  def handle_call({:submit, path}, _from, state) do
    id = state.next_id

    job = %{
      id: id,
      path: path,
      status: :queued,
      attempts: 0,
      result: nil,
      error: nil
    }

    new_state = %{
      state
      | jobs: Map.put(state.jobs, id, job),
        queue: state.queue ++ [id],
        next_id: id + 1
    }

    case persist(new_state) do
      :ok ->
        send(self(), :dispatch)
        {:reply, {:ok, id}, new_state}

      {:error, reason} ->
        {:reply, {:error, {:journal, reason}}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.jobs, id) do
        {:ok, job} -> {:ok, job}
        :error -> :error
      end

    {:reply, reply, state}
  end

  def handle_call(:statuses, _from, state) do
    jobs = state.jobs |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, jobs, state}
  end

  @impl true
  def handle_info(:dispatch, state) do
    {:noreply, dispatch(state)}
  end

  def handle_info({ref, result}, state) when is_map_key(state.running, ref) do
    Process.demonitor(ref, [:flush])
    id = Map.fetch!(state.running, ref)
    state = finish_job(state, ref, id, result)
    persist!(state)
    send(self(), :dispatch)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.running, ref) do
    id = Map.fetch!(state.running, ref)
    state = finish_job(state, ref, id, {:crash, reason})
    persist!(state)
    send(self(), :dispatch)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    if map_size(state.running) >= state.limit or state.queue == [] do
      state
    else
      [id | rest] = state.queue
      job = Map.fetch!(state.jobs, id)

      try do
        task =
          Task.Supervisor.async_nolink(CsvProcessor.TaskSupervisor, fn ->
            process_csv(job.path)
          end)

        updated_job = %{job | status: :running, attempts: job.attempts + 1}

        state = %{
          state
          | queue: rest,
            running: Map.put(state.running, task.ref, id),
            jobs: Map.put(state.jobs, id, updated_job)
        }

        persist!(state)
        dispatch(state)
      catch
        :exit, _reason ->
          Process.send_after(self(), :dispatch, 100)
          state
      end
    end
  end

  defp finish_job(state, ref, id, result) do
    job = Map.fetch!(state.jobs, id)
    state = %{state | running: Map.delete(state.running, ref)}

    case result do
      {:ok, stats} ->
        put_job(state, %{job | status: :completed, result: stats, error: nil})

      {:error, reason, true} ->
        retry_or_fail(state, job, reason)

      {:error, reason, false} ->
        put_job(state, %{job | status: :failed, error: reason})

      {:crash, reason} ->
        retry_or_fail(state, job, {:worker_crash, reason})

      other ->
        put_job(state, %{job | status: :failed, error: {:unexpected_result, other}})
    end
  end

  defp retry_or_fail(state, job, reason) do
    if job.attempts <= state.max_retries do
      queued_job = %{job | status: :queued, error: reason}
      put_job(%{state | queue: state.queue ++ [job.id]}, queued_job)
    else
      put_job(state, %{job | status: :failed, error: reason})
    end
  end

  defp put_job(state, job) do
    %{state | jobs: Map.put(state.jobs, job.id, job)}
  end

  defp process_csv(path) do
    case File.read(path) do
      {:ok, contents} ->
        case CsvProcessor.Parser.parse(contents) do
          {:ok, stats} -> {:ok, stats}
          {:error, reason} -> {:error, reason, false}
        end

      {:error, reason} ->
        {:error, {:file, reason}, transient?(reason)}
    end
  end

  defp transient?(reason) do
    reason in [:eagain, :eintr, :ebusy, :emfile, :enfile, :enospc, :eio, :etimedout, :enoent]
  end

  defp load_journal(path) do
    case File.read(path) do
      {:ok, data} ->
        try do
          snapshot = :erlang.binary_to_term(data, [:safe])
          jobs = Map.get(snapshot, :jobs, %{})

          recovered_jobs =
            Map.new(jobs, fn {id, job} ->
              if job.status == :running do
                {id, %{job | status: :queued}}
              else
                {id, job}
              end
            end)

          queued_ids =
            (Map.get(snapshot, :queue, []) ++
               for {id, job} <- recovered_jobs, job.status == :queued, do: id)
            |> Enum.uniq()
            |> Enum.filter(&match?(%{status: :queued}, Map.get(recovered_jobs, &1)))

          next_id =
            max(Map.get(snapshot, :next_id, 1), (Map.keys(recovered_jobs) |> Enum.max(fn -> 0 end)) + 1)

          {recovered_jobs, queued_ids, next_id}
        rescue
          _ -> {%{}, [], 1}
        end

      {:error, :enoent} ->
        {%{}, [], 1}

      {:error, _reason} ->
        {%{}, [], 1}
    end
  end

  defp persist!(state) do
    case persist(state) do
      :ok -> :ok
      {:error, reason} -> raise "CSV processor journal write failed: #{inspect(reason)}"
    end
  end

  defp persist(state) do
    path = state.journal
    temporary = path <> ".tmp.#{System.unique_integer([:positive])}"

    snapshot = %{
      jobs: state.jobs,
      queue: state.queue,
      next_id: state.next_id
    }

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary, :erlang.term_to_binary(snapshot), [:binary]),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary)
        {:error, reason}
    end
  end
end

defmodule CsvProcessor.Parser do
  def parse(contents) when is_binary(contents) do
    state = %{
      quoted: false,
      fields: 1,
      field_empty: true,
      row_started: false,
      rows: 0,
      max_columns: 0
    }

    scan(:binary.bin_to_list(contents), state)
  end

  defp scan([], %{quoted: true}), do: {:error, :unterminated_quoted_field}

  defp scan([], state) do
    if state.row_started or not state.field_empty or state.fields > 1 do
      {:ok,
       %{
         rows: state.rows + 1,
         max_columns: max(state.max_columns, state.fields)
       }}
    else
      {:ok, %{rows: state.rows, max_columns: state.max_columns}}
    end
  end

  defp scan([?", ?" | rest], %{quoted: true} = state) do
    scan(rest, %{state | field_empty: false, row_started: true})
  end

  defp scan([?" | rest], %{quoted: true} = state) do
    scan(rest, %{state | quoted: false})
  end

  defp scan([char | rest], %{quoted: true} = state) do
    _ = char
    scan(rest, %{state | field_empty: false, row_started: true})
  end

  defp scan([?, | rest], state) do
    scan(rest, %{
      state
      | fields: state.fields + 1,
        field_empty: true,
        row_started: true
    })
  end

  defp scan([?\r, ?\n | rest], state), do: end_row(rest, state)
  defp scan([?\r | rest], state), do: end_row(rest, state)
  defp scan([?\n | rest], state), do: end_row(rest, state)

  defp scan([?" | rest], %{field_empty: true} = state) do
    scan(rest, %{state | quoted: true, field_empty: false, row_started: true})
  end

  defp scan([?" | _rest], _state), do: {:error, :unexpected_quote}

  defp scan([_char | rest], state) do
    scan(rest, %{state | field_empty: false, row_started: true})
  end

  defp end_row(rest, state) do
    scan(rest, %{
      state
      | fields: 1,
        field_empty: true,
        row_started: false,
        rows: state.rows + 1,
        max_columns: max(state.max_columns, state.fields)
    })
  end
end
