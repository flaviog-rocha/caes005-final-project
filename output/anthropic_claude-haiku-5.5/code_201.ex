# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Processamento de arquivos CSV tolerante a falhas.
#
# Requisitos atendidos:
#   1. Novas tentativas com backoff exponencial para falhas temporárias.
#   2. Limite de operações simultâneas (max_concurrency).
#   3. Consulta do estado das operações (status/0 e status/1).
#   4. Recuperação após falhas: workers reiniciam a partir do checkpoint
#      e o manager recupera a fila a partir de um arquivo DETS.
#
# Uso:
#   {:ok, _} = CsvPipeline.start_link(max_concurrency: 4, store: "jobs.dets")
#   {:ok, id} = CsvPipeline.submit("dados.csv", {MeuModulo, :processa_linha}, max_attempts: 5)
#   CsvPipeline.status()
#   CsvPipeline.status(id)
#
# Contrato do handler: função exportada que recebe a lista de campos de uma linha
# e retorna :ok, {:error, :transient, motivo} (nova tentativa) ou {:error, motivo} (falha definitiva).

defmodule CsvPipeline do
  # API pública

  def start_link(opts \\ []), do: CsvPipeline.Sup.start_link(opts)

  def submit(path, {mod, fun} = handler, opts \\ [])
      when is_binary(path) and is_atom(mod) and is_atom(fun) do
    GenServer.call(CsvPipeline.Manager, {:submit, path, handler, opts})
  end

  # Estado de um job específico
  def status(id), do: GenServer.call(CsvPipeline.Manager, {:status, id})

  # Resumo geral: operações em andamento, fila e contadores por status
  def status, do: GenServer.call(CsvPipeline.Manager, :summary)
end

defmodule CsvPipeline.Sup do
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: CsvPipeline.Workers},
      {CsvPipeline.Manager, opts}
    ]

    # rest_for_one: se os workers caírem, o manager reinicia e recupera os jobs
    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule CsvPipeline.Manager do
  use GenServer

  @table :csv_pipeline_jobs
  @max_backoff_ms 60_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    store = Keyword.get(opts, :store, "csv_pipeline_jobs.dets")
    {:ok, _} = :dets.open_file(@table, file: String.to_charlist(store), type: :set)

    # Recuperação 1: workers órfãos de uma instância anterior do manager são encerrados
    Enum.each(Task.Supervisor.children(CsvPipeline.Workers), fn pid ->
      Task.Supervisor.terminate_child(CsvPipeline.Workers, pid)
    end)

    # Recuperação 2: jobs interrompidos voltam para a fila, preservando o checkpoint
    jobs = :dets.foldl(fn {id, job}, acc -> Map.put(acc, id, recover(job)) end, %{}, @table)
    Enum.each(jobs, fn {_id, job} -> persist(job) end)

    queue =
      jobs
      |> Enum.filter(fn {_id, job} -> job.status == :queued end)
      |> Enum.map(fn {id, _job} -> id end)
      |> Enum.sort()

    state = %{
      max: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      base_backoff: Keyword.get(opts, :base_backoff_ms, 500),
      jobs: jobs,
      running: %{},
      queue: :queue.from_list(queue)
    }

    {:ok, state, {:continue, :dispatch}}
  end

  @impl true
  def handle_continue(:dispatch, state), do: {:noreply, dispatch(state)}

  # Chamadas síncronas

  @impl true
  def handle_call({:submit, path, handler, opts}, _from, state) do
    id = {System.system_time(:nanosecond), :erlang.unique_integer([:positive])}

    job = %{
      id: id,
      path: path,
      handler: handler,
      status: :queued,
      attempts: 0,
      max_attempts: Keyword.get(opts, :max_attempts, state.max_attempts),
      checkpoint: 0,
      last_error: nil,
      submitted_at: now(),
      started_at: nil,
      finished_at: nil
    }

    persist(job)

    state = %{state | jobs: Map.put(state.jobs, id, job), queue: :queue.in(id, state.queue)}
    {:reply, {:ok, id}, dispatch(state)}
  end

  def handle_call({:status, id}, _from, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, job} -> {:reply, {:ok, job}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:summary, _from, state) do
    running = for id <- Map.values(state.running), do: Map.fetch!(state.jobs, id)

    summary = %{
      max_concurrency: state.max,
      queued: :queue.len(state.queue),
      running: running,
      counts: Enum.frequencies_by(Map.values(state.jobs), & &1.status)
    }

    {:reply, summary, state}
  end

  # Mensagens assíncronas

  # Checkpoint enviado pelo worker
  @impl true
  def handle_info({:progress, id, line}, state) do
    {:noreply, update_job(state, id, &%{&1 | checkpoint: line})}
  end

  # Resultado de uma tentativa
  def handle_info({ref, result}, %{running: running} = state)
      when is_reference(ref) and is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {id, running} = Map.pop(running, ref)

    state = finish_attempt(%{state | running: running}, id, result)
    {:noreply, dispatch(state)}
  end

  # Crash inesperado do worker: tratado como falha temporária
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {id, running} = Map.pop(running, ref)

    state = finish_attempt(%{state | running: running}, id, {:error, :transient, {:crash, reason}})
    {:noreply, dispatch(state)}
  end

  # Nova tentativa após backoff
  def handle_info({:retry, id}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :retry_wait}} ->
        state = update_job(state, id, &%{&1 | status: :queued})
        {:noreply, dispatch(%{state | queue: :queue.in(id, state.queue)})}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, _state), do: :dets.close(@table)

  # Funções privadas

  # Inicia jobs enquanto houver vagas e itens na fila (controle de concorrência)
  defp dispatch(%{max: max, running: running} = state) when map_size(running) >= max, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        state

      {{:value, id}, queue} ->
        state = %{state | queue: queue}

        state =
          update_job(
            state,
            id,
            &%{&1 | status: :running, attempts: &1.attempts + 1, started_at: now()}
          )

        job = Map.fetch!(state.jobs, id)

        task =
          Task.Supervisor.async_nolink(CsvPipeline.Workers, CsvPipeline.Worker, :run, [job])

        dispatch(%{state | running: Map.put(state.running, task.ref, id)})
    end
  end

  defp finish_attempt(state, id, :ok) do
    update_job(state, id, &%{&1 | status: :done, finished_at: now(), last_error: nil})
  end

  defp finish_attempt(state, id, {:error, :transient, reason}) do
    job = Map.fetch!(state.jobs, id)

    if job.attempts < job.max_attempts do
      delay = backoff(job.attempts, state.base_backoff)
      Process.send_after(self(), {:retry, id}, delay)

      update_job(state, id, &%{&1 | status: :retry_wait, last_error: reason, started_at: nil})
    else
      update_job(state, id, &%{&1 | status: :failed, last_error: reason, finished_at: now()})
    end
  end

  defp finish_attempt(state, id, {:error, reason}) do
    update_job(state, id, &%{&1 | status: :failed, last_error: reason, finished_at: now()})
  end

  defp finish_attempt(state, id, other) do
    finish_attempt(state, id, {:error, {:unexpected_result, other}})
  end

  # Backoff exponencial: base * 2^(tentativa - 1), limitado a @max_backoff_ms
  defp backoff(attempt, base), do: min(base * Integer.pow(2, attempt - 1), @max_backoff_ms)

  defp update_job(state, id, fun) do
    case Map.fetch(state.jobs, id) do
      {:ok, job} ->
        job = fun.(job)
        persist(job)
        %{state | jobs: Map.put(state.jobs, id, job)}

      :error ->
        state
    end
  end

  # Jobs que estavam em andamento durante a falha voltam para a fila
  defp recover(%{status: status} = job) when status in [:queued, :running, :retry_wait] do
    if job.attempts >= job.max_attempts do
      %{job | status: :failed, last_error: :interrupted, finished_at: now()}
    else
      %{job | status: :queued, started_at: nil}
    end
  end

  defp recover(job), do: job

  defp persist(job) do
    :ok = :dets.insert(@table, {job.id, job})
    :ok = :dets.sync(@table)
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule CsvPipeline.Worker do
  # Executa um job de CSV. Chamado dentro de uma Task supervisionada.

  @progress_every 1_000
  @transient_io [:emfile, :enfile, :eagain, :etimedout, :ebusy, :enomem, :eintr]

  def run(job) do
    case File.open(job.path, [:read, :utf8]) do
      {:ok, io} ->
        try do
          process(io, job)
        after
          File.close(io)
        end

      {:error, reason} when reason in @transient_io ->
        {:error, :transient, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Processa as linhas a partir do checkpoint. Como o checkpoint é gravado em lotes,
  # linhas após o último checkpoint podem ser reprocessadas em uma nova tentativa
  # (semântica at-least-once). Os handlers devem ser idempotentes.
  defp process(io, job) do
    io
    |> IO.stream(:line)
    |> Stream.with_index(1)
    |> Stream.drop(job.checkpoint)
    |> Enum.reduce_while(:ok, fn {line, n}, :ok ->
      case handle_line(line, job.handler) do
        :ok ->
          if rem(n, @progress_every) == 0, do: send(CsvPipeline.Manager, {:progress, job.id, n})
          {:cont, :ok}

        error ->
          {:halt, error}
      end
    end)
  end

  defp handle_line(line, {mod, fun}) do
    case String.trim(line) do
      "" ->
        :ok

      _ ->
        case apply(mod, fun, [parse_line(line)]) do
          :ok -> :ok
          {:error, :transient, _reason} = error -> error
          {:error, _reason} = error -> error
          other -> {:error, {:bad_return, other}}
        end
    end
  end

  # Parser CSV simples com suporte a campos entre aspas e aspas escapadas ("")
  def parse_line(line) do
    line
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
    |> split_fields([], "", false)
  end

  defp split_fields(<<>>, acc, cur, _quoted), do: Enum.reverse([cur | acc])

  defp split_fields(<<?", ?", rest::binary>>, acc, cur, true),
    do: split_fields(rest, acc, cur <> "\"", true)

  defp split_fields(<<?", rest::binary>>, acc, cur, quoted),
    do: split_fields(rest, acc, cur, not quoted)

  defp split_fields(<<?,, rest::binary>>, acc, cur, false),
    do: split_fields(rest, [cur | acc], "", false)

  defp split_fields(<<c, rest::binary>>, acc, cur, quoted),
    do: split_fields(rest, acc, cur <> <<c>>, quoted)
end
