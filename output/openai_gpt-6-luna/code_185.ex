# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir comunicação entre diferentes processos.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  use GenServer

  @max_retry_delay 60_000
  @active_statuses [:running, :retry_wait]

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def submit(id, operation, opts \\ [], server \\ __MODULE__) do
    GenServer.call(server, {:submit, id, operation, opts})
  end

  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  def operation_status(id, server \\ __MODULE__) do
    GenServer.call(server, {:operation_status, id})
  end

  def in_progress(server \\ __MODULE__) do
    GenServer.call(server, :in_progress)
  end

  def schedule_periodic(id, interval_ms, operation, opts \\ [], server \\ __MODULE__) do
    GenServer.call(server, {:schedule_periodic, id, interval_ms, operation, opts})
  end

  def cancel_periodic(id, server \\ __MODULE__) do
    GenServer.call(server, {:cancel_periodic, id})
  end

  @impl true
  def init(_opts) do
    {:ok, %{jobs: %{}, monitors: %{}, schedules: %{}}}
  end

  @impl true
  def handle_call({:submit, id, operation, opts}, _from, state) do
    with true <- is_function(operation, 0),
         {:ok, execution_opts} <- execution_options(opts) do
      case Map.fetch(state.jobs, id) do
        {:ok, job} ->
          {:reply, {:ok, snapshot(job)}, state}

        :error ->
          job = new_job(id, operation, execution_opts)
          {state, job} = start_job(state, job)
          {:reply, {:ok, snapshot(job)}, state}
      end
    else
      false -> {:reply, {:error, :operation_must_be_zero_arity}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:status, _from, state) do
    result = Map.new(state.jobs, fn {id, job} -> {id, snapshot(job)} end)
    {:reply, result, state}
  end

  def handle_call({:operation_status, id}, _from, state) do
    reply =
      case Map.fetch(state.jobs, id) do
        {:ok, job} -> {:ok, snapshot(job)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    result =
      state.jobs
      |> Enum.filter(fn {_id, job} -> job.status in @active_statuses end)
      |> Map.new(fn {id, job} -> {id, snapshot(job)} end)

    {:reply, result, state}
  end

  def handle_call({:schedule_periodic, id, interval_ms, operation, opts}, _from, state) do
    with true <- is_integer(interval_ms) and interval_ms > 0,
         true <- is_function(operation, 0),
         {:ok, execution_opts} <- execution_options(opts) do
      if Map.has_key?(state.schedules, id) do
        {:reply, {:error, :already_scheduled}, state}
      else
        timer = Process.send_after(self(), {:periodic_tick, id}, interval_ms)

        schedule = %{
          interval_ms: interval_ms,
          operation: operation,
          execution_opts: execution_opts,
          sequence: 1,
          timer: timer
        }

        {:reply, :ok, put_in(state.schedules[id], schedule)}
      end
    else
      false -> {:reply, {:error, :invalid_schedule}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_periodic, id}, _from, state) do
    case Map.pop(state.schedules, id) do
      {nil, _schedules} ->
        {:reply, {:error, :not_found}, state}

      {schedule, schedules} ->
        Process.cancel_timer(schedule.timer)
        {:reply, :ok, %{state | schedules: schedules}}
    end
  end

  @impl true
  def handle_info({:periodic_tick, id}, state) do
    case Map.fetch(state.schedules, id) do
      :error ->
        {:noreply, state}

      {:ok, schedule} ->
        run_id = {:periodic, id, schedule.sequence}
        job = new_job(run_id, schedule.operation, schedule.execution_opts)
        state = if Map.has_key?(state.jobs, run_id), do: state, else: elem(start_job(state, job), 0)

        next_schedule = %{schedule | sequence: schedule.sequence + 1}
        timer = Process.send_after(self(), {:periodic_tick, id}, schedule.interval_ms)
        next_schedule = %{next_schedule | timer: timer}

        {:noreply, put_in(state.schedules[id], next_schedule)}
    end
  end

  def handle_info({:operation_result, id, result}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :running} = job} ->
        Process.demonitor(job.monitor, [:flush])
        state = %{state | monitors: Map.delete(state.monitors, job.monitor)}
        {:noreply, finish_job(state, job, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_operation, id}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :retry_wait} = job} ->
        {state, _job} = start_job(state, job)
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.fetch(state.jobs, id) do
          {:ok, %{status: :running} = job} ->
            job = %{job | monitor: nil}
            {:noreply, retry_or_fail(state, job, {:worker_exit, reason})}

          _ ->
            {:noreply, state}
        end
    end
  end

  defp execution_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      max_attempts = Keyword.get(opts, :max_attempts, 3)
      retry_delay = Keyword.get(opts, :retry_delay, 1_000)

      if is_integer(max_attempts) and max_attempts > 0 and
           is_integer(retry_delay) and retry_delay >= 0 do
        {:ok, %{max_attempts: max_attempts, retry_delay: retry_delay}}
      else
        {:error, :invalid_execution_options}
      end
    else
      {:error, :invalid_execution_options}
    end
  end

  defp execution_options(_opts), do: {:error, :invalid_execution_options}

  defp new_job(id, operation, execution_opts) do
    %{
      id: id,
      operation: operation,
      status: :queued,
      attempt: 0,
      max_attempts: execution_opts.max_attempts,
      retry_delay: execution_opts.retry_delay,
      last_error: nil,
      result: nil,
      started_at: nil,
      finished_at: nil,
      monitor: nil
    }
  end

  defp start_job(state, job) do
    parent = self()
    attempt = job.attempt + 1
    id = job.id
    operation = job.operation

    {pid, monitor} =
      spawn_monitor(fn ->
        send(parent, {:operation_result, id, invoke(operation)})
      end)

    job = %{
      job
      | status: :running,
        attempt: attempt,
        started_at: now(),
        finished_at: nil,
        monitor: monitor
    }

    state = %{
      state
      | jobs: Map.put(state.jobs, id, job),
        monitors: Map.put(state.monitors, monitor, id)
    }

    _ = pid
    {state, job}
  end

  defp invoke(operation) do
    try do
      case operation.() do
        {:error, {:temporary, reason}} -> {:temporary, reason}
        {:error, :temporary} -> {:temporary, :temporary}
        {:error, reason} -> {:failure, reason}
        result -> {:success, result}
      end
    rescue
      error ->
        {:temporary, {:exception, error.__struct__, Exception.message(error)}}
    catch
      kind, reason ->
        {:temporary, {kind, reason}}
    end
  end

  defp finish_job(state, job, {:success, result}) do
    job = %{
      job
      | status: :succeeded,
        result: result,
        last_error: nil,
        finished_at: now(),
        monitor: nil
    }

    put_in(state.jobs[job.id], job)
  end

  defp finish_job(state, job, {:failure, reason}) do
    fail_job(state, job, reason)
  end

  defp finish_job(state, job, {:temporary, reason}) do
    retry_or_fail(state, job, reason)
  end

  defp retry_or_fail(state, job, reason) do
    if job.attempt < job.max_attempts do
      exponent = min(job.attempt - 1, 10)
      delay = min(job.retry_delay * :erlang.bsl(1, exponent), @max_retry_delay)
      job = %{job | status: :retry_wait, last_error: reason, monitor: nil}
      Process.send_after(self(), {:retry_operation, job.id}, delay)
      put_in(state.jobs[job.id], job)
    else
      fail_job(state, job, reason)
    end
  end

  defp fail_job(state, job, reason) do
    job = %{
      job
      | status: :failed,
        last_error: reason,
        finished_at: now(),
        monitor: nil
    }

    put_in(state.jobs[job.id], job)
  end

  defp snapshot(job) do
    Map.take(job, [
      :id,
      :status,
      :attempt,
      :max_attempts,
      :last_error,
      :result,
      :started_at,
      :finished_at
    ])
  end

  defp now, do: System.system_time(:millisecond)
end
