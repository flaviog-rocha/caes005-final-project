# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Detectar encerramentos inesperados de processos.
# 3. Manter um histórico das operações realizadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  use GenServer

  @default_max_retries 3
  @default_retry_delay 1_000
  @default_periodic_interval 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def enqueue(server, fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    retry_delay = Keyword.get(opts, :retry_delay, @default_retry_delay)

    if is_integer(max_retries) and max_retries >= 0 and
         is_integer(retry_delay) and retry_delay >= 0 do
      GenServer.call(server, {:enqueue, fun, max_retries, retry_delay})
    else
      {:error, :invalid_job_options}
    end
  end

  def enqueue(_server, _fun, _opts), do: {:error, :invalid_job}

  def status(server), do: GenServer.call(server, :status)
  def history(server), do: GenServer.call(server, {:history, :infinity})
  def history(server, limit), do: GenServer.call(server, {:history, limit})

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 1)
    periodic_fun = Keyword.get(opts, :periodic_fun)
    periodic_interval = Keyword.get(opts, :periodic_interval, @default_periodic_interval)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    unless is_nil(periodic_fun) or is_function(periodic_fun, 0) do
      raise ArgumentError, "periodic_fun must be a zero-arity function"
    end

    if periodic_fun && (not is_integer(periodic_interval) or periodic_interval <= 0) do
      raise ArgumentError, "periodic_interval must be a positive integer"
    end

    state = %{
      jobs: %{},
      queue: :queue.new(),
      running: %{},
      monitors: %{},
      history: [],
      max_concurrency: max_concurrency,
      periodic_fun: periodic_fun,
      periodic_interval: periodic_interval,
      periodic_token: nil
    }

    if periodic_fun do
      Process.send_after(self(), :periodic_tick, periodic_interval)
    end

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, fun, max_retries, retry_delay}, _from, state) do
    id = make_ref()

    job = %{
      fun: fun,
      status: :queued,
      retries: 0,
      attempts: 0,
      max_retries: max_retries,
      retry_delay: retry_delay,
      retry_token: nil,
      last_error: nil,
      result: nil
    }

    state =
      state
      |> put_in([:jobs, id], job)
      |> update_in([:queue], &:queue.in(id, &1))
      |> record(:enqueued, id, %{})
      |> pump()

    {:reply, {:ok, id}, state}
  end

  def handle_call(:status, _from, state) do
    jobs =
      Map.new(state.jobs, fn {id, job} ->
        {id, Map.take(job, [:status, :retries, :attempts, :last_error, :result])}
      end)

    {:reply, %{jobs: jobs, running: map_size(state.running)}, state}
  end

  def handle_call({:history, limit}, _from, state) do
    events = Enum.reverse(state.history)

    events =
      case limit do
        :infinity -> events
        n when is_integer(n) and n > 0 -> Enum.take(events, -n)
        _ -> []
      end

    {:reply, events, state}
  end

  @impl true
  def handle_info({:job_result, token, result}, state) do
    case Map.pop(state.running, token) do
      {nil, _running} ->
        {:noreply, state}

      {%{id: id, monitor: monitor}, running} ->
        Process.demonitor(monitor, [:flush])

        state =
          state
          |> Map.put(:running, running)
          |> update_in([:monitors], &Map.delete(&1, monitor))
          |> complete_job(id, result)
          |> pump()

        {:noreply, state}
    end
  end

  def handle_info({:periodic_result, token, result}, state) do
    if state.periodic_token == token do
      monitor =
        Enum.find_value(state.monitors, fn
          {ref, {:periodic, ^token}} -> ref
          _ -> nil
        end)

      if monitor, do: Process.demonitor(monitor, [:flush])

      state =
        state
        |> update_in([:monitors], fn monitors ->
          if monitor, do: Map.delete(monitors, monitor), else: monitors
        end)
        |> Map.put(:periodic_token, nil)
        |> record_periodic_result(result)
        |> schedule_periodic()

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info(:periodic_tick, %{periodic_fun: fun, periodic_token: nil} = state)
      when is_function(fun, 0) do
    token = make_ref()
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result = invoke(fun)
        send(parent, {:periodic_result, token, result})
      end)

    state =
      state
      |> Map.put(:periodic_token, token)
      |> update_in([:monitors], &Map.put(&1, monitor, {:periodic, token}))
      |> record(:periodic_started, :periodic, %{pid: pid})

    {:noreply, state}
  end

  def handle_info(:periodic_tick, state), do: {:noreply, state}

  def handle_info({:retry_job, id, token}, state) do
    case Map.get(state.jobs, id) do
      %{status: :retry_wait, retry_token: ^token} = job ->
        job = %{job | status: :queued, retry_token: nil}

        state =
          state
          |> put_in([:jobs, id], job)
          |> update_in([:queue], &:queue.in(id, &1))
          |> record(:retry_ready, id, %{attempt: job.attempts})
          |> pump()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {{:job, token}, monitors} ->
        case Map.pop(state.running, token) do
          {nil, _running} ->
            {:noreply, %{state | monitors: monitors}}

          {%{id: id}, running} ->
            state =
              %{state | monitors: monitors, running: running}
              |> fail_job(id, {:process_down, reason})
              |> pump()

            {:noreply, state}
        end

      {{:periodic, token}, monitors} ->
        state = %{state | monitors: monitors}

        if state.periodic_token == token do
          state =
            state
            |> Map.put(:periodic_token, nil)
            |> record(:periodic_crashed, :periodic, %{reason: reason})
            |> schedule_periodic()
        end

        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp pump(state) do
    if map_size(state.running) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, id}, queue} ->
          state = %{state | queue: queue}

          case Map.get(state.jobs, id) do
            %{status: :queued} = job ->
              token = make_ref()
              parent = self()

              {pid, monitor} =
                spawn_monitor(fn ->
                  result = invoke(job.fun)
                  send(parent, {:job_result, token, result})
                end)

              job = %{job | status: :running, attempts: job.attempts + 1}

              state =
                state
                |> put_in([:jobs, id], job)
                |> update_in([:running], &Map.put(&1, token, %{id: id, monitor: monitor}))
                |> update_in([:monitors], &Map.put(&1, monitor, {:job, token}))
                |> record(:started, id, %{attempt: job.attempts, pid: pid})

              pump(state)

            _ ->
              pump(state)
          end
      end
    end
  end

  defp complete_job(state, id, {:returned, {:error, reason}}), do: fail_job(state, id, reason)

  defp complete_job(state, id, {:error, reason}), do: fail_job(state, id, reason)

  defp complete_job(state, id, {:returned, result}) do
    job = Map.fetch!(state.jobs, id)
    state = put_in(state.jobs[id], %{job | status: :succeeded, result: result})
    record(state, :succeeded, id, %{result: result, attempt: job.attempts})
  end

  defp fail_job(state, id, reason) do
    job = Map.fetch!(state.jobs, id)

    if job.retries < job.max_retries do
      token = make_ref()
      retries = job.retries + 1
      job = %{job | status: :retry_wait, retries: retries, retry_token: token, last_error: reason}

      Process.send_after(self(), {:retry_job, id, token}, job.retry_delay)

      state
      |> put_in([:jobs, id], job)
      |> record(:retry_scheduled, id, %{
        reason: reason,
        retry: retries,
        delay: job.retry_delay
      })
    else
      job = %{job | status: :failed, last_error: reason}

      state
      |> put_in([:jobs, id], job)
      |> record(:failed, id, %{reason: reason, attempt: job.attempts})
    end
  end

  defp invoke(fun) do
    try do
      {:returned, fun.()}
    rescue
      exception ->
        {:error, {:exception, exception.__struct__, Exception.message(exception)}}
    catch
      kind, reason ->
        {:error, {kind, reason}}
    end
  end

  defp record_periodic_result(state, {:returned, {:error, reason}}) do
    record(state, :periodic_failed, :periodic, %{reason: reason})
  end

  defp record_periodic_result(state, {:error, reason}) do
    record(state, :periodic_failed, :periodic, %{reason: reason})
  end

  defp record_periodic_result(state, {:returned, result}) do
    record(state, :periodic_succeeded, :periodic, %{result: result})
  end

  defp schedule_periodic(%{periodic_fun: fun} = state) when is_function(fun, 0) do
    Process.send_after(self(), :periodic_tick, state.periodic_interval)
    state
  end

  defp schedule_periodic(state), do: state

  defp record(state, action, id, details) do
    event = %{
      action: action,
      id: id,
      at: System.system_time(:millisecond),
      details: details
    }

    update_in(state.history, &[event | &1])
  end
end
