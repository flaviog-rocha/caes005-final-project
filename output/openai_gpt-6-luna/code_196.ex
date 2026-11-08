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

  defstruct max_concurrency: 1,
            history_limit: 1_000,
            queue: :queue.new(),
            running: %{},
            tokens: %{},
            history: [],
            periodic: %{}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def enqueue(server, fun, opts \\ []) do
    GenServer.call(server, {:enqueue, fun, opts})
  end

  def history(server) do
    GenServer.call(server, :history)
  end

  def schedule_periodic(server, interval_ms, fun, opts \\ []) do
    GenServer.call(server, {:schedule_periodic, interval_ms, fun, opts})
  end

  def cancel_periodic(server, periodic_id) do
    GenServer.call(server, {:cancel_periodic, periodic_id})
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 1)
    history_limit = Keyword.get(opts, :history_limit, 1_000)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    unless is_integer(history_limit) and history_limit >= 0 do
      raise ArgumentError, "history_limit deve ser um inteiro não negativo"
    end

    {:ok,
     %__MODULE__{
       max_concurrency: max_concurrency,
       history_limit: history_limit
     }}
  end

  @impl true
  def handle_call({:enqueue, fun, opts}, _from, state) do
    with true <- is_function(fun, 0),
         {:ok, task_opts} <- normalize_task_opts(opts) do
      {state, id} = enqueue_task(state, fun, task_opts)
      {:reply, {:ok, id}, dispatch(state)}
    else
      false -> {:reply, {:error, :task_must_be_zero_arity_function}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:schedule_periodic, interval_ms, fun, opts}, _from, state) do
    with true <- is_integer(interval_ms) and interval_ms > 0,
         true <- is_function(fun, 0),
         {:ok, task_opts} <- normalize_task_opts(opts) do
      id = make_ref()
      timer = Process.send_after(self(), {:periodic_tick, id}, interval_ms)

      periodic = %{
        interval_ms: interval_ms,
        fun: fun,
        task_opts: task_opts,
        timer: timer
      }

      state =
        state
        |> put_in([Access.key(:periodic), id], periodic)
        |> record(:periodic_scheduled, nil, %{interval_ms: interval_ms})

      {:reply, {:ok, id}, state}
    else
      false -> {:reply, {:error, :invalid_periodic_operation}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_periodic, id}, _from, state) do
    case Map.pop(state.periodic, id) do
      {nil, _periodic} ->
        {:reply, false, state}

      {periodic, remaining} ->
        Process.cancel_timer(periodic.timer)

        state =
          %{state | periodic: remaining}
          |> record(:periodic_cancelled, nil, %{periodic_id: id})

        {:reply, true, state}
    end
  end

  @impl true
  def handle_info({:periodic_tick, id}, state) do
    case Map.get(state.periodic, id) do
      nil ->
        {:noreply, state}

      periodic ->
        {state, _task_id} = enqueue_task(state, periodic.fun, periodic.task_opts)
        timer = Process.send_after(self(), {:periodic_tick, id}, periodic.interval_ms)
        periodic = %{periodic | timer: timer}
        state = put_in(state.periodic[id], periodic)
        {:noreply, dispatch(state)}
    end
  end

  def handle_info({:retry, task}, state) do
    state =
      state
      |> Map.update!(:queue, &:queue.in(task, &1))
      |> record(:retry_queued, task, %{attempt: task.attempt})

    {:noreply, dispatch(state)}
  end

  def handle_info({:task_result, token, result}, state) do
    case Map.pop(state.tokens, token) do
      {nil, _tokens} ->
        {:noreply, state}

      {monitor_ref, tokens} ->
        case Map.pop(state.running, monitor_ref) do
          {nil, running} ->
            {:noreply, %{state | tokens: tokens, running: running}}

          {running_task, running} ->
            Process.demonitor(monitor_ref, [:flush])

            state = %{
              state
              | tokens: tokens,
                running: running
            }

            state = finish_task(state, running_task.task, result)
            {:noreply, dispatch(state)}
        end
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.running, monitor_ref) do
      {nil, _running} ->
        {:noreply, state}

      {running_task, running} ->
        state = %{
          state
          | running: running,
            tokens: Map.delete(state.tokens, running_task.token)
        }

        state =
          finish_task(
            state,
            running_task.task,
            {:error, {:process_down, reason}}
          )

        {:noreply, dispatch(state)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp normalize_task_opts(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      max_attempts = Keyword.get(opts, :max_attempts, 3)
      retry_delay = Keyword.get(opts, :retry_delay, 1_000)

      if is_integer(max_attempts) and max_attempts > 0 and
           is_integer(retry_delay) and retry_delay >= 0 do
        {:ok, %{max_attempts: max_attempts, retry_delay: retry_delay}}
      else
        {:error, :invalid_retry_options}
      end
    else
      {:error, :options_must_be_a_keyword_list}
    end
  end

  defp normalize_task_opts(_opts), do: {:error, :options_must_be_a_keyword_list}

  defp enqueue_task(state, fun, task_opts) do
    id = make_ref()

    task =
      Map.merge(task_opts, %{
        id: id,
        fun: fun,
        attempt: 1
      })

    state =
      state
      |> Map.update!(:queue, &:queue.in(task, &1))
      |> record(:enqueued, task, nil)

    {state, id}
  end

  defp dispatch(state) do
    if map_size(state.running) >= state.max_concurrency or :queue.is_empty(state.queue) do
      state
    else
      {{:value, task}, queue} = :queue.out(state.queue)
      token = make_ref()
      parent = self()

      {pid, monitor_ref} =
        spawn_monitor(fn ->
          result =
            try do
              task.fun.()
            rescue
              error -> {:error, {:exception, error, __STACKTRACE__}}
            catch
              kind, reason -> {:error, {kind, reason}}
            end

          send(parent, {:task_result, token, result})
        end)

      running_task = %{task: task, pid: pid, token: token}

      state = %{
        state
        | queue: queue,
          running: Map.put(state.running, monitor_ref, running_task),
          tokens: Map.put(state.tokens, token, monitor_ref)
      }

      state
      |> record(:started, task, %{pid: pid})
      |> dispatch()
    end
  end

  defp finish_task(state, task, {:error, reason}) do
    if task.attempt < task.max_attempts do
      next_task = %{task | attempt: task.attempt + 1}
      Process.send_after(self(), {:retry, next_task}, task.retry_delay)

      record(state, :retry_scheduled, task, %{
        reason: reason,
        next_attempt: next_task.attempt,
        retry_delay: task.retry_delay
      })
    else
      record(state, :failed, task, %{reason: reason})
    end
  end

  defp finish_task(state, task, result) do
    record(state, :succeeded, task, %{result: result})
  end

  defp record(state, operation, task, details) do
    if state.history_limit == 0 do
      state
    else
      entry = %{
        at: DateTime.utc_now(),
        operation: operation,
        task_id: if(task, do: task.id, else: nil),
        attempt: if(task, do: task.attempt, else: nil),
        details: details
      }

      history =
        [entry | state.history]
        |> Enum.take(state.history_limit)

      %{state | history: history}
    end
  end
end
