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

  @max_retry_delay_ms 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def submit(server, operation_id, fun, opts \\ []) when is_function(fun, 0) do
    GenServer.call(server, {:submit, operation_id, fun, opts})
  end

  def get_status(server, operation_id) do
    GenServer.call(server, {:get_status, operation_id})
  end

  def list_operations(server) do
    GenServer.call(server, :list_operations)
  end

  def add_periodic(server, job_id, interval_ms, fun, opts \\ [])
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    GenServer.call(server, {:add_periodic, job_id, interval_ms, fun, opts})
  end

  def remove_periodic(server, job_id) do
    GenServer.call(server, {:remove_periodic, job_id})
  end

  def list_periodic(server) do
    GenServer.call(server, :list_periodic)
  end

  @impl true
  def init(:ok) do
    {:ok, %{operations: %{}, monitors: %{}, jobs: %{}}}
  end

  @impl true
  def handle_call({:submit, operation_id, fun, opts}, _from, state) do
    cond do
      not is_function(fun, 0) ->
        {:reply, {:error, :invalid_operation}, state}

      Map.has_key?(state.operations, operation_id) ->
        {:reply, {:error, :already_submitted}, state}

      true ->
        operation = new_operation(operation_id, fun, opts, nil)
        state = put_in(state.operations[operation_id], operation)
        {:reply, {:ok, :started}, start_attempt(state, operation_id)}
    end
  end

  def handle_call({:get_status, operation_id}, _from, state) do
    reply =
      case Map.fetch(state.operations, operation_id) do
        {:ok, operation} -> {:ok, operation_summary(operation)}
        :error -> :error
      end

    {:reply, reply, state}
  end

  def handle_call(:list_operations, _from, state) do
    operations = Enum.map(state.operations, fn {_id, operation} -> operation_summary(operation) end)
    {:reply, operations, state}
  end

  def handle_call({:add_periodic, job_id, interval_ms, fun, opts}, _from, state) do
    cond do
      not is_function(fun, 0) ->
        {:reply, {:error, :invalid_operation}, state}

      Map.has_key?(state.jobs, job_id) ->
        {:reply, {:error, :already_exists}, state}

      true ->
        timer = Process.send_after(self(), {:periodic_tick, job_id}, interval_ms)

        job = %{
          id: job_id,
          interval_ms: interval_ms,
          fun: fun,
          opts: normalize_opts(opts),
          sequence: 0,
          active_operation: nil,
          timer: timer
        }

        {:reply, :ok, put_in(state.jobs[job_id], job)}
    end
  end

  def handle_call({:remove_periodic, job_id}, _from, state) do
    case Map.pop(state.jobs, job_id) do
      {nil, _jobs} ->
        {:reply, :error, state}

      {job, jobs} ->
        Process.cancel_timer(job.timer)
        {:reply, :ok, %{state | jobs: jobs}}
    end
  end

  def handle_call(:list_periodic, _from, state) do
    jobs =
      Enum.map(state.jobs, fn {_id, job} ->
        %{
          id: job.id,
          interval_ms: job.interval_ms,
          sequence: job.sequence,
          active_operation: job.active_operation
        }
      end)

    {:reply, jobs, state}
  end

  @impl true
  def handle_info({:periodic_tick, job_id}, state) do
    case Map.fetch(state.jobs, job_id) do
      :error ->
        {:noreply, state}

      {:ok, job} ->
        timer = Process.send_after(self(), {:periodic_tick, job_id}, job.interval_ms)
        sequence = job.sequence + 1
        operation_id = {:periodic, job_id, sequence}

        job = %{job | timer: timer, sequence: sequence}
        state = put_in(state.jobs[job_id], job)

        if is_nil(job.active_operation) do
          operation = new_operation(operation_id, job.fun, job.opts, job_id)
          state = put_in(state.operations[operation_id], operation)
          state = put_in(state.jobs[job_id].active_operation, operation_id)
          {:noreply, start_attempt(state, operation_id)}
        else
          {:noreply, state}
        end
    end
  end

  def handle_info({:retry_operation, operation_id}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: :retry_wait} = operation} ->
        state = put_in(state.operations[operation_id], %{operation | retry_timer: nil})
        {:noreply, start_attempt(state, operation_id)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:operation_result, operation_id, monitor, result}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: :running, monitor: ^monitor} = operation} ->
        Process.demonitor(monitor, [:flush])

        state = %{
          state
          | monitors: Map.delete(state.monitors, monitor),
            operations:
              Map.put(state.operations, operation_id, %{operation | monitor: nil})
        }

        {:noreply, resolve_attempt(state, operation_id, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {operation_id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.fetch(state.operations, operation_id) do
          {:ok, %{status: :running, monitor: ^monitor} = operation} ->
            state = put_in(state.operations[operation_id], %{operation | monitor: nil})
            {:noreply, resolve_attempt(state, operation_id, {:error, {:temporary, {:worker_down, reason}}})}

          _ ->
            {:noreply, state}
        end
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.jobs, fn {_id, job} -> Process.cancel_timer(job.timer) end)

    Enum.each(state.operations, fn {_id, operation} ->
      if operation.retry_timer, do: Process.cancel_timer(operation.retry_timer)
    end)

    :ok
  end

  defp new_operation(id, fun, opts, job_id) do
    %{
      id: id,
      fun: fun,
      opts: normalize_opts(opts),
      job_id: job_id,
      status: :pending,
      attempts: 0,
      monitor: nil,
      retry_timer: nil,
      result: nil,
      last_error: nil,
      inserted_at: now_ms(),
      updated_at: now_ms()
    }
  end

  defp normalize_opts(opts) do
    max_retries = Keyword.get(opts, :max_retries, 3)
    retry_delay_ms = Keyword.get(opts, :retry_delay_ms, 500)

    %{
      max_retries: if(is_integer(max_retries) and max_retries >= 0, do: max_retries, else: 3),
      retry_delay_ms:
        if(is_integer(retry_delay_ms) and retry_delay_ms >= 0, do: retry_delay_ms, else: 500)
    }
  end

  defp start_attempt(state, operation_id) do
    operation = Map.fetch!(state.operations, operation_id)
    parent = self()
    monitor_ref = make_ref()

    {_pid, monitor} =
      spawn_monitor(fn ->
        result = safely_execute(operation.fun)
        send(parent, {:operation_result, operation_id, monitor_ref, result})
      end)

    # The worker message uses a unique reference so stale results cannot update an operation.
    Process.demonitor(monitor, [:flush])
    {_pid, monitor} =
      spawn_monitor(fn ->
        result = safely_execute(operation.fun)
        send(parent, {:operation_result, operation_id, monitor, result})
      end)

    updated = %{
      operation
      | status: :running,
        attempts: operation.attempts + 1,
        monitor: monitor,
        retry_timer: nil,
        updated_at: now_ms()
    }

    %{
      state
      | operations: Map.put(state.operations, operation_id, updated),
        monitors: Map.put(state.monitors, monitor, operation_id)
    }
  end

  defp safely_execute(fun) do
    try do
      fun.()
    rescue
      exception ->
        {:error, {:temporary, {:exception, exception.__struct__, Exception.message(exception)}}}
    catch
      kind, reason ->
        {:error, {:temporary, {kind, reason}}}
    end
  end

  defp resolve_attempt(state, operation_id, {:error, {:temporary, reason}}) do
    operation = Map.fetch!(state.operations, operation_id)

    if operation.attempts <= operation.opts.max_retries do
      delay =
        min(
          round(operation.opts.retry_delay_ms * :math.pow(2, operation.attempts - 1)),
          @max_retry_delay_ms
        )

      timer = Process.send_after(self(), {:retry_operation, operation_id}, delay)

      updated = %{
        operation
        | status: :retry_wait,
          retry_timer: timer,
          last_error: reason,
          updated_at: now_ms()
      }

      put_in(state.operations[operation_id], updated)
    else
      finish_operation(state, operation, :failed, {:error, reason}, reason)
    end
  end

  defp resolve_attempt(state, operation_id, {:error, reason}) do
    operation = Map.fetch!(state.operations, operation_id)
    finish_operation(state, operation, :failed, {:error, reason}, reason)
  end

  defp resolve_attempt(state, operation_id, result) do
    operation = Map.fetch!(state.operations, operation_id)
    finish_operation(state, operation, :completed, result, nil)
  end

  defp finish_operation(state, operation, status, result, error) do
    updated = %{
      operation
      | status: status,
        result: result,
        last_error: error,
        monitor: nil,
        retry_timer: nil,
        updated_at: now_ms()
    }

    state = put_in(state.operations[operation.id], updated)

    case operation.job_id do
      nil ->
        state

      job_id ->
        case Map.fetch(state.jobs, job_id) do
          {:ok, %{active_operation: operation_id} = job} when operation_id == operation.id ->
            put_in(state.jobs[job_id].active_operation, nil)

          _ ->
            state
        end
    end
  end

  defp operation_summary(operation) do
    Map.take(operation, [
      :id,
      :status,
      :attempts,
      :result,
      :last_error,
      :inserted_at,
      :updated_at
    ])
  end

  defp now_ms, do: System.system_time(:millisecond)
end
