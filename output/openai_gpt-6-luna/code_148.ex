# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.OrderManager do
  use GenServer

  @default_ttl 86_400_000
  @default_retry_delay 1_000
  @default_max_retry_delay 30_000
  @default_max_attempts 5
  @default_maintenance_interval 1_000
  @default_periodic_interval 60_000

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def submit(id, payload, server \\ __MODULE__) do
    GenServer.call(server, {:submit, id, payload})
  end

  def status(id, server \\ __MODULE__) do
    GenServer.call(server, {:status, id})
  end

  def all(server \\ __MODULE__) do
    GenServer.call(server, :all)
  end

  @impl true
  def init(opts) do
    state = %{
      operations: %{},
      execute: Keyword.get(opts, :execute, fn _id, _payload -> {:ok, :accepted} end),
      periodic_fun: Keyword.get(opts, :periodic_fun),
      periodic_interval:
        Keyword.get(opts, :periodic_interval, @default_periodic_interval),
      maintenance_interval:
        Keyword.get(opts, :maintenance_interval, @default_maintenance_interval),
      record_ttl: Keyword.get(opts, :record_ttl, @default_ttl),
      retry_delay: Keyword.get(opts, :retry_delay, @default_retry_delay),
      max_retry_delay:
        Keyword.get(opts, :max_retry_delay, @default_max_retry_delay),
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
      periodic_pid: nil,
      periodic_monitor: nil,
      periodic_last_result: nil
    }

    schedule_maintenance(state.maintenance_interval)
    schedule_periodic(state.periodic_interval)

    {:ok, state}
  end

  @impl true
  def handle_call({:submit, id, payload}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} ->
        {:reply, {:duplicate, public_operation(operation)}, state}

      :error ->
        operation = %{
          id: id,
          payload: payload,
          status: :queued,
          attempts: 0,
          result: nil,
          error: nil,
          expires_at: nil,
          retry_token: nil,
          pid: nil,
          monitor: nil
        }

        state = put_in(state.operations[id], operation)
        {state, operation} = start_attempt(state, id, operation)

        {:reply, {:ok, public_operation(operation)}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} ->
        {:reply, {:ok, public_operation(operation)}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:all, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.map(&public_operation/1)

    periodic_status = if state.periodic_pid, do: :running, else: :idle

    {:reply,
     %{
       operations: operations,
       periodic: %{
         status: periodic_status,
         last_result: state.periodic_last_result
       }
     }, state}
  end

  @impl true
  def handle_info({:operation_result, id, pid, result}, state) do
    case Map.fetch(state.operations, id) do
      {:ok, %{pid: ^pid, monitor: monitor} = operation} ->
        Process.demonitor(monitor, [:flush])
        operation = %{operation | pid: nil, monitor: nil}
        state = put_in(state.operations[id], operation)

        case normalize_result(result) do
          {:ok, value} ->
            operation = terminal_operation(operation, :succeeded, value, nil, state.record_ttl)
            {:noreply, put_in(state.operations[id], operation)}

          {:temporary_error, reason} ->
            {:noreply, retry_or_fail(state, id, operation, reason)}

          {:permanent_error, reason} ->
            operation =
              terminal_operation(operation, :failed, nil, reason, state.record_ttl)

            {:noreply, put_in(state.operations[id], operation)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_operation, id, token}, state) do
    case Map.fetch(state.operations, id) do
      {:ok, %{status: :retry_wait, retry_token: ^token} = operation} ->
        {state, _operation} = start_attempt(state, id, operation)
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:periodic_result, pid, result}, %{periodic_pid: pid} = state) do
    Process.demonitor(state.periodic_monitor, [:flush])

    {:noreply,
     %{
       state
       | periodic_pid: nil,
         periodic_monitor: nil,
         periodic_last_result: result
     }}
  end

  def handle_info({:periodic_tick}, state) do
    state =
      if state.periodic_fun && is_nil(state.periodic_pid) do
        start_periodic_operation(state)
      else
        state
      end

    schedule_periodic(state.periodic_interval)
    {:noreply, state}
  end

  def handle_info(:maintenance, state) do
    now = System.system_time(:millisecond)

    operations =
      state.operations
      |> Enum.reject(fn {_id, operation} ->
        operation.expires_at && operation.expires_at <= now
      end)
      |> Map.new()

    schedule_maintenance(state.maintenance_interval)
    {:noreply, %{state | operations: operations}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    cond do
      state.periodic_monitor == ref and state.periodic_pid == pid ->
        {:noreply,
         %{
           state
           | periodic_pid: nil,
             periodic_monitor: nil,
             periodic_last_result: {:error, {:worker_exit, reason}}
         }}

      true ->
        case Enum.find(state.operations, fn {_id, operation} ->
               operation.monitor == ref and operation.pid == pid
             end) do
          {id, operation} ->
            operation = %{operation | pid: nil, monitor: nil}
            state = put_in(state.operations[id], operation)
            {:noreply, retry_or_fail(state, id, operation, {:worker_exit, reason})}

          nil ->
            {:noreply, state}
        end
    end
  end

  defp start_attempt(state, id, operation) do
    parent = self()
    execute = state.execute
    payload = operation.payload
    attempt = operation.attempts + 1

    {pid, monitor} =
      spawn_monitor(fn ->
        result = safe_call(fn -> execute.(id, payload) end)
        send(parent, {:operation_result, id, self(), result})
      end)

    operation = %{
      operation
      | status: :running,
        attempts: attempt,
        pid: pid,
        monitor: monitor,
        retry_token: nil,
        error: nil
    }

    {put_in(state.operations[id], operation), operation}
  end

  defp retry_or_fail(state, id, operation, reason) do
    if operation.attempts < state.max_attempts do
      token = make_ref()
      delay = retry_delay(state.retry_delay, state.max_retry_delay, operation.attempts)
      Process.send_after(self(), {:retry_operation, id, token}, delay)

      operation = %{
        operation
        | status: :retry_wait,
          retry_token: token,
          error: reason,
          expires_at: nil
      }

      put_in(state.operations[id], operation)
    else
      operation = terminal_operation(operation, :failed, nil, reason, state.record_ttl)
      put_in(state.operations[id], operation)
    end
  end

  defp terminal_operation(operation, status, result, error, ttl) do
    %{
      operation
      | status: status,
        result: result,
        error: error,
        expires_at: System.system_time(:millisecond) + ttl,
        retry_token: nil,
        pid: nil,
        monitor: nil
    }
  end

  defp retry_delay(base, maximum, attempts) do
    multiplier = :math.pow(2, max(attempts - 1, 0))
    min(trunc(base * multiplier), maximum)
  end

  defp start_periodic_operation(state) do
    parent = self()
    periodic_fun = state.periodic_fun

    {pid, monitor} =
      spawn_monitor(fn ->
        result = safe_call(fn -> invoke_periodic(periodic_fun) end)
        send(parent, {:periodic_result, self(), result})
      end)

    %{state | periodic_pid: pid, periodic_monitor: monitor}
  end

  defp invoke_periodic(fun) do
    case :erlang.fun_info(fun, :arity) do
      {:arity, 0} -> fun.()
      {:arity, 1} -> fun.(System.system_time(:millisecond))
    end
  end

  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error -> {:error, {:temporary, {:exception, error}}}
    catch
      kind, reason -> {:error, {:temporary, {kind, reason}}}
    end
  end

  defp normalize_result({:ok, value}), do: {:ok, value}
  defp normalize_result({:error, :temporary}), do: {:temporary_error, :temporary}
  defp normalize_result({:error, {:temporary, reason}}), do: {:temporary_error, reason}
  defp normalize_result({:error, reason}), do: {:permanent_error, reason}
  defp normalize_result(value), do: {:ok, value}

  defp public_operation(operation) do
    Map.take(operation, [
      :id,
      :payload,
      :status,
      :attempts,
      :result,
      :error,
      :expires_at
    ])
  end

  defp schedule_maintenance(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :maintenance, interval)
  end

  defp schedule_maintenance(_interval), do: :ok

  defp schedule_periodic(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), {:periodic_tick}, interval)
  end

  defp schedule_periodic(_interval), do: :ok
end
