# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Detectar encerramentos inesperados de processos.
# 4. Garantir a consistência dos dados durante operações concorrentes.
# 5. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  use GenServer

  @default_ttl 86_400_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  def submit_vote(server, operation_id, voter_id, choice) do
    GenServer.call(server, {:submit_vote, operation_id, voter_id, choice})
  end

  def operation_status(server, operation_id) do
    GenServer.call(server, {:operation_status, operation_id})
  end

  def in_progress(server) do
    GenServer.call(server, :in_progress)
  end

  def tally(server) do
    GenServer.call(server, :tally)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    ttl = Keyword.get(opts, :ttl, @default_ttl)

    unless is_integer(ttl) and ttl > 0 do
      raise ArgumentError, "ttl must be a positive integer in milliseconds"
    end

    {:ok,
     %{
       ttl: ttl,
       operations: %{},
       tasks: %{},
       votes: %{},
       expiration_timer: nil
     }}
  end

  @impl true
  def handle_call({:submit_vote, operation_id, voter_id, choice}, _from, state) do
    state = state |> purge_expired() |> reschedule_expiration()

    if valid_identifier?(operation_id) do
      case Map.fetch(state.operations, operation_id) do
        {:ok, operation} ->
          {:reply, {:ok, :duplicate, public_operation(operation)}, state}

        :error ->
          operation = %{
            status: :running,
            result: nil,
            worker_pid: nil,
            started_at: System.system_time(:millisecond),
            completed_at: nil,
            expires_at: nil
          }

          state = put_in(state.operations[operation_id], operation)

          try do
            task = Task.async(fn -> validate_vote(voter_id, choice) end)

            operation =
              Map.put(operation, :worker_pid, task.pid)

            state =
              state
              |> put_in([:operations, operation_id], operation)
              |> put_in([:tasks, task.ref], operation_id)

            {:reply, {:ok, :accepted}, state}
          rescue
            error ->
              state =
                state
                |> finish_operation(operation_id, :failed, {:task_start_failed, error.__struct__})
                |> reschedule_expiration()

              {:reply, {:error, :task_start_failed}, state}
          end
      end
    else
      {:reply, {:error, :invalid_operation_id}, state}
    end
  end

  def handle_call({:operation_status, operation_id}, _from, state) do
    state = state |> purge_expired() |> reschedule_expiration()

    reply =
      case Map.fetch(state.operations, operation_id) do
        {:ok, operation} -> {:ok, public_operation(operation)}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    state = state |> purge_expired() |> reschedule_expiration()

    operations =
      state.operations
      |> Enum.filter(fn {_id, operation} -> operation.status == :running end)
      |> Enum.map(fn {id, operation} -> Map.put(public_operation(operation), :id, id) end)

    {:reply, operations, state}
  end

  def handle_call(:tally, _from, state) do
    tally =
      Enum.reduce(state.votes, %{}, fn {_voter_id, choice}, acc ->
        Map.update(acc, choice, 1, &(&1 + 1))
      end)

    {:reply, tally, state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {operation_id, tasks} ->
        Process.demonitor(ref, [:flush])

        state = %{state | tasks: tasks}

        state =
          case result do
            {:ok, voter_id, choice} ->
              if Map.has_key?(state.votes, voter_id) do
                finish_operation(state, operation_id, :rejected, :already_voted)
              else
                state
                |> put_in([:votes, voter_id], choice)
                |> finish_operation(operation_id, :completed, :vote_recorded)
              end

            {:error, reason} ->
              finish_operation(state, operation_id, :rejected, reason)

            other ->
              finish_operation(state, operation_id, :failed, {:invalid_worker_result, other})
          end

        {:noreply, reschedule_expiration(state)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {operation_id, tasks} ->
        state =
          %{state | tasks: tasks}
          |> finish_operation(:failed, {:worker_exit, reason}, operation_id)

        {:noreply, reschedule_expiration(state)}
    end
  end

  def handle_info(:expire_records, state) do
    state =
      state
      |> Map.put(:expiration_timer, nil)
      |> purge_expired()
      |> reschedule_expiration()

    {:noreply, state}
  end

  # Task.async links its worker to this process. Trapping exits lets the
  # monitor's DOWN message update the operation instead of terminating the server.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  defp validate_vote(voter_id, choice) do
    cond do
      not valid_identifier?(voter_id) -> {:error, :invalid_voter_id}
      not valid_identifier?(choice) -> {:error, :invalid_choice}
      true -> {:ok, voter_id, choice}
    end
  end

  defp valid_identifier?(value) when is_binary(value), do: byte_size(value) > 0
  defp valid_identifier?(value) when is_integer(value), do: true
  defp valid_identifier?(value) when is_atom(value) and not is_nil(value), do: true
  defp valid_identifier?(_value), do: false

  defp finish_operation(state, operation_id, status, result) do
    now = System.monotonic_time(:millisecond)

    update_in(state.operations[operation_id], fn
      nil ->
        nil

      operation ->
        %{
          operation
          | status: status,
            result: result,
            worker_pid: nil,
            completed_at: System.system_time(:millisecond),
            expires_at: now + state.ttl
        }
    end)
  end

  defp public_operation(operation) do
    %{
      status: operation.status,
      result: operation.result,
      worker_pid: operation.worker_pid,
      started_at: operation.started_at,
      completed_at: operation.completed_at
    }
  end

  defp purge_expired(state) do
    now = System.monotonic_time(:millisecond)

    operations =
      Enum.reject(state.operations, fn {_id, operation} ->
        operation.status != :running and operation.expires_at <= now
      end)
      |> Map.new()

    %{state | operations: operations}
  end

  defp reschedule_expiration(state) do
    if state.expiration_timer do
      Process.cancel_timer(state.expiration_timer)
    end

    next_expiration =
      state.operations
      |> Enum.map(fn {_id, operation} -> operation.expires_at end)
      |> Enum.reject(&is_nil/1)
      |> Enum.min(fn -> nil end)

    timer =
      case next_expiration do
        nil ->
          nil

        expires_at ->
          delay = max(expires_at - System.monotonic_time(:millisecond), 0)
          Process.send_after(self(), :expire_records, delay)
      end

    %{state | expiration_timer: timer}
  end
end
