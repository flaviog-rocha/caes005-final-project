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

  @task_supervisor Module.concat(__MODULE__, TaskSupervisor)
  @default_ttl 86_400_000
  @max_cleanup_interval 60_000

  def start_link(opts \\ []) do
    case Task.Supervisor.start_link(name: @task_supervisor) do
      {:ok, _supervisor} ->
        GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

      error ->
        error
    end
  end

  def vote(operation_id, voter_id, choice) do
    vote(__MODULE__, operation_id, voter_id, choice)
  end

  def vote(server, operation_id, voter_id, choice) do
    GenServer.call(server, {:vote, operation_id, voter_id, choice})
  end

  def operation_status(operation_id) do
    operation_status(__MODULE__, operation_id)
  end

  def operation_status(server, operation_id) do
    GenServer.call(server, {:operation_status, operation_id})
  end

  def operations do
    operations(__MODULE__)
  end

  def operations(server) do
    GenServer.call(server, :operations)
  end

  def results do
    results(__MODULE__)
  end

  def results(server) do
    GenServer.call(server, :results)
  end

  @impl true
  def init(opts) do
    ttl =
      case Keyword.get(opts, :ttl, @default_ttl) do
        value when is_integer(value) and value > 0 -> value
        _ -> @default_ttl
      end

    schedule_cleanup(ttl)

    {:ok,
     %{
       ttl: ttl,
       operations: %{},
       tasks: %{},
       votes: %{},
       tally: %{}
     }}
  end

  @impl true
  def handle_call({:vote, nil, _voter_id, _choice}, _from, state) do
    {:reply, {:error, :invalid_operation_id}, state}
  end

  def handle_call({:vote, operation_id, voter_id, choice}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: :in_progress}} ->
        {:reply, {:ok, :already_in_progress}, state}

      {:ok, %{status: status, result: result}} ->
        {:reply, {:ok, status, result}, state}

      :error ->
        task =
          Task.Supervisor.async_nolink(@task_supervisor, fn ->
            prepare_vote(voter_id, choice)
          end)

        now = now_ms()

        operation = %{
          status: :in_progress,
          result: nil,
          created_at: now,
          expires_at: nil
        }

        state = %{
          state
          | operations: Map.put(state.operations, operation_id, operation),
            tasks: Map.put(state.tasks, task.ref, operation_id)
        }

        {:reply, {:ok, :accepted}, state}
    end
  end

  def handle_call({:operation_status, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        {:reply, {:ok, public_operation(operation)}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:operations, _from, state) do
    operations =
      Map.new(state.operations, fn {operation_id, operation} ->
        {operation_id, public_operation(operation)}
      end)

    {:reply, operations, state}
  end

  def handle_call(:results, _from, state) do
    {:reply, state.tally, state}
  end

  @impl true
  def handle_info({ref, task_result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {operation_id, tasks} ->
        Process.demonitor(ref, [:flush])
        state = %{state | tasks: tasks}
        {:noreply, apply_task_result(state, operation_id, task_result)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {operation_id, tasks} ->
        state = %{state | tasks: tasks}
        result = {:error, {:task_exit, reason}}
        {:noreply, finish_operation(state, operation_id, :failed, result)}
    end
  end

  def handle_info(:cleanup, state) do
    now = now_ms()

    operations =
      Map.reject(state.operations, fn {_id, operation} ->
        operation.status != :in_progress and operation.expires_at <= now
      end)

    schedule_cleanup(state.ttl)
    {:noreply, %{state | operations: operations}}
  end

  defp apply_task_result(state, operation_id, {:ok, voter_id, choice}) do
    if Map.has_key?(state.votes, voter_id) do
      finish_operation(state, operation_id, :completed, {:error, :already_voted})
    else
      votes = Map.put(state.votes, voter_id, choice)
      tally = Map.update(state.tally, choice, 1, &(&1 + 1))
      state = %{state | votes: votes, tally: tally}
      finish_operation(state, operation_id, :completed, {:ok, :vote_recorded})
    end
  end

  defp apply_task_result(state, operation_id, {:error, reason}) do
    finish_operation(state, operation_id, :failed, {:error, reason})
  end

  defp apply_task_result(state, operation_id, other_result) do
    finish_operation(state, operation_id, :failed, {:error, {:unexpected_result, other_result}})
  end

  defp finish_operation(state, operation_id, status, result) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        operation = %{
          operation
          | status: status,
            result: result,
            expires_at: now_ms() + state.ttl
        }

        %{state | operations: Map.put(state.operations, operation_id, operation)}

      :error ->
        state
    end
  end

  defp prepare_vote(nil, _choice), do: {:error, :invalid_voter_id}
  defp prepare_vote(_voter_id, nil), do: {:error, :invalid_choice}
  defp prepare_vote(voter_id, choice), do: {:ok, voter_id, choice}

  defp public_operation(operation) do
    %{status: operation.status, result: operation.result}
  end

  defp schedule_cleanup(ttl) do
    Process.send_after(self(), :cleanup, min(ttl, @max_cleanup_interval))
  end

  defp now_ms do
    System.monotonic_time(:millisecond)
  end
end
