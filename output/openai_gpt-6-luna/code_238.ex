# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  use GenServer

  @moduledoc """
  Uma fila concorrente de tarefas com estado serializado por GenServer e novas
  tentativas para falhas temporárias.
  """

  # Uso: TaskQueue.task(fila, do: expressão)
  defmacro task(server, do: body) do
    quote do
      TaskQueue.enqueue(unquote(server), fn -> unquote(body) end)
    end
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def enqueue(server, fun, opts \\ []) when is_function(fun, 0) do
    GenServer.call(server, {:enqueue, fun, opts})
  end

  def status(server, id), do: GenServer.call(server, {:status, id})

  def init(opts) do
    {:ok,
     %{
       concurrency: Keyword.get(opts, :concurrency, 4),
       running: %{},
       tasks: %{},
       queue: :queue.new()
     }}
  end

  def handle_call({:enqueue, fun, opts}, _from, state) do
    id = make_ref()

    task = %{
      fun: fun,
      attempts: 0,
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      retry_delay: Keyword.get(opts, :retry_delay, 100),
      status: :pending,
      result: nil,
      retry_token: nil
    }

    state =
      state
      |> put_in([:tasks, id], task)
      |> update_in([:queue], &:queue.in(id, &1))
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:status, id}, _from, state) do
    case Map.fetch(state.tasks, id) do
      {:ok, task} ->
        {:reply, {:ok, public_task(task)}, state}

      :error ->
        {:reply, :error, state}
    end
  end

  def handle_info({:task_done, id, pid, outcome}, state) do
    case Map.get(state.running, id) do
      %{pid: ^pid, ref: ref} ->
        Process.demonitor(ref, [:flush])

        state =
          state
          |> update_in([:running], &Map.delete(&1, id))
          |> complete_task(id, outcome)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_id, running} -> running.ref == ref end) do
      {id, _running} ->
        state =
          state
          |> update_in([:running], &Map.delete(&1, id))
          |> complete_task(id, {:retry, {:worker_exit, reason}})
          |> dispatch()

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_task, id, token}, state) do
    case Map.get(state.tasks, id) do
      %{status: :retry_wait, retry_token: ^token} = task ->
        task = %{task | status: :pending, retry_token: nil}

        state =
          state
          |> put_in([:tasks, id], task)
          |> update_in([:queue], &:queue.in(id, &1))
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch(state) when map_size(state.running) >= state.concurrency, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {{:value, id}, queue} ->
        state = %{state | queue: queue}

        case Map.get(state.tasks, id) do
          %{status: :pending} = task ->
            server = self()
            attempts = task.attempts + 1
            task = %{task | status: :running, attempts: attempts}
            state = put_in(state, [:tasks, id], task)

            {pid, ref} =
              spawn_monitor(fn ->
                outcome =
                  try do
                    case task.fun.() do
                      {:error, :temporary} = error -> {:retry, error}
                      {:error, {:temporary, _reason}} = error -> {:retry, error}
                      {:error, reason} -> {:failure, reason}
                      result -> {:success, result}
                    end
                  rescue
                    error -> {:retry, {:exception, error}}
                  catch
                    kind, reason -> {:retry, {:caught, kind, reason}}
                  end

                send(server, {:task_done, id, self(), outcome})
              end)

            state
            |> put_in([:running, id], %{pid: pid, ref: ref})
            |> dispatch()

          _ ->
            dispatch(state)
        end

      {:empty, _queue} ->
        state
    end
  end

  defp complete_task(state, id, outcome) do
    case Map.get(state.tasks, id) do
      nil ->
        state

      task ->
        case outcome do
          {:success, result} ->
            put_in(state, [:tasks, id], %{task | status: :completed, result: result})

          {:failure, reason} ->
            put_in(state, [:tasks, id], %{task | status: :failed, result: {:error, reason}})

          {:retry, reason} ->
            schedule_retry(state, id, task, reason)
        end
    end
  end

  defp schedule_retry(state, id, task, reason) do
    if task.attempts < task.max_attempts do
      token = make_ref()
      delay = retry_delay(task.retry_delay, task.attempts)
      Process.send_after(self(), {:retry_task, id, token}, delay)

      put_in(state, [:tasks, id], %{
        task
        | status: :retry_wait,
          result: {:retrying, reason},
          retry_token: token
      })
    else
      put_in(state, [:tasks, id], %{task | status: :failed, result: {:error, reason}})
    end
  end

  defp retry_delay(base, attempts) do
    Enum.reduce(1..max(attempts - 1, 0), min(base, 60_000), fn _, delay ->
      min(delay * 2, 60_000)
    end)
  end

  defp public_task(task) do
    Map.take(task, [:attempts, :max_attempts, :status, :result])
  end
end
