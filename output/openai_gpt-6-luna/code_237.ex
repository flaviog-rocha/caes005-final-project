# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue.DSL do
  @moduledoc """
  Macro para definir funções que enviam tarefas à fila.

  Exemplo:

      defmodule MyTasks do
        use TaskQueue.DSL

        def_task :sum, do: fn [a, b] -> a + b end
      end

      MyTasks.sum(queue, [2, 3])
  """

  defmacro __using__(_opts) do
    quote do
      import TaskQueue.DSL, only: [def_task: 2]
    end
  end

  defmacro def_task(name, do: fun) do
    quote do
      def unquote(name)(queue, args \\ []) do
        TaskQueue.enqueue(queue, fn -> (unquote(fun)).(args) end)
      end
    end
  end
end

defmodule TaskQueue do
  use GenServer

  @moduledoc """
  Fila concorrente de tarefas.

  Todas as alterações de estado são serializadas pelo GenServer. As tarefas
  executam em processos separados e podem ser reenviadas após falhas.
  """

  @public_fields [
    :id,
    :status,
    :attempts,
    :max_retries,
    :result,
    :error,
    :inserted_at,
    :updated_at
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "Enfileira uma função de aridade zero e retorna o identificador da tarefa."
  def enqueue(server, fun, opts \\ []) when is_function(fun, 0) do
    GenServer.call(server, {:enqueue, fun, opts})
  end

  @doc "Retorna os dados públicos de uma tarefa ou `{:error, :not_found}`."
  def get(server, id) do
    GenServer.call(server, {:get, id})
  end

  @doc """
  Aguarda a conclusão da tarefa.

  Retorna `{:ok, resultado}` em caso de sucesso ou `{:error, motivo}` em caso
  de falha definitiva.
  """
  def await(server, id, timeout \\ :infinity) do
    GenServer.call(server, {:await, id}, timeout)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       jobs: %{},
       queue: :queue.new(),
       running: %{},
       waiters: %{},
       max_concurrency: positive_integer(Keyword.get(opts, :max_concurrency, 4), 4),
       default_max_retries: non_negative_integer(Keyword.get(opts, :max_retries, 3), 3),
       retry_delay: non_negative_integer(Keyword.get(opts, :retry_delay, 250), 250),
       max_retry_delay:
         non_negative_integer(Keyword.get(opts, :max_retry_delay, 30_000), 30_000)
     }}
  end

  @impl true
  def handle_call({:enqueue, fun, opts}, _from, state) do
    id = make_ref()
    now = System.system_time(:millisecond)

    job = %{
      id: id,
      fun: fun,
      status: :queued,
      attempts: 0,
      max_retries:
        non_negative_integer(
          Keyword.get(opts, :max_retries, state.default_max_retries),
          state.default_max_retries
        ),
      retry_delay:
        non_negative_integer(Keyword.get(opts, :retry_delay, state.retry_delay), state.retry_delay),
      max_retry_delay:
        non_negative_integer(
          Keyword.get(opts, :max_retry_delay, state.max_retry_delay),
          state.max_retry_delay
        ),
      result: nil,
      error: nil,
      inserted_at: now,
      updated_at: now
    }

    state =
      state
      |> put_in([:jobs, id], job)
      |> update_in([:queue], &:queue.in(id, &1))
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, job} -> {:reply, {:ok, public_job(job)}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:await, id}, from, state) do
    case Map.fetch(state.jobs, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: :completed, result: result}} ->
        {:reply, {:ok, result}, state}

      {:ok, %{status: :failed, error: reason}} ->
        {:reply, {:error, reason}, state}

      {:ok, _job} ->
        waiters = Map.update(state.waiters, id, [from], &[from | &1])
        {:noreply, %{state | waiters: waiters}}
    end
  end

  @impl true
  def handle_info({:job_result, id, pid, result}, state) do
    case Map.get(state.running, pid) do
      %{id: ^id, monitor: monitor} ->
        Process.demonitor(monitor, [:flush])

        state =
          state
          |> update_in([:running], &Map.delete(&1, pid))
          |> handle_result(id, result)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, pid, reason}, state) do
    case Map.get(state.running, pid) do
      %{monitor: ^monitor, id: id} ->
        state =
          state
          |> update_in([:running], &Map.delete(&1, pid))
          |> handle_result(id, {:error, {:worker_exit, reason}})
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, id}, state) do
    case Map.get(state.jobs, id) do
      %{status: :retry_wait} = job ->
        job = %{job | status: :queued, updated_at: System.system_time(:millisecond)}

        state =
          state
          |> put_in([:jobs, id], job)
          |> update_in([:queue], &:queue.in(id, &1))
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch(state) do
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
              parent = self()

              {pid, monitor} =
                spawn_monitor(fn ->
                  result =
                    try do
                      job.fun.()
                    rescue
                      exception -> {:error, {:exception, exception, __STACKTRACE__}}
                    catch
                      kind, reason -> {:error, {kind, reason}}
                    end

                  send(parent, {:job_result, id, self(), result})
                end)

              job = %{
                job
                | status: :running,
                  attempts: job.attempts + 1,
                  updated_at: System.system_time(:millisecond)
              }

              state
              |> put_in([:jobs, id], job)
              |> update_in([:running], &Map.put(&1, pid, %{id: id, monitor: monitor}))
              |> dispatch()

            _ ->
              dispatch(state)
          end
      end
    end
  end

  defp handle_result(state, id, {:error, reason, :permanent}) do
    finish(state, id, :failed, nil, reason)
  end

  defp handle_result(state, id, {:error, reason}) do
    retry_or_finish(state, id, reason)
  end

  defp handle_result(state, id, result) do
    finish(state, id, :completed, result, nil)
  end

  defp retry_or_finish(state, id, reason) do
    job = Map.fetch!(state.jobs, id)

    if job.attempts <= job.max_retries do
      delay =
        min(
          job.retry_delay * trunc(:math.pow(2, max(job.attempts - 1, 0))),
          job.max_retry_delay
        )

      Process.send_after(self(), {:retry, id}, delay)

      job = %{
        job
        | status: :retry_wait,
          error: reason,
          updated_at: System.system_time(:millisecond)
      }

      put_in(state, [:jobs, id], job)
    else
      finish(state, id, :failed, nil, reason)
    end
  end

  defp finish(state, id, status, result, error) do
    job =
      state.jobs
      |> Map.fetch!(id)
      |> Map.merge(%{
        status: status,
        result: result,
        error: error,
        updated_at: System.system_time(:millisecond)
      })

    state = put_in(state, [:jobs, id], job)
    reply = if status == :completed, do: {:ok, result}, else: {:error, error}

    waiters = Map.get(state.waiters, id, [])
    Enum.each(waiters, &GenServer.reply(&1, reply))

    %{state | waiters: Map.delete(state.waiters, id)}
  end

  defp public_job(job), do: Map.take(job, @public_fields)

  defp positive_integer(value, _fallback) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, fallback), do: fallback

  defp non_negative_integer(value, _fallback)
       when is_integer(value) and value >= 0,
       do: value

  defp non_negative_integer(_value, fallback), do: fallback
end
