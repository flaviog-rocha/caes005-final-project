# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, devendo ter os seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Permitir consultar o estado atual das operações em andamento.
# 6. Permitir comunicação entre diferentes processos.
#
# O código deve ser funcional e organizado. No início do código, coloque um comentário com exatamente esse prompt, excluindo apenas este trecho final. Não faça nada além do que foi solicitado.

# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, devendo ter os seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Permitir consultar o estado atual das operações em andamento.
# 6. Permitir comunicação entre diferentes processos.
#
# O código deve ser funcional e organizado. No início do código, coloque um comentário com exatamente esse prompt, excluindo apenas este trecho final.

defmodule NotificationManager do
  use GenServer

  @default_task_supervisor NotificationManager.TaskSupervisor
  @default_tick_ms 1_000

  # Inicia o gerenciador. Normalmente, ele é iniciado por NotificationManager.Supervisor.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, server_options(name))
  end

  def subscribe(manager \\ __MODULE__, pid \\ self()) do
    GenServer.call(manager, {:subscribe, pid})
  end

  def unsubscribe(manager \\ __MODULE__, pid \\ self()) do
    GenServer.call(manager, {:unsubscribe, pid})
  end

  def status(manager \\ __MODULE__) do
    GenServer.call(manager, :status)
  end

  @impl true
  def init(opts) do
    jobs =
      opts
      |> Keyword.get(:jobs, [])
      |> Map.new(fn job ->
        id = Keyword.fetch!(job, :id)
        interval_ms = Keyword.fetch!(job, :interval_ms)
        operation = Keyword.fetch!(job, :operation)
        notify_when = Keyword.get(job, :notify_when, fn _value -> false end)

        unless is_integer(interval_ms) and interval_ms > 0 do
          raise ArgumentError, "interval_ms deve ser um inteiro positivo"
        end

        unless is_function(operation, 1) and is_function(notify_when, 1) do
          raise ArgumentError, "operation e notify_when devem ser funções de aridade 1"
        end

        {id,
         %{
           interval_ms: interval_ms,
           operation: operation,
           notify_when: notify_when,
           value: Keyword.get(job, :initial_value),
           status: :idle,
           last_error: nil,
           next_run_at: 0
         }}
      end)

    state = %{
      jobs: jobs,
      running: %{},
      subscribers: %{},
      subscriber_refs: %{},
      max_concurrency: Keyword.get(opts, :max_concurrency, 5),
      task_supervisor: Keyword.get(opts, :task_supervisor, @default_task_supervisor),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms)
    }

    validate_positive_option!(state.max_concurrency, :max_concurrency)
    validate_positive_option!(state.tick_ms, :tick_ms)

    Process.send_after(self(), :tick, 0)
    {:ok, state}
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) when is_pid(pid) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)

      state = %{
        state
        | subscribers: Map.put(state.subscribers, pid, ref),
          subscriber_refs: Map.put(state.subscriber_refs, ref, pid)
      }

      {:reply, :ok, state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])

        {:reply, :ok,
         %{
           state
           | subscribers: subscribers,
             subscriber_refs: Map.delete(state.subscriber_refs, ref)
         }}
    end
  end

  def handle_call(:status, _from, state) do
    jobs =
      Map.new(state.jobs, fn {id, job} ->
        {id,
         %{
           status: job.status,
           value: job.value,
           last_error: job.last_error,
           next_run_at: job.next_run_at
         }}
      end)

    {:reply, %{jobs: jobs, running_count: map_size(state.running)}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = dispatch_due_jobs(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:operation_done, id, result}, state) do
    case Map.fetch(state.jobs, id) do
      {:ok, %{status: :running} = job} ->
        state = %{state | running: Map.delete(state.running, id)}

        case result do
          {:ok, new_value} ->
            updated_job = %{job | status: :idle, value: new_value, last_error: nil}
            state = put_in(state.jobs[id], updated_job)

            if safely_satisfies?(job.notify_when, new_value) do
              notify_subscribers(state, {:notification, id, new_value})
            end

            {:noreply, dispatch_due_jobs(state)}

          {:error, reason} ->
            updated_job = %{job | status: :idle, last_error: reason}
            state = put_in(state.jobs[id], updated_job)
            {:noreply, dispatch_due_jobs(state)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.pop(state.subscriber_refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {_subscriber, refs} ->
        {:noreply,
         %{
           state
           | subscribers: Map.delete(state.subscribers, pid),
             subscriber_refs: refs
         }}
    end
  end

  defp dispatch_due_jobs(state) do
    now = System.monotonic_time(:millisecond)
    available_slots = state.max_concurrency - map_size(state.running)

    due_ids =
      state.jobs
      |> Enum.filter(fn {_id, job} ->
        job.status == :idle and job.next_run_at <= now
      end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.take(max(available_slots, 0))

    Enum.reduce(due_ids, state, fn id, acc ->
      job = acc.jobs[id]
      manager = self()

      task =
        Task.Supervisor.start_child(acc.task_supervisor, fn ->
          result = safely_run(job.operation, job.value)
          send(manager, {:operation_done, id, result})
        end)

      next_run_at = now + job.interval_ms

      case task do
        {:ok, pid} ->
          updated_job = %{job | status: :running, next_run_at: next_run_at}

          %{
            acc
            | jobs: Map.put(acc.jobs, id, updated_job),
              running: Map.put(acc.running, id, pid)
          }

        {:error, reason} ->
          updated_job = %{
            job
            | last_error: {:task_start_failed, reason},
              next_run_at: next_run_at
          }

          %{acc | jobs: Map.put(acc.jobs, id, updated_job)}
      end
    end)
  end

  defp safely_run(operation, value) do
    result =
      try do
        operation.(value)
      rescue
        error -> {:error, {:exception, error}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      {:ok, _new_value} -> result
      {:error, _reason} -> result
      other -> {:error, {:invalid_result, other}}
    end
  end

  defp safely_satisfies?(condition, value) do
    try do
      condition.(value) == true
    rescue
      _error -> false
    catch
      _kind, _reason -> false
    end
  end

  defp notify_subscribers(state, message) do
    Enum.each(state.subscribers, fn {pid, _ref} ->
      send(pid, message)
    end)
  end

  defp validate_positive_option!(value, _name)
       when is_integer(value) and value > 0,
       do: :ok

  defp validate_positive_option!(_value, name) do
    raise ArgumentError, "#{name} deve ser um inteiro positivo"
  end

  defp server_options(nil), do: []
  defp server_options(name), do: [name: name]
end

defmodule NotificationManager.Supervisor do
  use Elixir.Supervisor

  def start_link(opts \\ []) do
    task_supervisor = Keyword.get(opts, :task_supervisor, NotificationManager.TaskSupervisor)
    manager_name = Keyword.get(opts, :name, NotificationManager)
    supervisor_name = Keyword.get(opts, :supervisor_name, __MODULE__)

    manager_opts =
      opts
      |> Keyword.put(:task_supervisor, task_supervisor)
      |> Keyword.put(:name, manager_name)

    children = [
      {Task.Supervisor, name: task_supervisor},
      {NotificationManager, manager_opts}
    ]

    Elixir.Supervisor.start_link(children, strategy: :one_for_one, name: supervisor_name)
  end
end
