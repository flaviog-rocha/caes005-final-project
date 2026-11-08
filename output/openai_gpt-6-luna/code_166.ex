# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Fazer uso de macros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir comunicação entre diferentes processos.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue.Macros do
  @moduledoc false

  defmacro task(name, do: body) do
    context = Macro.var(:context, nil)

    quote do
      def execute(unquote(name), unquote(context)) do
        unquote(body)
      end
    end
  end
end

defmodule TaskQueue.Operations do
  @moduledoc false
  require TaskQueue.Macros

  TaskQueue.Macros.task :echo do
    Map.get(context, :value)
  end

  TaskQueue.Macros.task :sum do
    Map.get(context, :left, 0) + Map.get(context, :right, 0)
  end

  TaskQueue.Macros.task :sleep_echo do
    Process.sleep(Map.get(context, :delay_ms, 0))
    Map.get(context, :value)
  end
end

defmodule TaskQueue.Database do
  @moduledoc """
  Banco de dados fictício em memória, usado para armazenar o último snapshot da fila.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))
  end

  def ensure_started do
    case Process.whereis(__MODULE__) do
      nil ->
        case start_link() do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          error -> error
        end

      pid ->
        {:ok, pid}
    end
  end

  def save(snapshot, server \\ __MODULE__) do
    GenServer.cast(server, {:save, snapshot})
  end

  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:save, snapshot}, _state) do
    {:noreply, %{snapshot: snapshot, saved_at: DateTime.utc_now()}}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, state, state}
  end
end

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas concorrente com consulta de estado, processamento periódico
  e persistência fictícia em memória.
  """

  use GenServer

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def submit(task_name, args \\ %{}, server \\ __MODULE__) do
    GenServer.call(server, {:submit, task_name, args})
  end

  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  def get(job_id, server \\ __MODULE__) do
    GenServer.call(server, {:get, job_id})
  end

  @impl true
  def init(opts) do
    case TaskQueue.Database.ensure_started() do
      {:ok, _pid} ->
        state = %{
          jobs: %{},
          queue: :queue.new(),
          active: %{},
          concurrency: max(Keyword.get(opts, :concurrency, 2), 1),
          interval_ms: max(Keyword.get(opts, :interval_ms, 5_000), 1),
          retention_ms: max(Keyword.get(opts, :retention_ms, 60_000), 0),
          maintenance_runs: 0
        }

        schedule_maintenance(state.interval_ms)
        persist(state)
        {:ok, state}

      error ->
        {:stop, {:database_start_failed, error}}
    end
  end

  @impl true
  def handle_call({:submit, task_name, args}, _from, state) when is_map(args) do
    id = System.unique_integer([:positive, :monotonic])

    job = %{
      id: id,
      task: task_name,
      args: args,
      status: :queued,
      submitted_at: DateTime.utc_now(),
      started_at: nil,
      completed_at: nil,
      result: nil
    }

    state =
      state
      |> put_in([:jobs, id], job)
      |> update_in([:queue], &:queue.in(id, &1))
      |> dispatch()

    persist(state)
    {:reply, {:ok, id}, state}
  end

  def handle_call({:submit, _task_name, _args}, _from, state) do
    {:reply, {:error, :arguments_must_be_a_map}, state}
  end

  def handle_call(:status, _from, state) do
    counts =
      Enum.reduce(state.jobs, %{queued: 0, running: 0, completed: 0, failed: 0}, fn
        {_id, %{status: status}}, acc -> Map.update(acc, status, 1, &(&1 + 1))
      end)

    reply = %{
      counts: counts,
      queued_ids:
        state.jobs
        |> Enum.filter(fn {_id, job} -> job.status == :queued end)
        |> Enum.map(&elem(&1, 0)),
      running_ids: Enum.map(state.active, fn {_pid, entry} -> entry.id end),
      concurrency: state.concurrency,
      maintenance_runs: state.maintenance_runs
    }

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    jobs =
      state.jobs
      |> Map.values()
      |> Enum.sort_by(& &1.submitted_at, DateTime)

    {:reply, jobs, state}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.fetch(state.jobs, id), state}
  end

  @impl true
  def handle_info({:worker_result, id, pid, result}, state) do
    case Map.pop(state.active, pid) do
      {nil, _active} ->
        {:noreply, state}

      {%{id: ^id, monitor: monitor}, active} ->
        Process.demonitor(monitor, [:flush])

        job = Map.fetch!(state.jobs, id)
        {status, value} =
          case result do
            {:worker_error, reason} -> {:failed, reason}
            value -> {:completed, value}
          end

        updated_job = %{
          job
          | status: status,
            result: value,
            completed_at: System.monotonic_time(:millisecond)
        }

        state =
          %{state | active: active}
          |> put_in([:jobs, id], updated_job)
          |> dispatch()

        persist(state)
        {:noreply, state}

      {_entry, _active} ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Enum.find(state.active, fn {_pid, entry} -> entry.monitor == monitor end) do
      nil ->
        {:noreply, state}

      {pid, entry} ->
        job = Map.fetch!(state.jobs, entry.id)

        updated_job = %{
          job
          | status: :failed,
            result: {:worker_exit, reason},
            completed_at: System.monotonic_time(:millisecond)
        }

        state =
          %{state, active: Map.delete(state.active, pid)}
          |> put_in([:jobs, entry.id], updated_job)
          |> dispatch()

        persist(state)
        {:noreply, state}
    end
  end

  def handle_info(:maintenance, state) do
    now = System.monotonic_time(:millisecond)

    jobs =
      Enum.reject(state.jobs, fn {_id, job} ->
        job.status in [:completed, :failed] and is_integer(job.completed_at) and
          now - job.completed_at >= state.retention_ms
      end)
      |> Map.new()

    state = %{
      state
      | jobs: jobs,
        maintenance_runs: state.maintenance_runs + 1
    }

    persist(state)
    schedule_maintenance(state.interval_ms)
    {:noreply, state}
  end

  defp dispatch(state) do
    if map_size(state.active) >= state.concurrency do
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
                      TaskQueue.Operations.execute(job.task, job.args)
                    rescue
                      error ->
                        {:worker_error, {:exception, error.__struct__, Exception.message(error)}}
                    catch
                      kind, reason -> {:worker_error, {kind, reason}}
                    end

                  send(parent, {:worker_result, id, self(), result})
                end)

              running_job = %{job | status: :running, started_at: DateTime.utc_now()}

              state =
                state
                |> put_in([:jobs, id], running_job)
                |> put_in([:active, pid], %{id: id, monitor: monitor})

              dispatch(state)

            _other ->
              dispatch(state)
          end
      end
    end
  end

  defp persist(state) do
    TaskQueue.Database.save(%{
      jobs: state.jobs,
      queued_count: Enum.count(state.jobs, fn {_id, job} -> job.status == :queued end),
      running_count: map_size(state.active),
      maintenance_runs: state.maintenance_runs
    })
  end

  defp schedule_maintenance(interval_ms) do
    Process.send_after(self(), :maintenance, interval_ms)
  end
end
