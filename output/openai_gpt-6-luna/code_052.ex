# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Finance do
  @moduledoc false

  def start_link(opts \\ []) do
    Finance.Supervisor.start_link(opts)
  end

  def create(record), do: Finance.Store.create(record)
  def get(id), do: Finance.Store.get(id)
  def update(id, attributes), do: Finance.Store.update(id, attributes)
  def delete(id), do: Finance.Store.delete(id)

  def list_stream(batch_size \\ 500) do
    Finance.Store.list_stream(batch_size)
  end

  def ingest_stream(enumerable) do
    Enum.reduce_while(enumerable, {:ok, 0}, fn record, {:ok, count} ->
      case create(record) do
        {:ok, _record} -> {:cont, {:ok, count + 1}}
        error -> {:halt, error}
      end
    end)
  end

  def process_stream(enumerable, fun, opts \\ []) when is_function(fun, 1) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    timeout = Keyword.get(opts, :timeout, :infinity)
    ordered = Keyword.get(opts, :ordered, false)

    Task.Supervisor.async_stream_nolink(
      Finance.TaskSupervisor,
      enumerable,
      fun,
      max_concurrency: max_concurrency,
      timeout: timeout,
      ordered: ordered
    )
  end

  def process_records(fun, opts \\ []) when is_function(fun, 1) do
    batch_size = Keyword.get(opts, :batch_size, 500)

    list_stream(batch_size)
    |> process_stream(fun, opts)
  end

  def start_operation(id, fun) when is_function(fun, 0) do
    DynamicSupervisor.start_child(
      Finance.OperationSupervisor,
      {Finance.Operation, {id, fun}}
    )
    |> case do
      {:ok, _pid} -> {:ok, id}
      {:error, {:already_started, _pid}} -> {:error, :already_started}
      error -> error
    end
  end

  def operation_status(id) do
    case Registry.lookup(Finance.Registry, {:operation, id}) do
      [{pid, _value}] -> GenServer.call(pid, :status)
      [] -> {:error, :not_found}
    end
  end

  def operations do
    Registry.select(Finance.Registry, [
      {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}
    ])
    |> Enum.flat_map(fn
      {{:operation, id}, pid} ->
        try do
          [GenServer.call(pid, :status)]
        catch
          :exit, _reason -> []
        end

      _other ->
        []
    end)
  end

  def schedule_periodic(name, interval_ms, fun)
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    GenServer.call(Finance.Scheduler, {:schedule, name, interval_ms, fun})
  end

  def unschedule_periodic(name) do
    GenServer.call(Finance.Scheduler, {:unschedule, name})
  end
end

defmodule Finance.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Finance.Registry},
      {Task.Supervisor, name: Finance.TaskSupervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: Finance.OperationSupervisor},
      Finance.Store,
      Finance.Scheduler
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Finance.Store do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  def create(record) when is_map(record) do
    case Map.fetch(record, :id) do
      {:ok, id} when not is_nil(id) -> GenServer.call(__MODULE__, {:create, id, record})
      _ -> {:error, :invalid_record}
    end
  end

  def create(_record), do: {:error, :invalid_record}

  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def update(id, attributes), do: GenServer.call(__MODULE__, {:update, id, attributes})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  def list_stream(batch_size) when is_integer(batch_size) and batch_size > 0 do
    table = GenServer.call(__MODULE__, :table)
    match_spec = [{{:"$1", :"$2"}, [], [:"$_"]}]

    Stream.resource(
      fn -> select_chunk(table, match_spec, batch_size) end,
      fn
        :done ->
          {:halt, :done}

        {records, continuation} ->
          {Enum.map(records, fn {_id, record} -> record end),
           select_continuation(continuation)}
      end,
      fn _state -> :ok end
    )
  end

  @impl true
  def init(:ok) do
    {:ok, %{table: :ets.new(:finance_records, [:set, :protected, read_concurrency: true])}}
  end

  @impl true
  def handle_call(:table, _from, state), do: {:reply, state.table, state}

  def handle_call({:create, id, record}, _from, state) do
    reply =
      if :ets.insert_new(state.table, {id, record}) do
        {:ok, record}
      else
        {:error, :already_exists}
      end

    {:reply, reply, state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :ets.lookup(state.table, id) do
        [{^id, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, id, attributes}, _from, state) when is_map(attributes) do
    reply =
      case :ets.lookup(state.table, id) do
        [{^id, record}] ->
          updated = record |> Map.merge(attributes) |> Map.put(:id, id)
          :ets.insert(state.table, {id, updated})
          {:ok, updated}

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, _id, _attributes}, _from, state) do
    {:reply, {:error, :invalid_attributes}, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply =
      case :ets.lookup(state.table, id) do
        [{^id, record}] ->
          :ets.delete(state.table, id)
          {:ok, record}

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  defp select_chunk(table, match_spec, batch_size) do
    case :ets.select(table, match_spec, batch_size) do
      :"$end_of_table" -> :done
      {records, continuation} -> {records, continuation}
    end
  end

  defp select_continuation(continuation) do
    case :ets.select(continuation) do
      :"$end_of_table" -> :done
      {records, next_continuation} -> {records, next_continuation}
    end
  end
end

defmodule Finance.Operation do
  use GenServer

  def start_link(id, fun) do
    GenServer.start_link(__MODULE__, {id, fun},
      name: {:via, Registry, {Finance.Registry, {:operation, id}}}
    )
  end

  def child_spec({id, fun}) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [id, fun]},
      restart: :temporary
    }
  end

  @impl true
  def init({id, fun}) do
    {:ok, %{id: id, fun: fun, status: :queued, task_ref: nil}, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, state) do
    task = Task.Supervisor.async_nolink(Finance.TaskSupervisor, state.fun)
    {:noreply, %{state | status: :running, task_ref: task.ref}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, Map.drop(state, [:fun, :task_ref]), state}
  end

  @impl true
  def handle_info({ref, result}, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | status: :completed, result: result, task_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task_ref: ref} = state) do
    {:noreply, %{state | status: :failed, reason: reason, task_ref: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}
end

defmodule Finance.Scheduler do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok), do: {:ok, %{jobs: %{}}}

  @impl true
  def handle_call({:schedule, name, interval_ms, fun}, _from, state) do
    state = remove_job(state, name)
    timer = Process.send_after(self(), {:tick, name}, interval_ms)
    job = %{interval: interval_ms, fun: fun, timer: timer}
    {:reply, :ok, put_in(state.jobs[name], job)}
  end

  def handle_call({:unschedule, name}, _from, state) do
    exists = Map.has_key?(state.jobs, name)
    {:reply, if(exists, do: :ok, else: {:error, :not_found}), remove_job(state, name)}
  end

  @impl true
  def handle_info({:tick, name}, state) do
    case Map.fetch(state.jobs, name) do
      {:ok, job} ->
        id = {:periodic, name, System.unique_integer([:positive, :monotonic])}
        Finance.start_operation(id, job.fun)

        timer = Process.send_after(self(), {:tick, name}, job.interval)
        updated_job = %{job | timer: timer}
        {:noreply, put_in(state.jobs[name], updated_job)}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp remove_job(state, name) do
    case Map.pop(state.jobs, name) do
      {nil, _jobs} ->
        state

      {job, jobs} ->
        Process.cancel_timer(job.timer)
        %{state | jobs: jobs}
    end
  end
end
