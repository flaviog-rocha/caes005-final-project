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
  use Supervisor

  @registry Finance.Registry
  @worker_supervisor Finance.WorkerSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Finance.Supervisor)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "finance_transactions.dets")

    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @worker_supervisor},
      {Task.Supervisor, name: Finance.TaskSupervisor},
      {Finance.Store, path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(attrs) when is_map(attrs), do: Finance.Store.create(attrs)
  def get(id), do: Finance.Store.get(id)
  def update(id, changes) when is_map(changes), do: Finance.Store.update(id, changes)
  def delete(id), do: Finance.Store.delete(id)

  def stream(opts \\ []) do
    limit = Keyword.get(opts, :chunk_size, 500)

    unless is_integer(limit) and limit > 0 do
      raise ArgumentError, "chunk_size deve ser um inteiro positivo"
    end

    Stream.resource(
      fn -> Finance.Store.select_start(limit) end,
      fn
        :done ->
          {:halt, :done}

        {:chunk, records, continuation} ->
          {records, continuation}
      end,
      fn _ -> :ok end
    )
  end

  def start_operation(fun) when is_function(fun, 0) do
    id = new_id()

    case DynamicSupervisor.start_child(
           @worker_supervisor,
           {Finance.Operation, {id, fun}}
         ) do
      {:ok, _pid} -> {:ok, id}
      {:ok, _pid, _info} -> {:ok, id}
      error -> error
    end
  end

  def operation_status(id), do: lookup_status({:operation, id})

  def start_periodic(interval_ms, fun)
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    id = new_id()

    case DynamicSupervisor.start_child(
           @worker_supervisor,
           {Finance.Periodic, {id, interval_ms, fun}}
         ) do
      {:ok, _pid} -> {:ok, id}
      {:ok, _pid, _info} -> {:ok, id}
      error -> error
    end
  end

  def start_periodic(_interval_ms, _fun), do: {:error, :invalid_periodic_operation}

  def periodic_status(id), do: lookup_status({:periodic, id})

  def stop_periodic(id) do
    case Registry.lookup(@registry, {:periodic, id}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(@worker_supervisor, pid)
      [] -> {:error, :not_found}
    end
  end

  defp lookup_status(key) do
    case Registry.lookup(@registry, key) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, :status)
        catch
          :exit, _ -> {:error, :not_found}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp new_id do
    System.unique_integer([:positive, :monotonic])
    |> Integer.to_string()
  end
end

defmodule Finance.Store do
  use GenServer

  @table :finance_transactions

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  @impl true
  def init(path) do
    path = Path.expand(path)
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table,
           file: String.to_charlist(path),
           type: :set,
           keypos: 1
         ) do
      {:ok, @table} -> {:ok, %{path: path}}
      {:error, reason} -> {:stop, reason}
    end
  end

  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def update(id, changes), do: GenServer.call(__MODULE__, {:update, id, changes})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
  def select_start(limit), do: GenServer.call(__MODULE__, {:select_start, limit})
  def select_next(continuation), do: GenServer.call(__MODULE__, {:select_next, continuation})

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = System.unique_integer([:positive, :monotonic]) |> Integer.to_string()
    now = DateTime.utc_now()

    record =
      attrs
      |> Map.drop([:id, :created_at, :updated_at])
      |> Map.merge(%{id: id, created_at: now, updated_at: now})

    {:reply, persist({{:transaction, id}, record}), state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(@table, {:transaction, id}) do
        [{{:transaction, ^id}, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, id, changes}, _from, state) do
    reply =
      case :dets.lookup(@table, {:transaction, id}) do
        [{{:transaction, ^id}, record}] ->
          updated =
            record
            |> Map.merge(Map.drop(changes, [:id, :created_at, :updated_at]))
            |> Map.put(:updated_at, DateTime.utc_now())

          persist({{:transaction, id}, updated})

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply =
      case :dets.delete(@table, {:transaction, id}) do
        :ok ->
          case :dets.sync(@table) do
            :ok -> :ok
            error -> error
          end

        error ->
          error
      end

    {:reply, reply, state}
  end

  def handle_call({:select_start, limit}, _from, state) do
    match_spec = [{{:"$1", :"$2"}, [], [:"$2"]}]
    {:reply, normalize_select(:dets.select(@table, match_spec, limit)), state}
  end

  def handle_call({:select_next, continuation}, _from, state) do
    {:reply, normalize_select(:dets.select(continuation)), state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp persist(entry) do
    case :dets.insert(@table, entry) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> {:ok, elem(entry, 1)}
          error -> error
        end

      error ->
        error
    end
  end

  defp normalize_select({:matches, records, continuation}),
    do: {:chunk, records, continuation}

  defp normalize_select(:end_of_table), do: :done
  defp normalize_select({:error, reason}), do: raise("falha ao ler transações: #{inspect(reason)}")
end

defmodule Finance.Operation do
  use GenServer

  def start_link({id, fun}) do
    GenServer.start_link(__MODULE__, {id, fun},
      name: {:via, Registry, {Finance.Registry, {:operation, id}}}
    )
  end

  @impl true
  def init({id, fun}) do
    send(self(), :run)

    {:ok,
     %{
       id: id,
       fun: fun,
       status: :running,
       started_at: DateTime.utc_now(),
       finished_at: nil,
       result: nil,
       error: nil,
       task_ref: nil
     }}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status =
      Map.take(state, [:id, :status, :started_at, :finished_at, :result, :error])

    {:reply, status, state}
  end

  @impl true
  def handle_info(:run, state) do
    task =
      Task.Supervisor.async_nolink(Finance.TaskSupervisor, fn ->
        state.fun.()
      end)

    {:noreply, %{state | task_ref: task.ref}}
  end

  def handle_info({ref, result}, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])

    {:noreply,
     %{
       state
       | status: :completed,
         finished_at: DateTime.utc_now(),
         result: result,
         task_ref: nil,
         fun: nil
     }}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task_ref: ref} = state) do
    {:noreply,
     %{
       state
       | status: :failed,
         finished_at: DateTime.utc_now(),
         error: reason,
         task_ref: nil,
         fun: nil
     }}
  end

  def handle_info(_message, state), do: {:noreply, state}
end

defmodule Finance.Periodic do
  use GenServer

  def start_link({id, interval_ms, fun}) do
    GenServer.start_link(__MODULE__, {id, interval_ms, fun},
      name: {:via, Registry, {Finance.Registry, {:periodic, id}}}
    )
  end

  @impl true
  def init({id, interval_ms, fun}) do
    timer = Process.send_after(self(), :tick, interval_ms)

    {:ok,
     %{
       id: id,
       interval_ms: interval_ms,
       fun: fun,
       timer: timer,
       tick_count: 0,
       last_operation_id: nil
     }}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status =
      Map.take(state, [:id, :interval_ms, :tick_count, :last_operation_id])

    {:reply, status, state}
  end

  @impl true
  def handle_info(:tick, state) do
    operation_id =
      case Finance.start_operation(state.fun) do
        {:ok, id} -> id
        _error -> nil
      end

    timer = Process.send_after(self(), :tick, state.interval_ms)

    {:noreply,
     %{
       state
       | timer: timer,
         tick_count: state.tick_count + 1,
         last_operation_id: operation_id
     }}
  end

  @impl true
  def terminate(_reason, state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    :ok
  end
end
