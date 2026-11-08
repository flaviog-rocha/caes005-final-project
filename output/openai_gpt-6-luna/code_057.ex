# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir consultar o estado atual das operações em andamento.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  use GenServer

  @dets_name :financial_transactions_dets
  @default_file "financial_transactions.dets"

  # The durable :in_progress record is written before the worker starts.
  # After a restart, such operations are not run again: this prevents duplicate
  # execution, though an operation interrupted by a crash may remain in progress.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc """
  Submits an operation identified by `id`. `fun` receives `payload` and must
  return its result. A previously submitted `id` is never executed again.
  """
  def submit(server \\ __MODULE__, id, payload, fun)
      when is_function(fun, 1) do
    GenServer.call(server, {:submit, id, payload, fun})
  end

  def get(server \\ __MODULE__, id) do
    GenServer.call(server, {:get, id})
  end

  def in_progress(server \\ __MODULE__) do
    GenServer.call(server, :in_progress)
  end

  @impl true
  def init(opts) do
    file = opts |> Keyword.get(:file, @default_file) |> to_string()

    with :ok <- open_store(file) do
      table = :ets.new(:financial_transactions, [:set, :protected, read_concurrency: true])
      load_store(table)

      {:ok, %{table: table, store: @dets_name}}
    else
      {:error, reason} -> {:stop, {:cannot_open_store, reason}}
    end
  end

  @impl true
  def handle_call({:submit, id, payload, fun}, _from, state) do
    case :ets.lookup(state.table, id) do
      [{^id, record}] ->
        {:reply, {:ok, :already_submitted, record}, state}

      [] ->
        now = System.system_time(:millisecond)

        record = %{
          id: id,
          payload: payload,
          status: :in_progress,
          result: nil,
          started_at: now,
          updated_at: now
        }

        case persist(state.store, id, record) do
          :ok ->
            :ets.insert(state.table, {id, record})

            case Task.start(fn ->
                   outcome = run_operation(fun, payload)
                   send(self_server(state), {:operation_finished, id, outcome})
                 end) do
              {:ok, _pid} ->
                {:reply, {:ok, :started, record}, state}

              {:error, reason} ->
                {:reply, {:error, {:worker_start_failed, reason}}, state}
            end

          {:error, reason} ->
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    reply =
      case :ets.lookup(state.table, id) do
        [{^id, record}] -> {:ok, record}
        [] -> :not_found
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call(:in_progress, _from, state) do
    records =
      state.table
      |> :ets.tab2list()
      |> Enum.map(fn {_id, record} -> record end)
      |> Enum.filter(&(&1.status == :in_progress))

    {:reply, records, state}
  end

  @impl true
  def handle_info({:operation_finished, id, outcome}, state) do
    case :ets.lookup(state.table, id) do
      [{^id, %{status: :in_progress} = record}] ->
        {status, result} = outcome

        updated = %{
          record
          | status: status,
            result: result,
            updated_at: System.system_time(:millisecond)
        }

        case persist(state.store, id, updated) do
          :ok -> :ets.insert(state.table, {id, updated})
          {:error, _reason} -> :ok
        end

      _ ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.sync(state.store)
    :dets.close(state.store)
    :ok
  end

  defp open_store(file) do
    case :dets.open_file(@dets_name, file: String.to_charlist(file), type: :set, repair: true) do
      {:ok, _table} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_store(table) do
    :dets.foldl(
      fn {id, record}, :ok ->
        :ets.insert(table, {id, record})
        :ok
      end,
      :ok,
      @dets_name
    )
  end

  defp persist(store, id, record) do
    case :dets.insert(store, {id, record}) do
      :ok -> :dets.sync(store)
      {:error, reason} -> {:error, reason}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp run_operation(fun, payload) do
    try do
      case fun.(payload) do
        {:error, reason} -> {:failed, reason}
        result -> {:completed, result}
      end
    rescue
      exception -> {:failed, {:exception, Exception.message(exception)}}
    catch
      kind, reason -> {:failed, {kind, reason}}
    end
  end

  defp self_server(state) do
    # The GenServer owns the ETS table; its registered name is used by workers
    # to report completion without blocking operation submission.
    case Process.info(self(), :registered_name) do
      {:registered_name, name} when name != [] -> name
      _ -> self()
    end
  end
end
