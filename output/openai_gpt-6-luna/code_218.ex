# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Implementar expiração automática de registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Fazer uso de macros.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker.Validation do
  defmacro valid_if(condition, message) do
    quote do
      if unquote(condition), do: :ok, else: {:error, unquote(message)}
    end
  end
end

defmodule DeliveryTracker.Store do
  use GenServer

  @table :delivery_tracker_store

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    :ets.new(@table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.insert(@table, {:seq, 0})
    {:ok, %{}}
  end
end

defmodule DeliveryTracker.Worker do
  use GenServer

  @table :delivery_tracker_store

  def start_link(purge_interval) do
    GenServer.start_link(__MODULE__, purge_interval, name: __MODULE__)
  end

  @impl true
  def init(purge_interval) do
    schedule_purge(purge_interval)
    purge_expired()
    {:ok, %{purge_interval: purge_interval}}
  end

  @impl true
  def handle_call({:create, id, metadata, ttl_ms}, _from, state) do
    expire_one(id)

    case :ets.lookup(@table, {:delivery, id}) do
      [] ->
        now = System.system_time(:millisecond)

        record = %{
          id: id,
          status: :pending,
          metadata: metadata,
          created_at: now,
          updated_at: now,
          expires_at: now + ttl_ms
        }

        :ets.insert(@table, {{:delivery, id}, record})
        append_history(:created, id, %{status: :pending})
        {:reply, {:ok, record}, state}

      [_existing] ->
        {:reply, {:error, :already_exists}, state}
    end
  end

  def handle_call({:update_status, id, status}, _from, state) do
    expire_one(id)

    case :ets.lookup(@table, {:delivery, id}) do
      [{{:delivery, ^id}, record}] ->
        updated = %{record | status: status, updated_at: System.system_time(:millisecond)}
        :ets.insert(@table, {{:delivery, id}, updated})
        append_history(:status_updated, id, %{status: status})
        {:reply, {:ok, updated}, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    expire_one(id)

    reply =
      case :ets.lookup(@table, {:delivery, id}) do
        [{{:delivery, ^id}, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    expire_one(id)

    case :ets.lookup(@table, {:delivery, id}) do
      [{{:delivery, ^id}, _record}] ->
        :ets.delete(@table, {:delivery, id})
        append_history(:deleted, id, %{})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:history, _from, state) do
    entries =
      @table
      |> :ets.tab2list()
      |> Enum.flat_map(fn
        {{:history, _seq}, entry} -> [entry]
        _ -> []
      end)
      |> Enum.sort_by(& &1.seq)

    {:reply, {:ok, entries}, state}
  end

  @impl true
  def handle_info(:purge, state) do
    purge_expired()
    schedule_purge(state.purge_interval)
    {:noreply, state}
  end

  defp schedule_purge(interval) do
    Process.send_after(self(), :purge, interval)
  end

  defp purge_expired do
    now = System.system_time(:millisecond)

    @table
    |> :ets.tab2list()
    |> Enum.each(fn
      {{:delivery, id}, record} when record.expires_at <= now ->
        expire_one(id)

      _ ->
        :ok
    end)
  end

  defp expire_one(id) do
    case :ets.lookup(@table, {:delivery, id}) do
      [{{:delivery, ^id}, record}]
      when record.expires_at <= System.system_time(:millisecond) ->
        :ets.delete(@table, {:delivery, id})
        append_history(:expired, id, %{status: record.status})
        :expired

      _ ->
        :ok
    end
  end

  defp append_history(action, id, details) do
    seq = :ets.update_counter(@table, :seq, {2, 1}, {:seq, 0})

    entry = %{
      seq: seq,
      action: action,
      delivery_id: id,
      at: System.system_time(:millisecond),
      details: details
    }

    :ets.insert(@table, {{:history, seq}, entry})
    entry
  end
end

defmodule DeliveryTracker do
  use Supervisor
  require DeliveryTracker.Validation

  @table :delivery_tracker_store
  @statuses [:pending, :picked_up, :in_transit, :delivered, :failed]

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    purge_interval = Keyword.get(opts, :purge_interval, 1_000)

    children = [
      {DeliveryTracker.Store, []},
      {DeliveryTracker.Worker, purge_interval}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(id, metadata, ttl_ms) do
    with :ok <- valid_if(is_binary(id) and byte_size(id) > 0, :invalid_id),
         :ok <- valid_if(is_map(metadata), :invalid_metadata),
         :ok <- valid_if(is_integer(ttl_ms) and ttl_ms > 0, :invalid_ttl) do
      GenServer.call(DeliveryTracker.Worker, {:create, id, metadata, ttl_ms})
    end
  end

  def update_status(id, status) do
    with :ok <- valid_if(is_binary(id) and byte_size(id) > 0, :invalid_id),
         :ok <- valid_if(is_atom(status) and status in @statuses, :invalid_status) do
      GenServer.call(DeliveryTracker.Worker, {:update_status, id, status})
    end
  end

  def get(id) do
    with :ok <- valid_if(is_binary(id) and byte_size(id) > 0, :invalid_id) do
      GenServer.call(DeliveryTracker.Worker, {:get, id})
    end
  end

  def delete(id) do
    with :ok <- valid_if(is_binary(id) and byte_size(id) > 0, :invalid_id) do
      GenServer.call(DeliveryTracker.Worker, {:delete, id})
    end
  end

  def history do
    GenServer.call(DeliveryTracker.Worker, :history)
  end

  @doc """
  Retorna o histórico mais recente, limitado a `limit` entradas.
  """
  def recent_history(limit) do
    with :ok <- valid_if(is_integer(limit) and limit > 0, :invalid_limit),
         {:ok, entries} <- history() do
      {:ok, Enum.take(entries, -limit)}
    end
  end
end
