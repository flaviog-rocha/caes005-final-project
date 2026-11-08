# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir comunicação entre diferentes processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Implementar expiração automática de registros.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule EcommerceOrders do
  use Supervisor

  @registry EcommerceOrders.Registry
  @dynamic_supervisor EcommerceOrders.DynamicSupervisor
  @manager EcommerceOrders.Manager
  @store EcommerceOrders.Store

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {@store, path: Keyword.get(opts, :path, "orders.dets")},
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor},
      {@manager, ttl_ms: Keyword.get(opts, :ttl_ms, 86_400_000)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_order(id, attrs \\ %{}) when is_map(attrs) do
    GenServer.call(@manager, {:create, id, attrs})
  end

  def get_order(id) do
    with {:ok, pid} <- GenServer.call(@manager, {:lookup, id}) do
      safe_call(pid, :get)
    end
  end

  def update_order(id, attrs) when is_map(attrs) do
    with {:ok, pid} <- GenServer.call(@manager, {:lookup, id}) do
      safe_call(pid, {:update, attrs})
    end
  end

  def delete_order(id) do
    with {:ok, pid} <- GenServer.call(@manager, {:lookup, id}) do
      safe_call(pid, :delete)
    end
  end

  def list_orders do
    now = System.system_time(:millisecond)

    @store.all()
    |> Enum.filter(fn order -> order.expires_at > now end)
  end

  defp safe_call(pid, message) do
    GenServer.call(pid, message)
  catch
    :exit, reason -> {:error, {:process_unavailable, reason}}
  end

  defmodule Store do
    use GenServer

    @table :ecommerce_orders_dets

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    def get(id), do: GenServer.call(__MODULE__, {:get, id})
    def put(order), do: GenServer.call(__MODULE__, {:put, order})
    def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
    def all, do: GenServer.call(__MODULE__, :all)

    @impl true
    def init(opts) do
      path = opts |> Keyword.fetch!(:path) |> to_charlist()

      case :dets.open_file(@table, file: path, type: :set) do
        {:ok, @table} -> {:ok, @table}
        {:error, reason} -> {:stop, reason}
      end
    end

    @impl true
    def handle_call({:get, id}, _from, table) do
      result =
        case :dets.lookup(table, id) do
          [{^id, order}] -> order
          [] -> nil
        end

      {:reply, result, table}
    end

    def handle_call({:put, order}, _from, table) do
      result = :dets.insert(table, {order.id, order})
      if result == :ok, do: :dets.sync(table)
      {:reply, result, table}
    end

    def handle_call({:delete, id}, _from, table) do
      result = :dets.delete(table, id)
      if result == :ok, do: :dets.sync(table)
      {:reply, result, table}
    end

    def handle_call(:all, _from, table) do
      orders = :dets.foldl(fn {_id, order}, acc -> [order | acc] end, [], table)
      {:reply, orders, table}
    end

    @impl true
    def terminate(_reason, table) do
      :dets.sync(table)
      :dets.close(table)
      :ok
    end
  end

  defmodule Order do
    use GenServer

    def start_link({id, order, ttl_ms}) do
      GenServer.start_link(__MODULE__, {id, order, ttl_ms},
        name: {:via, Registry, {EcommerceOrders.Registry, id}}
      )
    end

    @impl true
    def init({_id, order, ttl_ms}) do
      schedule_expiry(order.expires_at)
      {:ok, %{order: order, ttl_ms: ttl_ms}}
    end

    @impl true
    def handle_call(:get, _from, state) do
      if expired?(state.order) do
        EcommerceOrders.Store.delete(state.order.id)
        {:stop, :normal, {:error, :expired}, state}
      else
        {:reply, {:ok, state.order}, state}
      end
    end

    def handle_call({:update, attrs}, _from, state) do
      if expired?(state.order) do
        EcommerceOrders.Store.delete(state.order.id)
        {:stop, :normal, {:error, :expired}, state}
      else
        now = System.system_time(:millisecond)
        safe_attrs = Map.drop(attrs, [:id, :created_at, :updated_at, :expires_at])

        order =
          state.order
          |> Map.merge(safe_attrs)
          |> Map.put(:updated_at, now)
          |> Map.put(:expires_at, now + state.ttl_ms)

        case EcommerceOrders.Store.put(order) do
          :ok ->
            schedule_expiry(order.expires_at)
            {:reply, {:ok, order}, %{state | order: order}}

          error ->
            {:reply, {:error, error}, state}
        end
      end
    end

    def handle_call(:delete, _from, state) do
      case EcommerceOrders.Store.delete(state.order.id) do
        :ok -> {:stop, :normal, :ok, state}
        error -> {:reply, {:error, error}, state}
      end
    end

    @impl true
    def handle_info({:expire, expires_at}, %{order: %{expires_at: expires_at}} = state) do
      remaining = expires_at - System.system_time(:millisecond)

      if remaining <= 0 do
        EcommerceOrders.Store.delete(state.order.id)
        {:stop, :normal, state}
      else
        Process.send_after(self(), {:expire, expires_at}, remaining)
        {:noreply, state}
      end
    end

    def handle_info({:expire, _old_expiry}, state), do: {:noreply, state}

    defp schedule_expiry(expires_at) do
      delay = max(expires_at - System.system_time(:millisecond), 0)
      Process.send_after(self(), {:expire, expires_at}, delay)
    end

    defp expired?(order), do: order.expires_at <= System.system_time(:millisecond)
  end

  defmodule Manager do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(opts) do
      send(self(), :recover)
      {:ok, %{workers: %{}, ttl_ms: max(Keyword.get(opts, :ttl_ms, 86_400_000), 1)}}
    end

    @impl true
    def handle_call({:create, id, attrs}, _from, state) do
      existing = EcommerceOrders.Store.get(id)

      if existing && not expired?(existing) do
        {:reply, {:error, :already_exists}, state}
      else
        if existing, do: EcommerceOrders.Store.delete(id)

        now = System.system_time(:millisecond)
        safe_attrs = Map.drop(attrs, [:id, :created_at, :updated_at, :expires_at])

        order =
          safe_attrs
          |> Map.put(:id, id)
          |> Map.put_new(:status, :pending)
          |> Map.put_new(:items, [])
          |> Map.put(:created_at, now)
          |> Map.put(:updated_at, now)
          |> Map.put(:expires_at, now + state.ttl_ms)

        case EcommerceOrders.Store.put(order) do
          :ok ->
            {pid, state} = ensure_worker(id, order, state)

            if is_pid(pid) do
              {:reply, {:ok, order}, state}
            else
              {:reply, {:error, :worker_start_failed}, state}
            end

          error ->
            {:reply, {:error, error}, state}
        end
      end
    end

    def handle_call({:lookup, id}, _from, state) do
      case EcommerceOrders.Store.get(id) do
        nil ->
          {:reply, {:error, :not_found}, state}

        order ->
          if expired?(order) do
            EcommerceOrders.Store.delete(id)
            {:reply, {:error, :not_found}, state}
          else
            {pid, state} = ensure_worker(id, order, state)

            if is_pid(pid) do
              {:reply, {:ok, pid}, state}
            else
              {:reply, {:error, :worker_unavailable}, state}
            end
          end
      end
    end

    @impl true
    def handle_info(:recover, state) do
      now = System.system_time(:millisecond)

      state =
        EcommerceOrders.Store.all()
        |> Enum.reduce(state, fn order, acc ->
          if order.expires_at <= now do
            EcommerceOrders.Store.delete(order.id)
            acc
          else
            {_pid, acc} = ensure_worker(order.id, order, acc)
            acc
          end
        end)

      {:noreply, state}
    end

    def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
      case Enum.find(state.workers, fn {_id, {_worker_pid, worker_ref}} -> worker_ref == ref end) do
        nil ->
          {:noreply, state}

        {id, _worker} ->
          state = %{state | workers: Map.delete(state.workers, id)}

          if reason not in [:normal, :shutdown] do
            case EcommerceOrders.Store.get(id) do
              nil ->
                {:noreply, state}

              order ->
                if expired?(order) do
                  EcommerceOrders.Store.delete(id)
                  {:noreply, state}
                else
                  {_pid, state} = ensure_worker(id, order, state)
                  {:noreply, state}
                end
            end
          else
            {:noreply, state}
          end
      end
    end

    defp ensure_worker(id, order, state) do
      case Registry.lookup(EcommerceOrders.Registry, id) do
        [{pid, _}] ->
          {pid, monitor_worker(id, pid, state)}

        [] ->
          spec = %{
            id: {EcommerceOrders.Order, id},
            start: {EcommerceOrders.Order, :start_link, [{id, order, state.ttl_ms}]},
            restart: :temporary,
            shutdown: 5_000,
            type: :worker
          }

          case DynamicSupervisor.start_child(EcommerceOrders.DynamicSupervisor, spec) do
            {:ok, pid} -> {pid, monitor_worker(id, pid, state)}
            {:error, {:already_started, pid}} -> {pid, monitor_worker(id, pid, state)}
            _error -> {nil, state}
          end
      end
    end

    defp monitor_worker(id, pid, state) do
      case Map.get(state.workers, id) do
        {^pid, _ref} ->
          state

        {_old_pid, old_ref} ->
          Process.demonitor(old_ref, [:flush])
          ref = Process.monitor(pid)
          %{state | workers: Map.put(state.workers, id, {pid, ref})}

        nil ->
          ref = Process.monitor(pid)
          %{state | workers: Map.put(state.workers, id, {pid, ref})}
      end
    end

    defp expired?(order), do: order.expires_at <= System.system_time(:millisecond)
  end
end
