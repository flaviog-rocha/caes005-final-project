# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Executar determinadas operações periodicamente.
# 3. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.Notifier do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def notify(message) do
    GenServer.cast(__MODULE__, {:notify, message})
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_cast({:notify, message}, state) do
    IO.puts("[notificação] #{inspect(message)}")
    {:noreply, state}
  end
end

defmodule Ecommerce.OrderManager do
  use GenServer

  @valid_statuses [:pending, :paid, :shipped, :cancelled]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_order(attrs) when is_map(attrs) do
    GenServer.call(__MODULE__, {:create_order, attrs})
  end

  def update_status(order_id, status) do
    GenServer.call(__MODULE__, {:update_status, order_id, status})
  end

  def expire_pending_orders(ttl_ms) do
    GenServer.call(__MODULE__, {:expire_pending_orders, ttl_ms})
  end

  @impl true
  def init(opts) do
    {:ok, %{orders: %{}, high_value_threshold: Keyword.get(opts, :high_value_threshold, 1_000)}}
  end

  @impl true
  def handle_call({:create_order, attrs}, _from, state) do
    case Map.get(attrs, :total) do
      total when is_number(total) and total >= 0 ->
        id = System.unique_integer([:positive, :monotonic])

        order = %{
          id: id,
          total: total,
          status: :pending,
          created_at_ms: System.system_time(:millisecond)
        }

        if total >= state.high_value_threshold do
          Ecommerce.Notifier.notify({:high_value_order_created, id, total})
        end

        {:reply, {:ok, order}, put_in(state.orders[id], order)}

      _ ->
        {:reply, {:error, :invalid_total}, state}
    end
  end

  def handle_call({:update_status, order_id, status}, _from, state)
      when status in @valid_statuses do
    case Map.fetch(state.orders, order_id) do
      {:ok, order} ->
        updated_order = %{order | status: status}
        Ecommerce.Notifier.notify({:order_status_changed, order_id, status})
        {:reply, {:ok, updated_order}, put_in(state.orders[order_id], updated_order)}

      :error ->
        {:reply, {:error, :order_not_found}, state}
    end
  end

  def handle_call({:update_status, _order_id, _status}, _from, state) do
    {:reply, {:error, :invalid_status}, state}
  end

  def handle_call({:expire_pending_orders, ttl_ms}, _from, state) do
    now = System.system_time(:millisecond)

    {orders, expired_ids} =
      Enum.reduce(state.orders, {state.orders, []}, fn {id, order}, {orders, expired} ->
        if order.status == :pending and now - order.created_at_ms >= ttl_ms do
          cancelled = %{order | status: :cancelled}
          {Map.put(orders, id, cancelled), [id | expired]}
        else
          {orders, expired}
        end
      end)

    Enum.each(expired_ids, fn id ->
      Ecommerce.Notifier.notify({:pending_order_expired, id})
    end)

    {:reply, {:ok, Enum.reverse(expired_ids)}, %{state | orders: orders}}
  end
end

defmodule Ecommerce.PeriodicOperations do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, 60_000)
    ttl_ms = Keyword.get(opts, :pending_order_ttl_ms, 86_400_000)
    schedule(interval_ms)
    {:ok, %{interval_ms: interval_ms, ttl_ms: ttl_ms}}
  end

  @impl true
  def handle_info(:run_periodic_operations, state) do
    Ecommerce.OrderManager.expire_pending_orders(state.ttl_ms)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  defp schedule(interval_ms) do
    Process.send_after(self(), :run_periodic_operations, interval_ms)
  end
end

defmodule Ecommerce.ProcessMonitor do
  use GenServer

  @targets [
    {Ecommerce.OrderManager, "gerenciador de pedidos"},
    {Ecommerce.PeriodicOperations, "operações periódicas"}
  ]

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    send(self(), :monitor_targets)
    {:ok, %{refs: %{}}}
  end

  @impl true
  def handle_info(:monitor_targets, state) do
    refs =
      Enum.reduce(@targets, state.refs, fn {name, _description}, refs ->
        already_monitored? = Enum.any?(refs, fn {_ref, monitored_name} -> monitored_name == name end)

        if already_monitored? do
          refs
        else
          case Process.whereis(name) do
            nil -> refs
            pid -> Map.put(refs, Process.monitor(pid), name)
          end
        end
      end)

    if map_size(refs) < length(@targets) do
      Process.send_after(self(), :monitor_targets, 1_000)
    end

    {:noreply, %{state | refs: refs}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, refs} ->
        {:noreply, %{state | refs: refs}}

      {name, refs} ->
        if reason not in [:normal, :shutdown] do
          description =
            case List.keyfind(@targets, name, 0) do
              {^name, value} -> value
              nil -> inspect(name)
            end

          Ecommerce.Notifier.notify({:unexpected_process_termination, description, reason})
        end

        Process.send_after(self(), :monitor_targets, 1_000)
        {:noreply, %{state | refs: refs}}
    end
  end
end

defmodule Ecommerce.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    notifier = {Ecommerce.Notifier, []}

    order_manager =
      {Ecommerce.OrderManager,
       high_value_threshold: Keyword.get(opts, :high_value_threshold, 1_000)}

    periodic_operations =
      {Ecommerce.PeriodicOperations,
       interval_ms: Keyword.get(opts, :interval_ms, 60_000),
       pending_order_ttl_ms: Keyword.get(opts, :pending_order_ttl_ms, 86_400_000)}

    children = [
      notifier,
      order_manager,
      periodic_operations,
      {Ecommerce.ProcessMonitor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Ecommerce do
  def start(opts \\ []) do
    Ecommerce.Supervisor.start_link(opts)
  end
end
