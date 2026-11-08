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

defmodule Ecommerce.OrderManager do
  use GenServer
  require Logger

  @sweep_interval 60_000
  @order_ttl 24 * 60 * 60 * 1_000
  @notification_threshold 500

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def place_order(id, total) do
    GenServer.call(__MODULE__, {:place_order, id, total})
  end

  def list_orders do
    GenServer.call(__MODULE__, :list_orders)
  end

  @impl true
  def init(_opts) do
    schedule_sweep()
    {:ok, %{orders: %{}}}
  end

  @impl true
  def handle_call({:place_order, id, total}, _from, state)
      when is_number(total) and total > 0 do
    if Map.has_key?(state.orders, id) do
      {:reply, {:error, :duplicate_order_id}, state}
    else
      order = %{
        id: id,
        total: total,
        status: :pending,
        created_at: System.monotonic_time(:millisecond)
      }

      if total >= @notification_threshold do
        Logger.info("Pedido #{inspect(id)} atingiu o limite de notificação: #{total}")
      end

      {:reply, {:ok, order}, put_in(state.orders[id], order)}
    end
  end

  def handle_call({:place_order, _id, _total}, _from, state) do
    {:reply, {:error, :invalid_order}, state}
  end

  def handle_call(:list_orders, _from, state) do
    {:reply, Map.values(state.orders), state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = System.monotonic_time(:millisecond)

    orders =
      Map.new(state.orders, fn {id, order} ->
        expired =
          order.status == :pending and now - order.created_at >= @order_ttl

        if expired do
          Logger.warning("Pedido #{inspect(id)} expirou e foi encerrado")
          {id, %{order | status: :expired}}
        else
          {id, order}
        end
      end)

    schedule_sweep()
    {:noreply, %{state | orders: orders}}
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval)
  end
end

defmodule Ecommerce.ProcessWatchdog do
  use GenServer
  require Logger

  @retry_interval 100

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :locate_order_manager)
    {:ok, %{monitor_ref: nil, pid: nil}}
  end

  @impl true
  def handle_info(:locate_order_manager, %{monitor_ref: nil} = state) do
    case Process.whereis(Ecommerce.OrderManager) do
      nil ->
        Process.send_after(self(), :locate_order_manager, @retry_interval)
        {:noreply, state}

      pid ->
        {:noreply, %{state | pid: pid, monitor_ref: Process.monitor(pid)}}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, pid, reason},
        %{monitor_ref: ref, pid: pid} = state
      ) do
    if reason not in [:normal, :shutdown] do
      Logger.error(
        "O processo de gerenciamento de pedidos encerrou inesperadamente: #{inspect(reason)}"
      )
    end

    Process.send_after(self(), :locate_order_manager, @retry_interval)
    {:noreply, %{state | monitor_ref: nil, pid: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}
end

defmodule Ecommerce.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Ecommerce.OrderManager, []},
      {Ecommerce.ProcessWatchdog, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
