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

# Arquivo autocontido: gerenciamento de pedidos de e-commerce em Elixir.
# Requisitos: notificações por condições, operações periódicas e detecção
# de encerramentos inesperados, com Supervisor e apenas a biblioteca padrão.
#
# Execução: elixir pedidos.exs

defmodule Ecommerce.Notifier do
  # Recebe eventos de notificação e os registra de forma assíncrona.
  # Em produção, aqui ficaria o envio por e-mail, webhook, push etc.
  use GenServer
  require Logger

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  # Cast: não bloqueia o chamador e não falha se o Notifier estiver reiniciando.
  def notify(type, payload), do: GenServer.cast(__MODULE__, {:notify, type, payload})

  @impl true
  def init(_), do: {:ok, %{total: 0}}

  @impl true
  def handle_cast({:notify, type, payload}, state) do
    Logger.info("[NOTIFICAÇÃO] #{type}: #{inspect(payload)}")
    {:noreply, %{state | total: state.total + 1}}
  end
end

defmodule Ecommerce.OrderManager do
  # Mantém o estado dos pedidos e avalia condições de notificação.
  # Observação: o estado é mantido em memória. Se este processo for
  # reiniciado pelo Supervisor, os pedidos em memória são perdidos.
  use GenServer
  alias Ecommerce.Notifier

  # Condição: pedidos com valor igual ou superior a este limite geram alerta.
  @high_value_threshold 1_000

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def create_order(customer, items), do: GenServer.call(__MODULE__, {:create, customer, items})

  def update_status(id, status), do: GenServer.call(__MODULE__, {:update_status, id, status})

  def get_order(id), do: GenServer.call(__MODULE__, {:get, id})

  # Operação usada pelo Scheduler: cancela pedidos pendentes além do tempo limite.
  def expire_stale(timeout_ms), do: GenServer.call(__MODULE__, {:expire, timeout_ms})

  @impl true
  def init(_), do: {:ok, %{orders: %{}, next_id: 1}}

  @impl true
  def handle_call({:create, customer, items}, _from, state) do
    total = Enum.reduce(items, 0, fn %{price: p, qty: q}, acc -> acc + p * q end)
    id = state.next_id

    order = %{
      id: id,
      customer: customer,
      items: items,
      total: total,
      status: :pending,
      created_at: now()
    }

    # Avalia a condição de valor alto no momento da criação.
    if total >= @high_value_threshold do
      Notifier.notify(:high_value_order, %{order_id: id, total: total})
    end

    state = %{state | orders: Map.put(state.orders, id, order), next_id: id + 1}
    {:reply, {:ok, order}, state}
  end

  def handle_call({:update_status, id, status}, _from, state) do
    case Map.fetch(state.orders, id) do
      {:ok, order} ->
        updated = %{order | status: status}
        evaluate_status_rules(updated)
        {:reply, {:ok, updated}, %{state | orders: Map.put(state.orders, id, updated)}}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.fetch(state.orders, id), state}
  end

  def handle_call({:expire, timeout_ms}, _from, state) do
    current = now()

    {expired_ids, orders} =
      Enum.reduce(state.orders, {[], state.orders}, fn {id, order}, {ids, map} ->
        if order.status == :pending and current - order.created_at >= timeout_ms do
          {[id | ids], Map.put(map, id, %{order | status: :cancelled})}
        else
          {ids, map}
        end
      end)

    Enum.each(expired_ids, &Notifier.notify(:order_expired, %{order_id: &1}))

    {:reply, {:ok, length(expired_ids)}, %{state | orders: orders}}
  end

  # Condição de status: cancelamentos e envios geram notificações.
  defp evaluate_status_rules(%{status: :cancelled, id: id}),
    do: Notifier.notify(:order_cancelled, %{order_id: id})

  defp evaluate_status_rules(%{status: :shipped, id: id}),
    do: Notifier.notify(:order_shipped, %{order_id: id})

  defp evaluate_status_rules(_order), do: :ok

  defp now, do: System.monotonic_time(:millisecond)
end

defmodule Ecommerce.Scheduler do
  # Executa operações periódicas (expiração de pedidos pendentes).
  use GenServer
  require Logger
  alias Ecommerce.OrderManager

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, 5_000),
      order_timeout: Keyword.get(opts, :order_timeout, 60_000),
      runs: 0
    }

    schedule_tick(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {:ok, expired} = OrderManager.expire_stale(state.order_timeout)

    if expired > 0, do: Logger.info("[AGENDADOR] #{expired} pedido(s) expirado(s)")

    schedule_tick(state.interval)
    {:noreply, %{state | runs: state.runs + 1}}
  end

  defp schedule_tick(interval), do: Process.send_after(self(), :tick, interval)
end

defmodule Ecommerce.Watchdog do
  # Detecta encerramentos inesperados de processos monitorando os processos
  # registrados. Ao reinício feito pelo Supervisor, o processo é monitorado
  # novamente na próxima varredura.
  use GenServer
  alias Ecommerce.Notifier

  @watched [Ecommerce.Notifier, Ecommerce.OrderManager, Ecommerce.Scheduler]
  @scan_interval 500

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    send(self(), :scan)
    {:ok, %{refs: %{}}}
  end

  @impl true
  def handle_info(:scan, state) do
    refs = monitor_missing(state.refs)
    Process.send_after(self(), :scan, @scan_interval)
    {:noreply, %{state | refs: refs}}
  end

  # Mensagem enviada pelo runtime quando um processo monitorado termina.
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    {name, refs} = Map.pop(state.refs, ref)

    # :normal e :shutdown são encerramentos esperados; o restante é inesperado.
    if name && not expected?(reason) do
      Notifier.notify(:unexpected_exit, %{name: name, pid: pid, reason: reason})
    end

    {:noreply, %{state | refs: refs}}
  end

  defp expected?(:normal), do: true
  defp expected?(:shutdown), do: true
  defp expected?({:shutdown, _}), do: true
  defp expected?(_), do: false

  defp monitor_missing(refs) do
    monitored = MapSet.new(Map.values(refs))

    Enum.reduce(@watched, refs, fn name, acc ->
      cond do
        MapSet.member?(monitored, name) ->
          acc

        true ->
          case Process.whereis(name) do
            nil -> acc
            pid -> Map.put(acc, Process.monitor(pid), name)
          end
      end
    end)
  end
end

defmodule Ecommerce do
  # Ponto de entrada: sobe a árvore de supervisão com estratégia one_for_one.
  # Cada processo filho é reiniciado de forma independente se falhar.
  def start(opts \\ []) do
    children = [
      Ecommerce.Notifier,
      {Ecommerce.OrderManager, []},
      {Ecommerce.Scheduler, opts},
      Ecommerce.Watchdog
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Ecommerce.Supervisor)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------

{:ok, _sup} = Ecommerce.start(interval: 1_000, order_timeout: 3_000)

# Pedido de valor alto: dispara notificação de condição.
{:ok, o1} = Ecommerce.OrderManager.create_order("Ana", [%{price: 1_500, qty: 1}])

# Pedido de valor baixo: será expirado pelo Scheduler após o timeout.
{:ok, _o2} = Ecommerce.OrderManager.create_order("Bruno", [%{price: 20, qty: 2}])

# Mudança de status: dispara notificação de cancelamento.
{:ok, _} = Ecommerce.OrderManager.update_status(o1.id, :cancelled)

Process.sleep(5_000)

# Simula encerramento inesperado: o Watchdog notifica e o Supervisor reinicia.
Process.exit(Process.whereis(Ecommerce.Scheduler), :kill)

Process.sleep(2_000)
