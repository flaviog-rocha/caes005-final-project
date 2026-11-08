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

# Arquivo: order_system.exs
# Execução: elixir order_system.exs

defmodule Ecommerce.Notifier do
  @moduledoc """
  Recebe eventos do sistema e despacha notificações conforme o nível.
  Mantém um histórico limitado para consulta.
  """
  use GenServer
  require Logger

  @history_limit 100

  def start_link(_opts), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  # Envio assíncrono para não bloquear o fluxo de pedidos.
  def notify(level, event, payload) when level in [:info, :warning, :error] do
    GenServer.cast(__MODULE__, {:notify, level, event, payload})
  end

  def history, do: GenServer.call(__MODULE__, :history)

  @impl true
  def init(history), do: {:ok, history}

  @impl true
  def handle_cast({:notify, level, event, payload}, history) do
    entry = %{at: DateTime.utc_now(), level: level, event: event, payload: payload}
    message = "[#{event}] #{inspect(payload)}"

    case level do
      :info -> Logger.info(message)
      :warning -> Logger.warning(message)
      :error -> Logger.error(message)
    end

    {:noreply, Enum.take([entry | history], @history_limit)}
  end

  @impl true
  def handle_call(:history, _from, history), do: {:reply, history, history}
end

defmodule Ecommerce.Order do
  @moduledoc """
  Processo que representa um pedido.
  É iniciado sob um DynamicSupervisor com restart :transient, ou seja,
  é reiniciado somente após encerramentos anormais.
  Observação: o estado não é persistido; após um crash o pedido é
  reinicializado a partir dos argumentos originais de criação.
  """
  use GenServer, restart: :transient

  alias Ecommerce.Notifier

  # Condição de notificação: pedidos com valor igual ou superior a este limite.
  @high_value_threshold 1_000

  def start_link(%{id: id} = opts) do
    GenServer.start_link(__MODULE__, opts, name: via(id))
  end

  def via(id), do: {:via, Registry, {Ecommerce.OrderRegistry, id}}

  def status(id), do: call(id, :status)

  def pay(id), do: call(id, :pay)

  # Usado pelo agendador: expira o pedido se ainda aguarda pagamento além do timeout.
  def expire_if_stale(pid, timeout_ms) do
    GenServer.call(pid, {:expire_if_stale, timeout_ms})
  catch
    :exit, _ -> :gone
  end

  # Falha simulada para testar a detecção de encerramentos inesperados.
  def crash(id), do: GenServer.cast(via(id), :crash)

  defp call(id, message) do
    GenServer.call(via(id), message)
  catch
    :exit, _ -> {:error, :not_found}
  end

  @impl true
  def init(%{id: id, total: total}) do
    state = %{
      id: id,
      total: total,
      status: :pending_payment,
      created_at: System.monotonic_time(:millisecond)
    }

    if total >= @high_value_threshold do
      Notifier.notify(:info, :high_value_order, %{id: id, total: total})
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state), do: {:reply, {:ok, state.status}, state}

  def handle_call(:pay, _from, %{status: :pending_payment} = state) do
    Notifier.notify(:info, :order_paid, %{id: state.id, total: state.total})
    {:reply, :ok, %{state | status: :paid}}
  end

  def handle_call(:pay, _from, state) do
    {:reply, {:error, {:invalid_status, state.status}}, state}
  end

  def handle_call({:expire_if_stale, timeout_ms}, _from, state) do
    age = System.monotonic_time(:millisecond) - state.created_at

    if state.status == :pending_payment and age >= timeout_ms do
      Notifier.notify(:warning, :order_expired, %{id: state.id, age_ms: age})
      # Encerramento normal: o Watchdog não tentará reiniciar o processo.
      {:stop, :normal, :expired, %{state | status: :cancelled}}
    else
      {:reply, :active, state}
    end
  end

  @impl true
  def handle_cast(:crash, _state) do
    raise "falha simulada no processo do pedido"
  end
end

defmodule Ecommerce.Orders do
  @moduledoc """
  API pública de gerenciamento de pedidos.
  """
  alias Ecommerce.{Order, Watchdog}

  def create(total) when is_number(total) and total > 0 do
    id = System.unique_integer([:positive, :monotonic])

    case DynamicSupervisor.start_child(Ecommerce.OrderSupervisor, {Order, %{id: id, total: total}}) do
      {:ok, _pid} ->
        Watchdog.watch(id)
        {:ok, id}

      error ->
        error
    end
  end

  def pay(id), do: Order.pay(id)

  def status(id), do: Order.status(id)
end

defmodule Ecommerce.Watchdog do
  @moduledoc """
  Monitora os processos dos pedidos com Process.monitor/1.
  Ao detectar um encerramento anormal, emite uma notificação e tenta
  reconectar-se ao novo processo criado pelo supervisor.
  Observação: o estado de monitoramento não sobrevive a um crash deste próprio
  processo; ele é reconstruído pelo supervisor raiz ao reiniciar.
  """
  use GenServer

  alias Ecommerce.Notifier

  @max_attempts 5
  @retry_ms 50

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def watch(id), do: GenServer.cast(__MODULE__, {:watch, id})

  @impl true
  def init(_), do: {:ok, %{monitors: %{}}}

  @impl true
  def handle_cast({:watch, id}, state), do: {:noreply, attach(id, 1, state)}

  @impl true
  def handle_info({:retry, id, attempt}, state), do: {:noreply, attach(id, attempt, state)}

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        if normal_exit?(reason) do
          Notifier.notify(:info, :order_process_stopped, %{id: id, reason: reason})
          {:noreply, state}
        else
          Notifier.notify(:error, :order_process_crashed, %{id: id, reason: reason})
          Process.send_after(self(), {:retry, id, 1}, @retry_ms)
          {:noreply, state}
        end
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp attach(id, attempt, state) do
    alive =
      Registry.lookup(Ecommerce.OrderRegistry, id)
      |> Enum.filter(fn {pid, _} -> Process.alive?(pid) end)

    case alive do
      [{pid, _} | _] ->
        ref = Process.monitor(pid)
        %{state | monitors: Map.put(state.monitors, ref, id)}

      [] when attempt < @max_attempts ->
        Process.send_after(self(), {:retry, id, attempt + 1}, @retry_ms)
        state

      [] ->
        Notifier.notify(:error, :order_recovery_failed, %{id: id, attempts: attempt})
        state
    end
  end

  defp normal_exit?(:normal), do: true
  defp normal_exit?(:shutdown), do: true
  defp normal_exit?({:shutdown, _}), do: true
  defp normal_exit?(_), do: false
end

defmodule Ecommerce.Scheduler do
  @moduledoc """
  Executa periodicamente:
  - expiração de pedidos aguardando pagamento além do timeout;
  - registro de métricas do número de pedidos ativos.
  """
  use GenServer
  require Logger

  alias Ecommerce.{Order, Notifier}

  @default_interval_ms 1_000
  # Em produção, use algo como 30 minutos.
  @default_payment_timeout_ms 5_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval_ms, @default_interval_ms),
      timeout: Keyword.get(opts, :payment_timeout_ms, @default_payment_timeout_ms)
    }

    schedule(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    orders = Registry.select(Ecommerce.OrderRegistry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])

    Enum.each(orders, fn {_id, pid} -> Order.expire_if_stale(pid, state.timeout) end)

    Logger.debug("Pedidos ativos: #{length(orders)}")

    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end

defmodule Ecommerce.OrderSystem do
  @moduledoc """
  Supervisor raiz do sistema de pedidos.
  Estratégia :one_for_one: falhas isoladas não derrubam os demais componentes.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Ecommerce.Notifier, []},
      {Registry, keys: :unique, name: Ecommerce.OrderRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Ecommerce.OrderSupervisor},
      {Ecommerce.Watchdog, []},
      {Ecommerce.Scheduler, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 5)
  end
end

# Demonstração
{:ok, _sup} = Ecommerce.OrderSystem.start_link(interval_ms: 1_000, payment_timeout_ms: 3_000)

{:ok, paid_id} = Ecommerce.Orders.create(250)
{:ok, big_id} = Ecommerce.Orders.create(1_500)
{:ok, _expire_id} = Ecommerce.Orders.create(80)

:ok = Ecommerce.Orders.pay(paid_id)

Ecommerce.Order.crash(big_id)
Process.sleep(300)

IO.inspect(Ecommerce.Orders.status(big_id), label: "status após crash/restart")

Process.sleep(4_000)

IO.inspect(length(Ecommerce.Notifier.history()), label: "notificações registradas")
