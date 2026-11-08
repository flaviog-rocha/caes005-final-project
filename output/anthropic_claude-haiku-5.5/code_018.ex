# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Fazer uso de macros.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Implementar expiração automática de registros.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task.Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# ============================================================
# Gerenciamento de pedidos de e-commerce (arquivo autocontido)
# Requisitos: atualizações atômicas, macros, consulta de operações em
# andamento, expiração automática, notificações por condição e Task.Supervisor.
# Execução: elixir pedidos.exs
# ============================================================

# ------------------------------------------------------------
# Macros: geração de regras em compile-time e bloco atômico
# ------------------------------------------------------------
defmodule ECommerce.Macros do
  # Gera, em tempo de compilação, a função `transition_allowed?/2`
  # a partir de uma lista declarativa `status_origem: [status_destino, ...]`.
  defmacro transitions(rules) do
    clauses =
      for {from, targets} <- rules, to <- targets do
        quote do
          def transition_allowed?(unquote(from), unquote(to)), do: true
        end
      end

    quote do
      unquote_splicing(clauses)
      def transition_allowed?(_from, _to), do: false
    end
  end

  # Executa o bloco sobre `state` de forma atômica.
  # O bloco deve retornar `{:ok, novo_estado}` ou `{:error, motivo}`.
  # Em caso de erro (ou exceção) devolve `{:error, motivo, estado_original}`,
  # garantindo que nenhuma alteração parcial seja aplicada.
  defmacro atomic(state, do: block) do
    quote do
      original = unquote(state)

      try do
        case unquote(block) do
          {:ok, new_state} -> {:ok, new_state}
          {:error, reason} -> {:error, reason, original}
          other -> {:error, {:invalid_return, other}, original}
        end
      rescue
        exception -> {:error, exception, original}
      end
    end
  end
end

# ------------------------------------------------------------
# Modelo de pedido e regras de transição de status
# ------------------------------------------------------------
defmodule ECommerce.Order do
  @moduledoc false
  import ECommerce.Macros

  defstruct [:id, :status, :total, :items, :created_at, :expires_at]

  # Regras de transição de status (geradas pela macro)
  transitions novo: [:pago, :cancelado, :expirado],
              pago: [:enviado, :cancelado],
              enviado: [:entregue],
              entregue: [],
              cancelado: [],
              expirado: []

  # Cria um pedido calculando o total em centavos
  def new(id, items, ttl_ms, now) do
    total = Enum.reduce(items, 0, fn %{price: p, qty: q}, acc -> acc + p * q end)

    %__MODULE__{
      id: id,
      status: :novo,
      items: items,
      total: total,
      created_at: now,
      expires_at: now + ttl_ms
    }
  end

  # Aplica a transição somente se permitida
  def transition(%__MODULE__{status: from} = order, to) do
    if transition_allowed?(from, to) do
      {:ok, %{order | status: to}}
    else
      {:error, {:invalid_transition, from, to}}
    end
  end

  # Apenas pedidos novos (ainda não pagos) expiram automaticamente
  def expired?(%__MODULE__{status: :novo, expires_at: exp}, now), do: now >= exp
  def expired?(_order, _now), do: false
end

# ------------------------------------------------------------
# Notificações: regras de condição avaliadas a cada evento
# ------------------------------------------------------------
defmodule ECommerce.Notifier do
  @moduledoc false

  @high_value_threshold 100_000
  @backlog_threshold 100

  # Avalia todas as regras para um evento e retorna as notificações geradas
  def evaluate(event, order, backlog) do
    for {type, predicate} <- rules(), predicate.(event, order, backlog) do
      %{type: type, order_id: order.id, event: event, at: DateTime.utc_now()}
    end
  end

  # Envia notificações de forma assíncrona via Task.Supervisor
  # para não bloquear o gerenciador de pedidos
  def dispatch(notifications, subscribers, sup) do
    for notification <- notifications, pid <- subscribers do
      Task.Supervisor.start_child(sup, fn ->
        send(pid, {:ecommerce_notification, notification})
      end)
    end

    :ok
  end

  defp rules do
    [
      high_value_order: fn event, order, _backlog ->
        event == :created and order.total >= @high_value_threshold
      end,
      payment_confirmed: fn event, _order, _backlog ->
        event == {:status, :pago}
      end,
      order_shipped: fn event, _order, _backlog ->
        event == {:status, :enviado}
      end,
      order_expired: fn event, _order, _backlog ->
        event == :expired
      end,
      backlog_alert: fn event, _order, backlog ->
        event == :created and backlog >= @backlog_threshold
      end
    ]
  end
end

# ------------------------------------------------------------
# Gerenciador de pedidos (GenServer)
# ------------------------------------------------------------
defmodule ECommerce.OrderManager do
  @moduledoc false
  use GenServer
  import ECommerce.Macros

  alias ECommerce.{Notifier, Order}

  @sup ECommerce.TaskSup
  @default_ttl_ms 15 * 60 * 1000
  @default_sweep_ms 1_000
  @default_op_retention_ms 60 * 1000

  # ---------------- API pública ----------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_order(items, opts \\ []) do
    GenServer.call(__MODULE__, {:create_order, items, opts})
  end

  def get_order(id) do
    GenServer.call(__MODULE__, {:get_order, id})
  end

  def transition_order(id, to), do: transition_orders([{id, to}])

  # Atualização em lote: ou todas as transições são aplicadas, ou nenhuma
  def transition_orders(pairs) do
    GenServer.call(__MODULE__, {:transition_orders, pairs})
  end

  # Executa uma operação assíncrona (fun recebe o pedido e retorna
  # {:ok, novo_status} | {:error, motivo}) sob o Task.Supervisor
  def run_operation(order_id, name, fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:run_operation, order_id, name, fun})
  end

  # Consulta operações: :all, :running, :completed ou :failed
  def operations(filter \\ :all) do
    GenServer.call(__MODULE__, {:operations, filter})
  end

  def operation(op_id) do
    GenServer.call(__MODULE__, {:operation, op_id})
  end

  def subscribe(pid \\ self()) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  # ---------------- Callbacks ----------------

  @impl true
  def init(opts) do
    state = %{
      orders: %{},
      operations: %{},
      tasks: %{},
      subscribers: MapSet.new(),
      ttl_ms: Keyword.get(opts, :ttl_ms, @default_ttl_ms),
      sweep_ms: Keyword.get(opts, :sweep_ms, @default_sweep_ms),
      op_retention_ms: Keyword.get(opts, :op_retention_ms, @default_op_retention_ms)
    }

    schedule_sweep(state.sweep_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:create_order, items, opts}, _from, state) do
    case validate_items(items) do
      :ok ->
        id = Keyword.get_lazy(opts, :id, fn -> make_id("ord") end)
        ttl = Keyword.get(opts, :ttl_ms, state.ttl_ms)

        if Map.has_key?(state.orders, id) do
          {:reply, {:error, :duplicate_id}, state}
        else
          order = Order.new(id, items, ttl, now())
          new_state = %{state | orders: Map.put(state.orders, id, order)}
          {:reply, {:ok, order}, notify(new_state, :created, order)}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_order, id}, _from, state) do
    {:reply, fetch(state.orders, id), state}
  end

  def handle_call({:transition_orders, pairs}, _from, state) do
    case commit_transitions(state, pairs) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:run_operation, order_id, name, fun}, _from, state) do
    case fetch(state.orders, order_id) do
      {:error, _} = error ->
        {:reply, error, state}

      {:ok, order} ->
        op_id = make_id("op")
        # A tarefa roda isolada sob o Task.Supervisor; o resultado só é
        # aplicado pelo GenServer, de forma atômica, ao ser concluído
        task = Task.Supervisor.async_nolink(@sup, fn -> fun.(order) end)

        op = %{
          id: op_id,
          order_id: order_id,
          name: name,
          status: :running,
          started_at: now(),
          finished_at: nil,
          result: nil
        }

        new_state = %{
          state
          | operations: Map.put(state.operations, op_id, op),
            tasks: Map.put(state.tasks, task.ref, op_id)
        }

        {:reply, {:ok, op_id}, new_state}
    end
  end

  def handle_call({:operations, filter}, _from, state) do
    ops =
      state.operations
      |> Map.values()
      |> Enum.filter(&(filter == :all or &1.status == filter))
      |> Enum.sort_by(& &1.started_at)

    {:reply, ops, state}
  end

  def handle_call({:operation, op_id}, _from, state) do
    {:reply, fetch(state.operations, op_id), state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  @impl true
  # Resultado de uma tarefa concluída com sucesso
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.tasks, ref) do
      {nil, _tasks} -> {:noreply, state}
      {op_id, tasks} -> {:noreply, finish_operation(%{state | tasks: tasks}, op_id, result)}
    end
  end

  # Monitor: tarefa caiu ou assinante desconectou
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}

      {op_id, tasks} ->
        {:noreply,
         finish_operation(%{state | tasks: tasks}, op_id, {:error, {:crashed, reason}})}
    end
  end

  # Varredura periódica: expira pedidos e remove operações antigas
  def handle_info(:sweep, state) do
    now = now()

    {expired, active} =
      Enum.split_with(state.orders, fn {_id, order} -> Order.expired?(order, now) end)

    state = %{state | orders: Map.new(active)}

    state =
      Enum.reduce(expired, state, fn {_id, order}, acc ->
        notify(acc, :expired, %{order | status: :expirado})
      end)

    ops =
      Enum.reject(state.operations, fn {_id, op} ->
        finished_older_than?(op, now, state.op_retention_ms)
      end)

    schedule_sweep(state.sweep_ms)
    {:noreply, %{state | operations: Map.new(ops)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------- Funções privadas ----------------

  # Aplica um conjunto de transições de forma atômica (tudo ou nada)
  defp commit_transitions(state, pairs) do
    result =
      atomic state do
        case batch_transition(state, pairs) do
          {:ok, new_state, changed} -> {:ok, {new_state, changed}}
          {:error, reason} -> {:error, reason}
        end
      end

    case result do
      {:ok, {new_state, changed}} ->
        # Notificações só são geradas após a confirmação do lote
        {:ok,
         Enum.reduce(changed, new_state, fn order, acc ->
           notify(acc, {:status, order.status}, order)
         end)}

      {:error, reason, _original} ->
        {:error, reason}
    end
  end

  defp batch_transition(state, pairs) do
    result =
      Enum.reduce_while(pairs, {:ok, state.orders, []}, fn {id, to}, {:ok, orders, changed} ->
        case transition_in(orders, id, to) do
          {:ok, orders, order} -> {:cont, {:ok, orders, [order | changed]}}
          {:error, reason} -> {:halt, {:error, {id, reason}}}
        end
      end)

    case result do
      {:ok, orders, changed} -> {:ok, %{state | orders: orders}, Enum.reverse(changed)}
      {:error, _reason} = error -> error
    end
  end

  defp transition_in(orders, id, to) do
    case Map.fetch(orders, id) do
      :error ->
        {:error, :not_found}

      {:ok, order} ->
        case Order.transition(order, to) do
          {:ok, updated} -> {:ok, Map.put(orders, id, updated), updated}
          {:error, _reason} = error -> error
        end
    end
  end

  # Conclui uma operação: aplica o resultado somente se tudo for válido
  defp finish_operation(state, op_id, result) do
    op = Map.fetch!(state.operations, op_id)

    {status, new_state, reason} =
      case result do
        {:ok, to} ->
          case commit_transitions(state, [{op.order_id, to}]) do
            {:ok, committed} -> {:completed, committed, :ok}
            {:error, reason} -> {:failed, state, reason}
          end

        {:error, reason} ->
          {:failed, state, reason}

        other ->
          {:failed, state, {:invalid_result, other}}
      end

    op = %{op | status: status, finished_at: now(), result: reason}
    %{new_state | operations: Map.put(new_state.operations, op_id, op)}
  end

  defp notify(state, event, order) do
    backlog = count_pending(state.orders)
    notifications = Notifier.evaluate(event, order, backlog)
    Notifier.dispatch(notifications, state.subscribers, @sup)
    state
  end

  defp count_pending(orders) do
    Enum.count(orders, fn {_id, order} -> order.status == :novo end)
  end

  defp validate_items([_ | _] = items) do
    if Enum.all?(items, &valid_item?/1), do: :ok, else: {:error, :invalid_items}
  end

  defp validate_items(_), do: {:error, :empty_items}

  defp valid_item?(%{sku: sku, price: p, qty: q})
       when is_binary(sku) and is_integer(p) and p >= 0 and is_integer(q) and q > 0,
       do: true

  defp valid_item?(_), do: false

  defp fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end

  defp finished_older_than?(%{finished_at: nil}, _now, _retention), do: false
  defp finished_older_than?(%{finished_at: t}, now, retention), do: now - t >= retention

  defp schedule_sweep(ms), do: Process.send_after(self(), :sweep, ms)

  defp now, do: System.monotonic_time(:millisecond)

  defp make_id(prefix), do: "#{prefix}_#{System.unique_integer([:positive, :monotonic])}"
end

# ------------------------------------------------------------
# Árvore de supervisão: Task.Supervisor + gerenciador
# ------------------------------------------------------------
defmodule ECommerce.App do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: ECommerce.TaskSup},
      {ECommerce.OrderManager, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ------------------------------------------------------------
# Demonstração de uso
# ------------------------------------------------------------
defmodule ECommerce.Demo do
  @moduledoc false
  alias ECommerce.OrderManager, as: OM

  def run do
    {:ok, _} = ECommerce.App.start_link(sweep_ms: 200)
    :ok = OM.subscribe()

    # Pedido de alto valor dispara notificação
    {:ok, order} = OM.create_order([%{sku: "NOTEBOOK", price: 250_000, qty: 1}])
    IO.inspect(order.status, label: "pedido criado")
    drain()

    # Operação assíncrona sob o Task.Supervisor
    {:ok, op} =
      OM.run_operation(order.id, :pagamento, fn o ->
        Process.sleep(300)
        if o.status == :novo, do: {:ok, :pago}, else: {:error, :estado_invalido}
      end)

    IO.inspect(OM.operations(:running), label: "operações em andamento")
    Process.sleep(500)
    IO.inspect(OM.operation(op), label: "operação concluída")
    drain()

    # Lote com um item inválido: nada deve ser aplicado
    IO.inspect(
      OM.transition_orders([{order.id, :enviado}, {"inexistente", :enviado}]),
      label: "lote inválido"
    )

    IO.inspect(OM.get_order(order.id), label: "estado após lote inválido")

    # Lote válido
    IO.inspect(OM.transition_orders([{order.id, :enviado}]), label: "lote válido")
    drain()

    # Expiração automática de pedido não pago
    {:ok, short} = OM.create_order([%{sku: "MOUSE", price: 9_990, qty: 2}], ttl_ms: 300)
    Process.sleep(800)
    IO.inspect(OM.get_order(short.id), label: "pedido expirado")
    drain()
  end

  defp drain do
    receive do
      {:ecommerce_notification, notification} ->
        IO.inspect(notification, label: "notificação")
        drain()
    after
      200 -> :ok
    end
  end
end

ECommerce.Demo.run()
