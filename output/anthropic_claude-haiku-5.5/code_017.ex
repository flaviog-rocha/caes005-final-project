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

# ============================================================================
# order_manager.ex
# Gerenciamento de pedidos de e-commerce com atualizações atômicas,
# consulta de operações em andamento, expiração automática e notificações.
#
# Uso básico (comentário de referência):
#
#   {:ok, _} = OrderManager.start_link(ttl_ms: 60_000, sweep_ms: 1_000)
#   OrderManager.subscribe()
#   {:ok, order} = OrderManager.create(total: 1500, items: [:notebook])
#
#   import OrderManager.Macros
#   require OrderManager.Macros
#
#   OrderManager.Macros.atomic order.id do
#     set_fields(order, status: :paid)
#   end
#
#   OrderManager.operations()
#   receive do
#     {:order_notification, payload} -> IO.inspect(payload)
#   end
# ============================================================================

# ----------------------------------------------------------------------------
# Macros para declaração de regras de notificação em tempo de compilação.
# ----------------------------------------------------------------------------
defmodule OrderManager.Rules do
  @moduledoc false

  # Ao usar `use OrderManager.Rules`, o módulo recebe a macro `defrule/2`
  # e um atributo acumulador com todas as regras declaradas.
  defmacro __using__(_opts) do
    quote do
      import OrderManager.Rules, only: [defrule: 2]
      Module.register_attribute(__MODULE__, :rules, accumulate: true)
      @before_compile OrderManager.Rules
    end
  end

  # Declara uma regra: gera uma função `rule_<nome>(order)` que retorna
  # booleano e registra a mensagem de notificação associada.
  defmacro defrule(name, opts) when is_atom(name) do
    condition = Keyword.fetch!(opts, :condition)
    message = Keyword.fetch!(opts, :notify)
    fun = String.to_atom("rule_#{name}")
    order = Macro.var(:order, nil)

    quote do
      @rules {unquote(name), unquote(message)}
      def unquote(fun)(unquote(order)), do: unquote(condition)
    end
  end

  # Gera funções utilitárias a partir das regras acumuladas.
  defmacro __before_compile__(_env) do
    quote do
      @doc false
      def rules, do: Enum.reverse(@rules)

      @doc "Retorna os nomes das regras satisfeitas por um pedido."
      def matching(order) do
        for {name, _msg} <- rules(),
            apply(__MODULE__, String.to_atom("rule_#{name}"), [order]),
            do: name
      end

      @doc """
      Retorna as notificações das regras que passaram a valer na transição
      entre `previous` e `current` (evita notificações repetidas).
      """
      def transition_notifications(previous, current) do
        before = if previous, do: matching(previous), else: []
        now = matching(current)
        for {name, msg} <- rules(), name in now, name not in before, do: {name, msg}
      end
    end
  end
end

# ----------------------------------------------------------------------------
# Regras de notificação concretas do domínio.
# ----------------------------------------------------------------------------
defmodule OrderManager.Notifications do
  @moduledoc false
  use OrderManager.Rules

  defrule :high_value,
    condition: order.total >= 1000,
    notify: "Pedido de alto valor (>= 1000)"

  defrule :shipped,
    condition: order.status == :shipped,
    notify: "Pedido enviado"

  defrule :cancelled,
    condition: order.status == :cancelled,
    notify: "Pedido cancelado"

  defrule :expired,
    condition: order.status == :expired,
    notify: "Pedido expirado automaticamente"
end

# ----------------------------------------------------------------------------
# Macros de uso público.
# ----------------------------------------------------------------------------
defmodule OrderManager.Macros do
  @moduledoc """
  Macros de conveniência.

  * `atomic/2` - executa uma transformação sobre o pedido de forma atômica.
    O bloco recebe a variável `order` (snapshot) e deve retornar o pedido
    atualizado ou `{:error, motivo}`. Qualquer erro, exceção ou timeout
    descarta a transformação inteira.
  * `set_fields/2` - gera um pedido com os campos informados substituídos.
  """

  defmacro atomic(order_id, do: block) do
    order = Macro.var(:order, nil)

    quote do
      OrderManager.transaction(unquote(order_id), fn unquote(order) ->
        unquote(block)
      end)
    end
  end

  defmacro set_fields(order, fields) do
    quote do
      Map.merge(unquote(order), Map.new(unquote(fields)))
    end
  end
end

# ----------------------------------------------------------------------------
# GenServer que mantém o estado dos pedidos e coordena as operações.
# ----------------------------------------------------------------------------
defmodule OrderManager.Server do
  @moduledoc false
  use GenServer

  @task_supervisor OrderManager.TaskSupervisor
  @default_ttl_ms 30 * 60_000
  @default_sweep_ms 5_000
  @default_op_timeout_ms 10_000

  # Transições de estado permitidas.
  @transitions %{
    pending: [:paid, :cancelled, :expired],
    paid: [:shipped, :cancelled],
    shipped: [],
    cancelled: [],
    expired: []
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    state = %{
      # id => pedido
      orders: %{},
      # ref da task => operação em andamento
      ops: %{},
      # id do pedido => ref da task (trava por pedido)
      locks: %{},
      # ref de monitoração => pid assinante
      subs: %{},
      ttl_ms: Keyword.get(opts, :ttl_ms, @default_ttl_ms),
      sweep_ms: Keyword.get(opts, :sweep_ms, @default_sweep_ms),
      op_timeout_ms: Keyword.get(opts, :op_timeout_ms, @default_op_timeout_ms)
    }

    schedule_sweep(state)
    {:ok, state}
  end

  # --------------------------------------------------------------------------
  # Chamadas síncronas
  # --------------------------------------------------------------------------

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    total = Map.get(attrs, :total)

    if is_number(total) and total >= 0 do
      now = now_ms()
      id = :erlang.unique_integer([:positive, :monotonic])
      ttl = Map.get(attrs, :ttl_ms, state.ttl_ms)

      order = %{
        id: id,
        status: :pending,
        total: total,
        items: Map.get(attrs, :items, []),
        version: 1,
        created_at: now,
        expires_at: now + ttl
      }

      {:reply, {:ok, order}, %{state | orders: Map.put(state.orders, id, order)}}
    else
      {:reply, {:error, :invalid_total}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch_order(state, id), state}
  end

  # Inicia uma transação atômica sobre o pedido. A resposta só é enviada
  # quando a Task termina (commit ou rollback).
  def handle_call({:transaction, id, fun}, from, state) do
    cond do
      not Map.has_key?(state.orders, id) ->
        {:reply, {:error, :not_found}, state}

      Map.has_key?(state.locks, id) ->
        {:reply, {:error, :busy}, state}

      true ->
        order = Map.fetch!(state.orders, id)

        # A função roda em uma Task supervisionada, sobre um snapshot imutável.
        task = Task.Supervisor.async_nolink(@task_supervisor, fn -> execute(fun, order) end)
        timer = Process.send_after(self(), {:op_timeout, task.ref}, state.op_timeout_ms)

        op = %{
          ref: task.ref,
          task: task,
          order_id: id,
          from: from,
          timer: timer,
          started_at: now_ms()
        }

        {:noreply,
         %{
           state
           | ops: Map.put(state.ops, task.ref, op),
             locks: Map.put(state.locks, id, task.ref)
         }}
    end
  end

  # Lista as operações em andamento.
  def handle_call(:operations, _from, state) do
    now = now_ms()

    list =
      for {_ref, op} <- state.ops do
        %{
          order_id: op.order_id,
          status: :running,
          started_at: op.started_at,
          running_ms: now - op.started_at
        }
      end

    {:reply, list, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    mref = Process.monitor(pid)
    {:reply, :ok, %{state | subs: Map.put(state.subs, mref, pid)}}
  end

  # --------------------------------------------------------------------------
  # Mensagens assíncronas
  # --------------------------------------------------------------------------

  # Resultado de uma Task de transformação.
  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.ops, ref) do
      {nil, _} ->
        {:noreply, state}

      {op, ops} ->
        Process.cancel_timer(op.timer)
        Process.demonitor(ref, [:flush])

        state = %{state | ops: ops, locks: Map.delete(state.locks, op.order_id)}

        case result do
          {:ok, new_order} ->
            {:noreply, commit(state, op, new_order)}

          {:error, reason} ->
            GenServer.reply(op.from, {:error, reason})
            {:noreply, state}
        end
    end
  end

  # Timeout de operação: encerra a Task e descarta a transformação.
  def handle_info({:op_timeout, ref}, state) do
    case Map.pop(state.ops, ref) do
      {nil, _} ->
        {:noreply, state}

      {op, ops} ->
        Task.shutdown(op.task, :brutal_kill)
        GenServer.reply(op.from, {:error, :timeout})
        {:noreply, %{state | ops: ops, locks: Map.delete(state.locks, op.order_id)}}
    end
  end

  # Falha de Task ou assinante que caiu.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    cond do
      Map.has_key?(state.ops, ref) ->
        {op, ops} = Map.pop(state.ops, ref)
        Process.cancel_timer(op.timer)
        GenServer.reply(op.from, {:error, {:crashed, reason}})
        {:noreply, %{state | ops: ops, locks: Map.delete(state.locks, op.order_id)}}

      Map.has_key?(state.subs, ref) ->
        {:noreply, %{state | subs: Map.delete(state.subs, ref)}}

      true ->
        {:noreply, state}
    end
  end

  # Expiração automática de pedidos pendentes e não bloqueados.
  def handle_info(:sweep, state) do
    now = now_ms()

    {expired, kept} =
      Enum.reduce(state.orders, {[], %{}}, fn {id, order}, {exp, keep} ->
        if order.status == :pending and order.expires_at <= now and
             not Map.has_key?(state.locks, id) do
          {[order | exp], keep}
        else
          {exp, Map.put(keep, id, order)}
        end
      end)

    state = %{state | orders: kept}

    Enum.each(expired, fn order ->
      expired_order = %{order | status: :expired, version: order.version + 1}
      notifs = OrderManager.Notifications.transition_notifications(order, expired_order)
      notify(state, order.id, notifs)
    end)

    schedule_sweep(state)
    {:noreply, state}
  end

  # --------------------------------------------------------------------------
  # Funções privadas
  # --------------------------------------------------------------------------

  # Executa a transformação de forma isolada. Qualquer falha vira erro,
  # garantindo que nada seja aplicado parcialmente.
  defp execute(fun, order) do
    case fun.(order) do
      %{} = new -> validate(order, new)
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp validate(old, new) do
    cond do
      new.id != old.id ->
        {:error, :id_change_forbidden}

      not is_number(new.total) or new.total < 0 ->
        {:error, :invalid_total}

      new.status != old.status and not transition?(old.status, new.status) ->
        {:error, {:invalid_transition, old.status, new.status}}

      true ->
        {:ok, new}
    end
  end

  defp transition?(from, to), do: to in Map.get(@transitions, from, [])

  # Aplica o commit de forma única e dispara notificações.
  defp commit(state, op, new_order) do
    case Map.fetch(state.orders, op.order_id) do
      {:ok, current} ->
        committed = Map.update!(new_order, :version, &(&1 + 1))
        notifs = OrderManager.Notifications.transition_notifications(current, committed)

        notify(state, op.order_id, notifs)
        GenServer.reply(op.from, {:ok, committed})

        %{state | orders: Map.put(state.orders, op.order_id, committed)}

      :error ->
        GenServer.reply(op.from, {:error, :not_found})
        state
    end
  end

  # Envia notificações aos assinantes através de Task supervisionada.
  defp notify(_state, _order_id, []), do: :ok

  defp notify(state, order_id, notifications) do
    subs = Map.values(state.subs)

    payloads =
      for {rule, msg} <- notifications do
        %{order_id: order_id, rule: rule, message: msg, at: now_ms()}
      end

    Task.Supervisor.start_child(@task_supervisor, fn ->
      for pid <- subs, payload <- payloads do
        send(pid, {:order_notification, payload})
      end
    end)

    :ok
  end

  defp fetch_order(state, id) do
    case Map.fetch(state.orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :not_found}
    end
  end

  defp schedule_sweep(state) do
    Process.send_after(self(), :sweep, state.sweep_ms)
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end

# ----------------------------------------------------------------------------
# API pública e supervisão.
# ----------------------------------------------------------------------------
defmodule OrderManager do
  @moduledoc """
  API pública do gerenciador de pedidos.

  Inicia um Task.Supervisor (para as transformações e notificações) e o
  servidor de pedidos sob um supervisor próprio.
  """

  @task_supervisor OrderManager.TaskSupervisor
  @server OrderManager.Server

  @doc "Inicia a árvore de supervisão. Opções: :ttl_ms, :sweep_ms, :op_timeout_ms."
  def start_link(opts \\ []) do
    children = [
      {Task.Supervisor, name: @task_supervisor},
      {@server, opts}
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: OrderManager.Supervisor)
  end

  @doc "Cria um pedido. Aceita :total, :items e opcionalmente :ttl_ms."
  def create(attrs), do: GenServer.call(@server, {:create, Map.new(attrs)})

  @doc "Consulta um pedido pelo id."
  def get(id), do: GenServer.call(@server, {:get, id})

  @doc false
  # Usada pela macro `OrderManager.Macros.atomic/2`.
  def transaction(id, fun) when is_function(fun, 1) do
    GenServer.call(@server, {:transaction, id, fun}, 15_000)
  end

  @doc "Lista as operações atômicas em andamento."
  def operations, do: GenServer.call(@server, :operations)

  @doc "Inscreve um processo para receber notificações de regras."
  def subscribe(pid \\ self()), do: GenServer.call(@server, {:subscribe, pid})
end
