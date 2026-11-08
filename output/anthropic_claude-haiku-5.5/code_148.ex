# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de pedidos de e-commerce com OTP puro (sem dependências externas).
#
# Requisitos atendidos:
#   1. Novas tentativas após falhas temporárias: retorno {:error, :temporary, motivo}
#      dispara nova tentativa com backoff exponencial até `max_attempts`.
#   2. Expiração automática: pedidos pendentes expiram após `order_ttl_ms` e resultados
#      de operações expiram após `idempotency_ttl_ms`; uma varredura periódica os remove.
#   3. Execução periódica: `schedule_periodic/4` executa uma função a cada intervalo,
#      sem sobreposição (o próximo disparo só é agendado após a conclusão do anterior).
#   4. Consulta de estado: `running_operations/1` e `operation_status/2`.
#   5. Idempotência: cada chave de operação é executada no máximo uma vez. Chamadas
#      concorrentes aguardam o mesmo resultado e chamadas posteriores recebem o resultado
#      armazenado até a expiração.
#
# Exemplo de uso (em iex):
#
#   {:ok, _} = Ecommerce.OrderManager.start_link(max_attempts: 4)
#   {:ok, pedido} = Ecommerce.OrderManager.create_order(%{total: 150.0, itens: [:camiseta]})
#
#   gateway = fn ->
#     case :rand.uniform(3) do
#       1 -> {:ok, %{txn_id: "abc123"}}
#       2 -> {:error, :temporary, :timeout}
#       3 -> {:error, :permanent, :cartao_recusado}
#     end
#   end
#
#   Ecommerce.OrderManager.pay_order(pedido.id, gateway)
#   Ecommerce.OrderManager.pay_order(pedido.id, gateway)   # não executa o gateway de novo
#   Ecommerce.OrderManager.running_operations()
#   Ecommerce.OrderManager.operation_status({:pay, pedido.id})
#
#   Ecommerce.OrderManager.schedule_periodic(:relatorio, 60_000, fn ->
#     {:ok, :relatorio_gerado}
#   end)

defmodule Ecommerce.OrderManager do
  @moduledoc """
  Gerenciador de pedidos com retentativas, expiração, tarefas periódicas,
  consulta de estado e idempotência de operações.

  Função de operação esperada: `fun/0` retornando:
    * `{:ok, valor}` - sucesso (resultado é armazenado e reaproveitado);
    * `{:error, :temporary, motivo}` - falha temporária (nova tentativa);
    * `{:error, :permanent, motivo}` - falha definitiva (sem nova tentativa).
  Exceções lançadas pela função são tratadas como falhas permanentes.
  """

  use GenServer

  # Valores padrão (em milissegundos)
  @default_max_attempts 3
  @default_backoff_ms 100
  @default_idempotency_ttl :timer.hours(24)
  @default_order_ttl :timer.minutes(30)
  @default_sweep_interval :timer.seconds(30)
  @default_timeout 30_000

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o gerenciador. Opções: :name, :max_attempts, :base_backoff_ms, :idempotency_ttl_ms, :order_ttl_ms, :sweep_interval_ms."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Cria um pedido pendente que expira automaticamente se não for pago."
  def create_order(server \\ __MODULE__, attrs) do
    GenServer.call(server, {:create_order, attrs})
  end

  @doc "Consulta um pedido pelo id."
  def get_order(server \\ __MODULE__, id) do
    GenServer.call(server, {:get_order, id})
  end

  @doc "Paga um pedido usando `gateway_fun` (idempotente e com retentativas)."
  def pay_order(server \\ __MODULE__, order_id, gateway_fun, timeout \\ @default_timeout) do
    execute(server, {:pay, order_id}, gateway_fun, timeout)
  end

  @doc """
  Executa uma operação identificada por `key` no máximo uma vez.
  Retorna o resultado final da operação (`{:ok, valor}` ou `{:error, motivo}`).
  """
  def execute(server \\ __MODULE__, key, fun, timeout \\ @default_timeout) when is_function(fun, 0) do
    GenServer.call(server, {:execute, key, fun}, timeout)
  end

  @doc "Lista as operações atualmente em execução."
  def running_operations(server \\ __MODULE__) do
    GenServer.call(server, :running_operations)
  end

  @doc "Retorna o estado de uma operação específica ou `:not_found`."
  def operation_status(server \\ __MODULE__, key) do
    GenServer.call(server, {:operation_status, key})
  end

  @doc "Agenda `fun` para execução periódica a cada `interval_ms` milissegundos."
  def schedule_periodic(server \\ __MODULE__, name, interval_ms, fun)
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    GenServer.call(server, {:schedule_periodic, name, interval_ms, fun})
  end

  @doc "Cancela uma tarefa periódica."
  def cancel_periodic(server \\ __MODULE__, name) do
    GenServer.call(server, {:cancel_periodic, name})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      orders: %{},
      ops: %{},
      monitors: %{},
      periodic: %{},
      next_id: 1,
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
      base_backoff_ms: Keyword.get(opts, :base_backoff_ms, @default_backoff_ms),
      idempotency_ttl_ms: Keyword.get(opts, :idempotency_ttl_ms, @default_idempotency_ttl),
      order_ttl_ms: Keyword.get(opts, :order_ttl_ms, @default_order_ttl),
      sweep_interval_ms: Keyword.get(opts, :sweep_interval_ms, @default_sweep_interval)
    }

    Process.send_after(self(), :sweep, state.sweep_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:create_order, attrs}, _from, state) do
    now = now_ms()
    id = state.next_id

    order = %{
      id: id,
      attrs: attrs,
      status: :pending,
      created_at: now,
      expires_at: now + state.order_ttl_ms
    }

    new_state = %{state | orders: Map.put(state.orders, id, order), next_id: id + 1}
    {:reply, {:ok, order}, new_state}
  end

  def handle_call({:get_order, id}, _from, state) do
    reply =
      case Map.get(state.orders, id) do
        nil -> :not_found
        order -> order
      end

    {:reply, reply, state}
  end

  def handle_call({:execute, key, fun}, from, state) do
    # Operação já conhecida: é reaproveitada (idempotência), sem checar pré-condições.
    case Map.fetch(state.ops, key) do
      {:ok, _op} ->
        run_or_join(state, key, fun, from)

      :error ->
        case precheck(state, key) do
          :ok -> run_or_join(state, key, fun, from)
          {:error, _reason} = error -> {:reply, error, state}
        end
    end
  end

  def handle_call(:running_operations, _from, state) do
    list = for {_key, %{status: :running} = op} <- state.ops, do: summary(op)
    {:reply, list, state}
  end

  def handle_call({:operation_status, key}, _from, state) do
    reply =
      case Map.get(state.ops, key) do
        nil -> :not_found
        op -> summary(op)
      end

    {:reply, reply, state}
  end

  def handle_call({:schedule_periodic, name, interval, fun}, _from, state) do
    if Map.has_key?(state.periodic, name) do
      {:reply, {:error, :already_scheduled}, state}
    else
      timer = Process.send_after(self(), {:tick, name}, interval)
      job = %{interval: interval, fun: fun, tick: 1, timer: timer}
      {:reply, :ok, %{state | periodic: Map.put(state.periodic, name, job)}}
    end
  end

  def handle_call({:cancel_periodic, name}, _from, state) do
    case Map.pop(state.periodic, name) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {job, periodic} ->
        if job.timer, do: Process.cancel_timer(job.timer)
        {:reply, :ok, %{state | periodic: periodic}}
    end
  end

  @impl true
  # Tentativa atual reportada pelo worker (atualiza o estado consultável).
  def handle_info({:op_attempt, key, n}, state) do
    state =
      case Map.get(state.ops, key) do
        %{status: :running} = op -> put_op(state, %{op | attempt: n})
        _ -> state
      end

    {:noreply, state}
  end

  # Resultado final reportado pelo worker.
  def handle_info({:op_finished, key, outcome}, state) do
    {:noreply, finish_op(state, key, outcome)}
  end

  # Worker terminou de forma inesperada (crash).
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.monitors, ref) do
      {:ok, key} -> {:noreply, finish_op(state, key, {:error, {:crashed, reason}})}
      :error -> {:noreply, state}
    end
  end

  # Disparo de tarefa periódica.
  def handle_info({:tick, name}, state) do
    case Map.get(state.periodic, name) do
      nil ->
        {:noreply, state}

      job ->
        key = {:periodic, name, job.tick}
        job = %{job | tick: job.tick + 1, timer: nil}
        state = %{state | periodic: Map.put(state.periodic, name, job)}
        {:noreply, start_op(state, key, job.fun, [])}
    end
  end

  # Expiração automática de registros.
  def handle_info(:sweep, state) do
    now = now_ms()

    orders =
      Map.reject(state.orders, fn {_id, order} ->
        order.status == :pending and order.expires_at <= now
      end)

    ops =
      Map.reject(state.ops, fn {_key, op} ->
        op.status != :running and op.expires_at <= now
      end)

    Process.send_after(self(), :sweep, state.sweep_interval_ms)
    {:noreply, %{state | orders: orders, ops: ops}}
  end

  # ---------------------------------------------------------------------------
  # Helpers privados
  # ---------------------------------------------------------------------------

  defp now_ms, do: System.monotonic_time(:millisecond)

  # Verifica pré-condições de negócio antes de iniciar uma operação nova.
  defp precheck(state, {:pay, order_id}) do
    case Map.get(state.orders, order_id) do
      nil -> {:error, :order_not_found}
      %{status: :pending} -> :ok
      %{status: status} -> {:error, {:invalid_status, status}}
    end
  end

  defp precheck(_state, _key), do: :ok

  # Decide entre: aguardar operação em andamento, devolver resultado armazenado
  # ou iniciar nova execução.
  defp run_or_join(state, key, fun, from) do
    case Map.get(state.ops, key) do
      nil ->
        {:noreply, start_op(state, key, fun, [from])}

      %{status: :running} = op ->
        {:noreply, put_op(state, %{op | waiters: [from | op.waiters]})}

      %{outcome: outcome} ->
        {:reply, outcome, state}
    end
  end

  defp start_op(state, key, fun, waiters) do
    server = self()

    {pid, ref} =
      spawn_monitor(fn ->
        worker(server, key, fun, state.max_attempts, state.base_backoff_ms)
      end)

    op = %{
      key: key,
      status: :running,
      attempt: 0,
      outcome: nil,
      waiters: waiters,
      started_at: now_ms(),
      finished_at: nil,
      expires_at: nil,
      pid: pid,
      ref: ref
    }

    %{state | ops: Map.put(state.ops, key, op), monitors: Map.put(state.monitors, ref, key)}
  end

  defp finish_op(state, key, outcome) do
    case Map.get(state.ops, key) do
      %{status: :running} = op ->
        Process.demonitor(op.ref, [:flush])

        status = if match?({:ok, _}, outcome), do: :succeeded, else: :failed
        now = now_ms()

        done = %{
          op
          | status: status,
            outcome: outcome,
            waiters: [],
            pid: nil,
            ref: nil,
            finished_at: now,
            expires_at: now + state.idempotency_ttl_ms
        }

        Enum.each(op.waiters, &GenServer.reply(&1, outcome))

        state = %{
          state
          | ops: Map.put(state.ops, key, done),
            monitors: Map.delete(state.monitors, op.ref)
        }

        state
        |> apply_side_effects(key, outcome)
        |> maybe_reschedule(key)

      _ ->
        state
    end
  end

  # Efeitos colaterais no domínio após uma operação concluída com sucesso.
  defp apply_side_effects(state, {:pay, order_id}, {:ok, _result}) do
    case Map.fetch(state.orders, order_id) do
      {:ok, order} ->
        %{state | orders: Map.put(state.orders, order_id, %{order | status: :paid})}

      :error ->
        state
    end
  end

  defp apply_side_effects(state, _key, _outcome), do: state

  # Após concluir um disparo periódico, agenda o próximo (evita sobreposição).
  defp maybe_reschedule(state, {:periodic, name, _tick}) do
    case Map.get(state.periodic, name) do
      nil ->
        state

      job ->
        timer = Process.send_after(self(), {:tick, name}, job.interval)
        %{state | periodic: Map.put(state.periodic, name, %{job | timer: timer})}
    end
  end

  defp maybe_reschedule(state, _key), do: state

  defp put_op(state, op), do: %{state | ops: Map.put(state.ops, op.key, op)}

  defp summary(op), do: Map.take(op, [:key, :status, :attempt, :started_at, :finished_at, :outcome])

  # Processo worker: executa a função com retentativas e reporta ao servidor.
  defp worker(server, key, fun, max_attempts, base_backoff) do
    outcome = attempt(server, key, fun, 1, max_attempts, base_backoff)
    send(server, {:op_finished, key, outcome})
  end

  defp attempt(server, key, fun, n, max_attempts, base_backoff) do
    send(server, {:op_attempt, key, n})

    case run_fun(fun) do
      {:ok, value} ->
        {:ok, value}

      # Falha temporária com tentativas restantes: backoff exponencial e nova tentativa.
      {:error, :temporary, _reason} when n < max_attempts ->
        Process.sleep(base_backoff * Integer.pow(2, n - 1))
        attempt(server, key, fun, n + 1, max_attempts, base_backoff)

      {:error, :temporary, reason} ->
        {:error, {:retries_exhausted, reason}}

      {:error, :permanent, reason} ->
        {:error, reason}

      other ->
        {:error, {:invalid_return, other}}
    end
  end

  defp run_fun(fun) do
    fun.()
  rescue
    e -> {:error, :permanent, {:exception, Exception.message(e)}}
  end
end
