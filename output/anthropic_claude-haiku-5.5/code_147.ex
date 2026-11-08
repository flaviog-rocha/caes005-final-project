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

# Gerenciador de pedidos de e-commerce em Elixir, usando apenas a biblioteca padrão.
#
# Requisitos atendidos:
# 1. Novas tentativas após falhas temporárias: backoff exponencial com jitter e limite de tentativas.
# 2. Expiração automática de registros: pedidos não pagos e operações concluídas expiram e são removidos pela varredura periódica.
# 3. Execução periódica de operações: jobs recorrentes com intervalo fixo, sem sobreposição de execuções.
# 4. Consulta do estado das operações em andamento: status, tentativas, último erro e resultados.
# 5. Impedir execução duplicada: cada operação é identificada por uma chave de idempotência que só pode ser submetida uma vez.

defmodule Ecommerce.OrderManager do
  @moduledoc """
  Gerenciador de pedidos com tentativas automáticas, expiração de registros,
  jobs periódicos, consulta de estado e idempotência de operações.

  Contrato das funções de operação (arity 0) submetidas via `submit_operation/4`:

    * `{:ok, resultado}`                 - sucesso
    * `{:error, :transient, motivo}`     - falha temporária (será re-tentada)
    * `{:error, :permanent, motivo}`     - falha definitiva (não será re-tentada)

  Qualquer outro retorno é tratado como falha permanente. Exceções lançadas
  pela função são tratadas como falhas temporárias.
  """

  use GenServer
  require Logger

  @max_attempts 3
  @base_backoff_ms 200
  @max_backoff_ms 10_000
  @sweep_interval_ms 1_000
  @order_ttl_ms 15 * 60 * 1_000
  @operation_retention_ms 60 * 60 * 1_000

  ## API pública

  @doc "Inicia o processo. Aceita `:name`, `:max_attempts` e `:order_ttl_ms`."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Cria um pedido aguardando pagamento, que expira automaticamente após o TTL."
  def create_order(server, order_id, attrs \\ %{}) do
    GenServer.call(server, {:create_order, order_id, attrs})
  end

  @doc "Consulta um pedido pelo id."
  def get_order(server, order_id) do
    GenServer.call(server, {:get_order, order_id})
  end

  @doc "Marca um pedido aguardando pagamento como pago, removendo sua expiração."
  def mark_order_paid(server, order_id) do
    GenServer.call(server, {:mark_order_paid, order_id})
  end

  @doc """
  Submete uma operação identificada por `key` (chave de idempotência).
  Se a chave já tiver sido submetida (em execução, re-tentando, concluída ou
  falha), a submissão é rejeitada e a operação não é executada novamente.

  Opções: `:max_attempts`.
  """
  def submit_operation(server, key, fun, opts \\ []) do
    GenServer.call(server, {:submit_operation, key, fun, opts})
  end

  @doc "Consulta o estado atual de uma operação pela chave."
  def operation_status(server, key) do
    GenServer.call(server, {:operation_status, key})
  end

  @doc "Lista as operações atualmente em execução ou aguardando nova tentativa."
  def running_operations(server) do
    GenServer.call(server, :running_operations)
  end

  @doc """
  Agenda uma função (arity 0) para execução periódica a cada `interval_ms`.
  A próxima execução só é agendada após a conclusão da anterior, evitando sobreposição.
  """
  def schedule_periodic(server, name, interval_ms, fun) do
    GenServer.call(server, {:schedule_periodic, name, interval_ms, fun})
  end

  @doc "Cancela um job periódico."
  def cancel_periodic(server, name) do
    GenServer.call(server, {:cancel_periodic, name})
  end

  @doc "Lista o estado dos jobs periódicos."
  def periodic_jobs(server) do
    GenServer.call(server, :periodic_jobs)
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    state = %{
      orders: %{},
      operations: %{},
      # mapa de referência da tarefa => dono ({:operation, key} | {:periodic, name})
      refs: %{},
      periodic: %{},
      default_max_attempts: Keyword.get(opts, :max_attempts, @max_attempts),
      order_ttl_ms: Keyword.get(opts, :order_ttl_ms, @order_ttl_ms)
    }

    schedule_sweep()
    {:ok, state}
  end

  @impl true
  def handle_call({:create_order, order_id, attrs}, _from, state) do
    if Map.has_key?(state.orders, order_id) do
      {:reply, {:error, :duplicate_order}, state}
    else
      order = %{
        id: order_id,
        attrs: attrs,
        status: :awaiting_payment,
        expires_at: now() + state.order_ttl_ms
      }

      {:reply, {:ok, order}, %{state | orders: Map.put(state.orders, order_id, order)}}
    end
  end

  def handle_call({:get_order, order_id}, _from, state) do
    {:reply, fetch(state.orders, order_id), state}
  end

  def handle_call({:mark_order_paid, order_id}, _from, state) do
    case Map.fetch(state.orders, order_id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: :awaiting_payment} = order} ->
        order = %{order | status: :paid, expires_at: nil}
        {:reply, {:ok, order}, %{state | orders: Map.put(state.orders, order_id, order)}}

      {:ok, order} ->
        {:reply, {:error, {:invalid_status, order.status}}, state}
    end
  end

  def handle_call({:submit_operation, key, fun, opts}, _from, state) when is_function(fun, 0) do
    case Map.fetch(state.operations, key) do
      # Impede execução duplicada (idempotência)
      {:ok, op} ->
        {:reply, {:error, {:already_submitted, op.status}}, state}

      :error ->
        op = %{
          key: key,
          fun: fun,
          status: :running,
          attempts: 0,
          max_attempts: Keyword.get(opts, :max_attempts, state.default_max_attempts),
          last_error: nil,
          result: nil,
          submitted_at: now(),
          updated_at: now(),
          expires_at: nil
        }

        state =
          %{state | operations: Map.put(state.operations, key, op)}
          |> start_attempt(key)

        {:reply, :ok, state}
    end
  end

  def handle_call({:operation_status, key}, _from, state) do
    case Map.fetch(state.operations, key) do
      {:ok, op} -> {:reply, {:ok, summarize(op)}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:running_operations, _from, state) do
    ops =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status in [:running, :retrying]))
      |> Enum.map(&summarize/1)

    {:reply, ops, state}
  end

  def handle_call({:schedule_periodic, name, interval_ms, fun}, _from, state)
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 0) do
    if Map.has_key?(state.periodic, name) do
      {:reply, {:error, :already_scheduled}, state}
    else
      job = %{
        interval_ms: interval_ms,
        fun: fun,
        status: :waiting,
        runs: 0,
        last_result: nil
      }

      Process.send_after(self(), {:tick, name}, interval_ms)
      {:reply, :ok, %{state | periodic: Map.put(state.periodic, name, job)}}
    end
  end

  def handle_call({:cancel_periodic, name}, _from, state) do
    if Map.has_key?(state.periodic, name) do
      {:reply, :ok, %{state | periodic: Map.delete(state.periodic, name)}}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:periodic_jobs, _from, state) do
    jobs =
      Enum.map(state.periodic, fn {name, job} ->
        job
        |> Map.take([:interval_ms, :status, :runs, :last_result])
        |> Map.put(:name, name)
      end)

    {:reply, jobs, state}
  end

  # Resultado de uma tarefa assíncrona (Task.async_nolink)
  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    # Remove a monitoração e a mensagem :DOWN correspondente
    Process.demonitor(ref, [:flush])

    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {owner, refs} ->
        {:noreply, handle_task_result(%{state | refs: refs}, owner, result)}
    end
  end

  # Tarefa terminou de forma anormal (crash)
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {owner, refs} ->
        failure = {:error, :transient, {:crashed, reason}}
        {:noreply, handle_task_result(%{state | refs: refs}, owner, failure)}
    end
  end

  # Nova tentativa de uma operação
  def handle_info({:retry, key}, state) do
    case Map.fetch(state.operations, key) do
      {:ok, %{status: :retrying}} -> {:noreply, start_attempt(state, key)}
      _ -> {:noreply, state}
    end
  end

  # Disparo de job periódico
  def handle_info({:tick, name}, state) do
    case Map.fetch(state.periodic, name) do
      {:ok, %{status: :waiting}} -> {:noreply, start_periodic_run(state, name)}
      _ -> {:noreply, state}
    end
  end

  # Varredura de expiração
  def handle_info(:sweep, state) do
    now = now()

    orders =
      Map.reject(state.orders, fn {_id, order} ->
        order.status == :awaiting_payment and order.expires_at <= now
      end)

    operations =
      Map.reject(state.operations, fn {_key, op} ->
        op.status in [:completed, :failed] and op.expires_at <= now
      end)

    schedule_sweep()
    {:noreply, %{state | orders: orders, operations: operations}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Execução de operações

  defp start_attempt(state, key) do
    op = Map.fetch!(state.operations, key)
    task = Task.async_nolink(op.fun)

    op = %{op | status: :running, attempts: op.attempts + 1, updated_at: now()}

    %{
      state
      | operations: Map.put(state.operations, key, op),
        refs: Map.put(state.refs, task.ref, {:operation, key})
    }
  end

  defp handle_task_result(state, {:operation, key}, result) do
    op = Map.fetch!(state.operations, key)
    attempts = op.attempts
    max = op.max_attempts

    case normalize(result) do
      {:ok, value} ->
        finish_op(state, key, :completed, %{result: value, last_error: nil})

      {:error, :permanent, reason} ->
        finish_op(state, key, :failed, %{last_error: reason})

      {:error, :transient, reason} when attempts < max ->
        schedule_retry(state, key, attempts, reason)

      {:error, :transient, reason} ->
        finish_op(state, key, :failed, %{last_error: reason})
    end
  end

  defp handle_task_result(state, {:periodic, name}, result) do
    case Map.fetch(state.periodic, name) do
      :error ->
        state

      {:ok, job} ->
        result = normalize(result)

        if match?({:error, _, _}, result) do
          Logger.warning("Job periódico #{inspect(name)} falhou: #{inspect(result)}")
        end

        job = %{job | status: :waiting, runs: job.runs + 1, last_result: result}
        Process.send_after(self(), {:tick, name}, job.interval_ms)
        %{state | periodic: Map.put(state.periodic, name, job)}
    end
  end

  defp start_periodic_run(state, name) do
    job = Map.fetch!(state.periodic, name)
    task = Task.async_nolink(job.fun)

    %{
      state
      | periodic: Map.put(state.periodic, name, %{job | status: :running}),
        refs: Map.put(state.refs, task.ref, {:periodic, name})
    }
  end

  defp schedule_retry(state, key, attempts, reason) do
    Process.send_after(self(), {:retry, key}, backoff(attempts))
    update_op(state, key, &%{&1 | status: :retrying, last_error: reason, updated_at: now()})
  end

  defp finish_op(state, key, status, changes) do
    update_op(state, key, fn op ->
      changes =
        Map.merge(changes, %{
          status: status,
          updated_at: now(),
          expires_at: now() + @operation_retention_ms
        })

      Map.merge(op, changes)
    end)
  end

  defp update_op(state, key, fun) do
    %{state | operations: Map.update!(state.operations, key, fun)}
  end

  ## Utilitários

  # Backoff exponencial com teto e jitter aleatório
  defp backoff(attempts) do
    exp = min(@max_backoff_ms, @base_backoff_ms * Integer.pow(2, attempts - 1))
    exp + :rand.uniform(@base_backoff_ms)
  end

  defp normalize({:ok, _} = ok), do: ok
  defp normalize({:error, kind, _} = err) when kind in [:transient, :permanent], do: err
  defp normalize(other), do: {:error, :permanent, {:invalid_return, other}}

  defp summarize(op) do
    Map.take(op, [
      :key,
      :status,
      :attempts,
      :max_attempts,
      :last_error,
      :result,
      :submitted_at,
      :updated_at
    ])
  end

  defp fetch(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  defp now, do: System.monotonic_time(:millisecond)
end

# Exemplo de uso:
#
#   {:ok, _pid} = Ecommerce.OrderManager.start_link()
#
#   {:ok, _order} = Ecommerce.OrderManager.create_order(Ecommerce.OrderManager, "pedido-1", %{total: 100})
#   :ok = Ecommerce.OrderManager.mark_order_paid(Ecommerce.OrderManager, "pedido-1")
#
#   gateway = fn ->
#     if :rand.uniform() < 0.5,
#       do: {:ok, :cobrado},
#       else: {:error, :transient, :timeout}
#   end
#
#   :ok = Ecommerce.OrderManager.submit_operation(Ecommerce.OrderManager, {:charge, "pedido-1"}, gateway)
#   {:error, {:already_submitted, _}} =
#     Ecommerce.OrderManager.submit_operation(Ecommerce.OrderManager, {:charge, "pedido-1"}, gateway)
#
#   Ecommerce.OrderManager.operation_status(Ecommerce.OrderManager, {:charge, "pedido-1"})
#   Ecommerce.OrderManager.running_operations(Ecommerce.OrderManager)
#
#   :ok = Ecommerce.OrderManager.schedule_periodic(
#     Ecommerce.OrderManager,
#     :relatorio,
#     60_000,
#     fn -> IO.puts("Gerando relatório de pedidos") ; {:ok, :gerado} end
#   )
