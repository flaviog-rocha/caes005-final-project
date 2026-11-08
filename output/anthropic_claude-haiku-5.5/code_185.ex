# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir comunicação entre diferentes processos.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciador de transações financeiras em um único arquivo.
#
# Requisitos atendidos:
#   1. Consulta do estado atual das operações (status/1, list/1, await/2).
#   2. Execução periódica de operações (schedule/4, cancel_schedule/1).
#   3. Comunicação entre processos (eventos via Registry em modo :duplicate,
#      e respostas síncronas via await/2).
#   4. Idempotência: uma operação com o mesmo id nunca é executada duas vezes
#      (enquanto estiver registrada no gerenciador).
#   5. Novas tentativas com backoff exponencial para falhas temporárias
#      ({:error, :temporary}, timeouts e falhas/crashes do processo da operação).
#
# Exemplo de uso:
#
#   {:ok, _} = TxSystem.start_link()
#   :ok = TxSystem.subscribe()
#
#   TxSystem.submit("pagamento-42", fn ->
#     # chamada externa que pode falhar temporariamente
#     {:ok, :pago}
#   end)
#
#   {:ok, _op} = TxSystem.status("pagamento-42")
#   {:ok, :pago} = TxSystem.await("pagamento-42", 10_000)
#
#   :ok = TxSystem.schedule(:conciliacao, 60_000, fn -> {:ok, :conciliado} end)
#
#   # Mensagens recebidas pelo processo assinante:
#   # {:tx_event, {:started, id, tentativa}}
#   # {:tx_event, {:retry_scheduled, id, tentativa, atraso_ms, motivo}}
#   # {:tx_event, {:completed, id, resultado}} | {:tx_event, {:failed, id, resultado}}
#   # {:tx_event, {:job_tick, nome, execucao}}

defmodule TxSystem do
  @moduledoc """
  Supervisor e API pública do sistema de gerenciamento de transações.
  """
  use Supervisor

  alias TxSystem.Manager

  @registry TxSystem.Registry
  @task_sup TxSystem.TaskSupervisor

  @doc "Inicia o sistema. Opções: :max_attempts, :base_backoff, :max_backoff, :timeout."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registry em modo :duplicate permite que vários processos assinem eventos.
      {Registry, keys: :duplicate, name: @registry},
      # Supervisor de tarefas que executam as operações de forma isolada.
      {Task.Supervisor, name: @task_sup},
      {Manager, Keyword.put(opts, :task_sup, @task_sup)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Submete uma operação com identificador único.
  Retorna `{:accepted, id}` ou `{:duplicate, op}` se a operação já existir
  (nesse caso a função NÃO é executada novamente).
  """
  def submit(id, fun, opts \\ []) when is_function(fun, 0) do
    Manager.submit(id, fun, opts)
  end

  @doc "Aguarda o resultado final de uma operação (bloqueia até o timeout)."
  def await(id, timeout \\ 5_000), do: Manager.await(id, timeout)

  @doc "Consulta o estado atual de uma operação."
  def status(id), do: Manager.status(id)

  @doc "Lista operações, opcionalmente filtradas por status (:running, :retrying, :completed, :failed)."
  def list(status_filter \\ nil), do: Manager.list(status_filter)

  @doc "Agenda uma operação recorrente a cada `interval` milissegundos."
  def schedule(name, interval, fun, opts \\ []) when is_function(fun, 0) and is_integer(interval) do
    Manager.schedule(name, interval, fun, opts)
  end

  @doc "Cancela uma operação recorrente agendada."
  def cancel_schedule(name), do: Manager.cancel_schedule(name)

  @doc "Assina os eventos do sistema no processo chamador."
  def subscribe do
    with {:ok, _} <- Registry.register(@registry, :events, nil), do: :ok
  end

  @doc "Cancela a assinatura de eventos do processo chamador."
  def unsubscribe, do: Registry.unregister(@registry, :events)
end

defmodule TxSystem.Manager do
  @moduledoc false
  # Processo central: mantém o estado das operações, controla tentativas,
  # timeouts, retries, agendamentos e notificações.

  use GenServer

  @registry TxSystem.Registry
  @default_max_attempts 4
  @default_base_backoff 200
  @default_max_backoff 5_000
  @default_timeout 5_000

  # ---------------------------------------------------------------------------
  # API
  # ---------------------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def submit(id, fun, opts), do: GenServer.call(__MODULE__, {:submit, id, fun, opts})
  def await(id, timeout), do: GenServer.call(__MODULE__, {:await, id}, timeout)
  def status(id), do: GenServer.call(__MODULE__, {:status, id})
  def list(filter), do: GenServer.call(__MODULE__, {:list, filter})

  def schedule(name, interval, fun, opts),
    do: GenServer.call(__MODULE__, {:schedule, name, interval, fun, opts})

  def cancel_schedule(name), do: GenServer.call(__MODULE__, {:cancel_schedule, name})

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      task_sup: Keyword.fetch!(opts, :task_sup),
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts),
      base_backoff: Keyword.get(opts, :base_backoff, @default_base_backoff),
      max_backoff: Keyword.get(opts, :max_backoff, @default_max_backoff),
      timeout: Keyword.get(opts, :timeout, @default_timeout),
      # id => operação
      ops: %{},
      # ref da task => {id, timer_ref, %Task{}}
      tasks: %{},
      # nome => job recorrente
      jobs: %{}
    }

    {:ok, state}
  end

  # ---------------------------------------------------------------------------
  # Chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:submit, id, fun, opts}, _from, state) do
    case Map.fetch(state.ops, id) do
      # Idempotência: a operação já existe, não é executada de novo.
      {:ok, op} -> {:reply, {:duplicate, public(op)}, state}
      :error -> {:reply, {:accepted, id}, do_submit(state, id, fun, opts)}
    end
  end

  def handle_call({:await, id}, from, state) do
    case Map.fetch(state.ops, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: status, result: result}} when status in [:completed, :failed] ->
        {:reply, result, state}

      {:ok, op} ->
        {:noreply, put_op(state, %{op | waiters: [from | op.waiters]})}
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.ops, id) do
        {:ok, op} -> {:ok, public(op)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list, filter}, _from, state) do
    ops =
      state.ops
      |> Map.values()
      |> Enum.filter(fn op -> is_nil(filter) or op.status == filter end)
      |> Enum.map(&public/1)

    {:reply, ops, state}
  end

  def handle_call({:schedule, name, interval, fun, opts}, _from, state) do
    if Map.has_key?(state.jobs, name) do
      {:reply, {:error, :already_scheduled}, state}
    else
      # `epoch` diferencia execuções de agendamentos com o mesmo nome ao longo do tempo,
      # evitando colisão de ids de idempotência após cancelamento e novo agendamento.
      job = %{
        interval: interval,
        fun: fun,
        opts: opts,
        runs: 0,
        epoch: make_ref(),
        timer: Process.send_after(self(), {:tick, name}, interval)
      }

      {:reply, :ok, %{state | jobs: Map.put(state.jobs, name, job)}}
    end
  end

  def handle_call({:cancel_schedule, name}, _from, state) do
    case Map.pop(state.jobs, name) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {job, jobs} ->
        Process.cancel_timer(job.timer)
        {:reply, :ok, %{state | jobs: jobs}}
    end
  end

  # ---------------------------------------------------------------------------
  # Mensagens assíncronas (resultados de tasks, timeouts, retries, ticks)
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {{id, timer, _task}, tasks} ->
        Process.demonitor(ref, [:flush])
        Process.cancel_timer(timer)

        state =
          %{state | tasks: tasks}
          |> handle_result(id, result)

        {:noreply, state}
    end
  end

  # Processo da operação terminou de forma anormal (crash): falha temporária.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {{id, timer, _task}, tasks} ->
        Process.cancel_timer(timer)
        {:noreply, retry_or_fail(%{state | tasks: tasks}, id, {:crashed, reason})}
    end
  end

  # Timeout de tentativa: encerra a task e tenta novamente (se houver tentativas).
  def handle_info({:timeout, ref}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {{id, _timer, task}, tasks} ->
        Task.shutdown(task, :brutal_kill)
        {:noreply, retry_or_fail(%{state | tasks: tasks}, id, :timeout)}
    end
  end

  # Nova tentativa após backoff.
  def handle_info({:retry, id}, state) do
    case Map.get(state.ops, id) do
      %{status: :retrying} -> {:noreply, start_attempt(state, id)}
      _ -> {:noreply, state}
    end
  end

  # Execução periódica: cada execução é uma operação com id único.
  def handle_info({:tick, name}, state) do
    case Map.fetch(state.jobs, name) do
      :error ->
        {:noreply, state}

      {:ok, job} ->
        run = job.runs + 1
        op_id = {:job, name, job.epoch, run}
        timer = Process.send_after(self(), {:tick, name}, job.interval)
        job = %{job | runs: run, timer: timer}

        state =
          %{state | jobs: Map.put(state.jobs, name, job)}
          |> do_submit(op_id, job.fun, job.opts)
          |> notify({:job_tick, name, run})

        {:noreply, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Lógica interna
  # ---------------------------------------------------------------------------

  defp do_submit(state, id, fun, opts) do
    op = %{
      id: id,
      fun: fun,
      status: :pending,
      attempts: 0,
      max_attempts: Keyword.get(opts, :max_attempts, state.max_attempts),
      timeout: Keyword.get(opts, :timeout, state.timeout),
      result: nil,
      last_error: nil,
      waiters: [],
      submitted_at: System.monotonic_time(:millisecond)
    }

    state
    |> put_op(op)
    |> start_attempt(id)
  end

  defp start_attempt(state, id) do
    op = Map.fetch!(state.ops, id)

    task = Task.Supervisor.async_nolink(state.task_sup, op.fun)
    timer = Process.send_after(self(), {:timeout, task.ref}, op.timeout)
    op = %{op | status: :running, attempts: op.attempts + 1}

    state
    |> put_op(op)
    |> Map.update!(:tasks, &Map.put(&1, task.ref, {id, timer, task}))
    |> notify({:started, id, op.attempts})
  end

  # Resultado de uma tentativa.
  defp handle_result(state, id, {:ok, _} = result), do: finish(state, id, :completed, result)
  defp handle_result(state, id, {:error, :temporary}), do: retry_or_fail(state, id, :temporary)
  defp handle_result(state, id, {:error, _} = result), do: finish(state, id, :failed, result)

  defp handle_result(state, id, other),
    do: finish(state, id, :failed, {:error, {:invalid_return, other}})

  defp retry_or_fail(state, id, reason) do
    op = Map.fetch!(state.ops, id)

    if op.attempts < op.max_attempts do
      delay = backoff(state, op.attempts)
      Process.send_after(self(), {:retry, id}, delay)

      state
      |> put_op(%{op | status: :retrying, last_error: reason})
      |> notify({:retry_scheduled, id, op.attempts, delay, reason})
    else
      finish(state, id, :failed, {:error, reason})
    end
  end

  # Backoff exponencial limitado: base * 2^(tentativa-1), até max_backoff.
  defp backoff(state, attempt) do
    min(state.base_backoff * Integer.pow(2, attempt - 1), state.max_backoff)
  end

  defp finish(state, id, status, result) do
    op = Map.fetch!(state.ops, id)

    # Comunicação síncrua: responde a todos que aguardavam o resultado.
    Enum.each(op.waiters, &GenServer.reply(&1, result))

    op = %{op | status: status, result: result, waiters: []}

    state
    |> put_op(op)
    |> notify({status, id, result})
  end

  defp put_op(state, op), do: %{state | ops: Map.put(state.ops, op.id, op)}

  # Remove campos internos (função e waiters) antes de expor o estado.
  defp public(op), do: Map.drop(op, [:fun, :waiters])

  # Publica evento para todos os processos assinantes via Registry.
  defp notify(state, event) do
    Registry.dispatch(@registry, :events, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:tx_event, event})
    end)

    state
  end
end
