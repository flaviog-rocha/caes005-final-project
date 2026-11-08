# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Detectar encerramentos inesperados de processos.
# 3. Manter um histórico das operações realizadas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas concorrente e tolerante a falhas, implementada com GenServer
  e apenas recursos da biblioteca padrão.

  Garantias:
    * Consistência: todo estado é mutado somente dentro do GenServer, que processa
      mensagens sequencialmente. Os workers nunca tocam o estado. Resultados de
      workers desconhecidos ou já finalizados são descartados.
    * Detecção de encerramentos inesperados: cada worker é monitorado; saídas
      não previstas (exceções, :kill, timeout) geram um evento no histórico.
    * Histórico: todas as operações relevantes são registradas (limitado em tamanho).
    * Novas tentativas: falhas transitórias (ou crashes) são reagendadas com
      backoff exponencial até `max_attempts`.
    * Operações periódicas: a cada intervalo, tarefas excedendo o timeout são
      encerradas e um snapshot de estatísticas é registrado.

  Contrato do executor (função de aridade 1 recebendo o payload):
    * :ok
    * {:error, :transient, motivo}  -> reagendado
    * {:error, :permanent, motivo}  -> falha definitiva
    * Qualquer outra coisa é tratada como resultado inesperado (falha definitiva).
    * Exceções/saídas do processo são detectadas como crash e tratadas como transitórias.

  Exemplo:
      {:ok, _pid} = TaskQueue.start_link(executor: &MyJob.run/1, max_workers: 4)
      {:ok, _id} = TaskQueue.enqueue(%{action: :send_email, to: "a@b.c"})
      TaskQueue.stats()
      TaskQueue.history(20)
  """

  use GenServer

  @default_max_workers 4
  @default_max_attempts 3
  @base_backoff_ms 200
  @task_timeout_ms 30_000
  @periodic_interval_ms 5_000
  @history_limit 500

  # ============================================================
  # API pública
  # ============================================================

  @doc "Inicia a fila. Opções: :name, :executor, :max_workers, :max_attempts."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Enfileira um payload e retorna {:ok, task_id}."
  def enqueue(server \\ __MODULE__, payload) do
    GenServer.call(server, {:enqueue, payload})
  end

  @doc "Retorna as últimas `limit` entradas do histórico (mais recentes primeiro)."
  def history(server \\ __MODULE__, limit \\ 50) do
    GenServer.call(server, {:history, limit})
  end

  @doc "Retorna estatísticas atuais da fila."
  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # ============================================================
  # Callbacks do GenServer
  # ============================================================

  @impl true
  def init(opts) do
    state = %{
      queue: :queue.new(),
      # task_id => %{pid, ref, task, started_at}
      running: %{},
      history: [],
      executor: Keyword.get(opts, :executor, &default_executor/1),
      max_workers: Keyword.get(opts, :max_workers, @default_max_workers),
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts)
    }

    schedule_periodic()
    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, payload}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])
    task = %{id: id, payload: payload, attempts: 0}

    state =
      %{state | queue: :queue.in(task, state.queue)}
      |> log(:enqueued, id, nil)
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:history, limit}, _from, state) do
    {:reply, Enum.take(state.history, limit), state}
  end

  def handle_call(:stats, _from, state) do
    reply = %{
      queued: :queue.len(state.queue),
      running: map_size(state.running),
      history_size: length(state.history),
      max_workers: state.max_workers
    }

    {:reply, reply, state}
  end

  @impl true
  # Resultado enviado pelo worker antes de encerrar normalmente.
  def handle_info({:task_result, id, result}, state) do
    case Map.pop(state.running, id) do
      {nil, _running} ->
        # Resultado tardio/desconhecido: descartado para preservar consistência.
        {:noreply, state}

      {entry, running} ->
        Process.demonitor(entry.ref, [:flush])

        state =
          %{state | running: running}
          |> complete(entry.task, result)
          |> dispatch()

        {:noreply, state}
    end
  end

  # Encerramento do worker: resultado já tratado ou saída inesperada.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case find_by_ref(state.running, ref) do
      nil ->
        {:noreply, state}

      {id, entry} ->
        state =
          %{state | running: Map.delete(state.running, id)}
          |> log(:crashed, id, %{reason: reason, attempt: entry.task.attempts})
          |> complete(entry.task, {:error, :transient, {:crash, reason}})
          |> dispatch()

        {:noreply, state}
    end
  end

  # Reagendamento após backoff.
  def handle_info({:requeue, task}, state) do
    state =
      %{state | queue: :queue.in(task, state.queue)}
      |> log(:requeued, task.id, %{attempt: task.attempts + 1})
      |> dispatch()

    {:noreply, state}
  end

  # Operação periódica: timeouts + snapshot.
  def handle_info(:periodic, state) do
    now = now()

    timed_out =
      for {id, entry} <- state.running,
          now - entry.started_at > @task_timeout_ms,
          do: {id, entry}

    state =
      Enum.reduce(timed_out, state, fn {id, entry}, acc ->
        # O DOWN subsequente (reason :killed) será tratado como crash e reagendado.
        Process.exit(entry.pid, :kill)
        log(acc, :timeout, id, %{elapsed_ms: now - entry.started_at})
      end)

    state =
      state
      |> log(:snapshot, nil, %{
        queued: :queue.len(state.queue),
        running: map_size(state.running)
      })
      |> dispatch()

    schedule_periodic()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ============================================================
  # Despacho e execução
  # ============================================================

  defp dispatch(state) do
    cond do
      map_size(state.running) >= state.max_workers ->
        state

      :queue.is_empty(state.queue) ->
        state

      true ->
        {{:value, task}, rest} = :queue.out(state.queue)
        task = %{task | attempts: task.attempts + 1}
        parent = self()
        executor = state.executor

        # O worker apenas executa e reporta. Sem estado compartilhado.
        {pid, ref} =
          spawn_monitor(fn ->
            result = executor.(task.payload)
            send(parent, {:task_result, task.id, result})
          end)

        entry = %{pid: pid, ref: ref, task: task, started_at: now()}

        %{state | queue: rest, running: Map.put(state.running, task.id, entry)}
        |> log(:started, task.id, %{attempt: task.attempts})
        |> dispatch()
    end
  end

  # ============================================================
  # Tratamento de resultados
  # ============================================================

  defp complete(state, task, :ok) do
    log(state, :completed, task.id, %{attempts: task.attempts})
  end

  defp complete(state, task, {:error, :transient, reason}) do
    if task.attempts < state.max_attempts do
      schedule_retry(state, task, reason)
    else
      log(state, :failed, task.id, %{reason: reason, attempts: task.attempts, retries_exhausted: true})
    end
  end

  defp complete(state, task, {:error, :permanent, reason}) do
    log(state, :failed, task.id, %{reason: reason, attempts: task.attempts})
  end

  defp complete(state, task, other) do
    log(state, :failed, task.id, %{reason: {:unexpected_result, other}, attempts: task.attempts})
  end

  defp schedule_retry(state, task, reason) do
    # Backoff exponencial: base * 2^(tentativas - 1)
    delay = @base_backoff_ms * Integer.pow(2, task.attempts - 1)
    Process.send_after(self(), {:requeue, task}, delay)

    log(state, :retry_scheduled, task.id, %{delay_ms: delay, reason: reason, attempts: task.attempts})
  end

  # ============================================================
  # Utilitários
  # ============================================================

  defp log(state, event, task_id, details) do
    entry = %{
      at: System.system_time(:millisecond),
      event: event,
      task_id: task_id,
      details: details
    }

    %{state | history: Enum.take([entry | state.history], @history_limit)}
  end

  defp find_by_ref(running, ref) do
    Enum.find(running, fn {_id, entry} -> entry.ref == ref end)
  end

  defp schedule_periodic do
    Process.send_after(self(), :periodic, @periodic_interval_ms)
  end

  defp now, do: System.monotonic_time(:millisecond)

  defp default_executor(_payload), do: :ok
end
