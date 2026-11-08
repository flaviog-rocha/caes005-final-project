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
  Fila de tarefas concorrente e tolerante a falhas.

  Requisitos atendidos:
    1. Consistência: todo o estado é alterado sequencialmente dentro do GenServer,
       então operações concorrentes de múltiplos chamadores nunca corrompem a fila.
    2. Detecção de encerramentos inesperados: cada worker é monitorado com
       `spawn_monitor/1`; qualquer término sem resultado é registrado.
    3. Histórico: cada operação relevante gera um evento limitado em tamanho.
    4. Novas tentativas: falhas temporárias e crashes são reagendados com backoff
       exponencial, até `max_attempts`.
    5. Jobs periódicos: funções configuradas em `:periodic` são executadas em intervalos fixos.

  Contrato de uma tarefa (função de aridade 0):
    * :ok                           -> sucesso
    * {:error, :temporary, motivo}  -> falha temporária (nova tentativa)
    * {:error, :permanent, motivo}  -> falha definitiva (sem nova tentativa)
    * qualquer outro retorno ou término inesperado -> tratado como falha temporária
  """

  use GenServer
  require Logger

  @default_opts [
    name: __MODULE__,
    max_concurrency: 4,
    max_attempts: 3,
    base_backoff_ms: 200,
    history_limit: 500,
    periodic: []
  ]

  ## API pública

  @doc "Inicia a fila. Aceita as opções de @default_opts."
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc "Enfileira uma tarefa. Retorna `{:ok, id}`."
  def enqueue(server, fun, opts \\ []) when is_function(fun, 0) do
    GenServer.call(server, {:enqueue, fun, opts})
  end

  @doc "Retorna um resumo do estado atual da fila."
  def status(server), do: GenServer.call(server, :status)

  @doc "Retorna o histórico de eventos em ordem cronológica."
  def history(server), do: GenServer.call(server, :history)

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    config = Map.new(opts)

    # Agenda a primeira execução de cada job periódico.
    config.periodic
    |> Enum.with_index()
    |> Enum.each(fn {{interval, _job}, i} ->
      Process.send_after(self(), {:periodic, i}, interval)
    end)

    state = %{
      config: config,
      pending: :queue.new(),   # tarefas prontas para execução (FIFO)
      waiting: %{},            # id => tarefa aguardando backoff de retry
      running: %{},            # pid => %{task: tarefa, ref: monitor_ref}
      failed: [],              # [{id, motivo}] de falhas definitivas
      history: [],             # eventos, do mais recente para o mais antigo
      next_id: 1
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, fun, opts}, _from, state) do
    id = state.next_id

    task = %{
      id: id,
      fun: fun,
      attempts: 0,
      max_attempts: Keyword.get(opts, :max_attempts, state.config.max_attempts)
    }

    state =
      %{state | next_id: id + 1, pending: :queue.in(task, state.pending)}
      |> record(:enqueued, id, %{})
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call(:status, _from, state) do
    {:reply, snapshot(state), state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  # Resultado enviado explicitamente pelo worker.
  @impl true
  def handle_info({:task_result, pid, result}, state) do
    case Map.get(state.running, pid) do
      %{task: task, ref: ref} ->
        # O resultado chegou; removemos o monitor para não receber :DOWN depois.
        Process.demonitor(ref, [:flush])

        state =
          %{state | running: Map.delete(state.running, pid)}
          |> handle_result(task, result)
          |> dispatch()

        {:noreply, state}

      nil ->
        # Resultado de worker desconhecido ou já tratado: ignorado de forma idempotente.
        {:noreply, state}
    end
  end

  # Encerramento do worker. Se o resultado não chegou antes, o processo terminou inesperadamente.
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.get(state.running, pid) do
      %{task: task, ref: ^ref} ->
        Logger.warning("TaskQueue: worker da tarefa #{task.id} encerrou inesperadamente: #{inspect(reason)}")

        state =
          %{state | running: Map.delete(state.running, pid)}
          |> handle_result(task, {:error, :temporary, {:crashed, reason}})
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # Fim do backoff: a tarefa volta para a fila de execução.
  def handle_info({:retry, id}, state) do
    case Map.pop(state.waiting, id) do
      {nil, _waiting} ->
        {:noreply, state}

      {task, waiting} ->
        state =
          %{state | waiting: waiting, pending: :queue.in(task, state.pending)}
          |> record(:requeued, id, %{attempt: task.attempts})
          |> dispatch()

        {:noreply, state}
    end
  end

  # Job periódico: executa de forma assíncrona com um snapshot do estado e reagenda.
  def handle_info({:periodic, i}, state) do
    {interval, job} = Enum.at(state.config.periodic, i)
    snap = snapshot(state)

    Task.start(fn -> job.(snap) end)
    Process.send_after(self(), {:periodic, i}, interval)

    {:noreply, record(state, :periodic_run, nil, %{job: i})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Funções privadas

  # Inicia workers enquanto houver capacidade e tarefas pendentes.
  defp dispatch(state) do
    cond do
      map_size(state.running) >= state.config.max_concurrency -> state
      :queue.is_empty(state.pending) -> state
      true ->
        {{:value, task}, pending} = :queue.out(state.pending)

        state
        |> Map.put(:pending, pending)
        |> start_worker(task)
        |> dispatch()
    end
  end

  defp start_worker(state, task) do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        result = task.fun.()
        send(parent, {:task_result, self(), result})
      end)

    task = %{task | attempts: task.attempts + 1}

    %{state | running: Map.put(state.running, pid, %{task: task, ref: ref})}
    |> record(:started, task.id, %{attempt: task.attempts, pid: inspect(pid)})
  end

  defp handle_result(state, task, :ok) do
    record(state, :succeeded, task.id, %{attempt: task.attempts})
  end

  defp handle_result(state, task, {:error, :permanent, reason}) do
    fail(state, task, reason)
  end

  defp handle_result(state, task, {:error, :temporary, reason}) do
    retry_or_fail(state, task, reason)
  end

  defp handle_result(state, task, other) do
    retry_or_fail(state, task, {:unexpected_result, other})
  end

  defp retry_or_fail(state, task, reason) do
    if task.attempts < task.max_attempts do
      # Backoff exponencial: base * 2^(tentativas - 1).
      delay = state.config.base_backoff_ms * Integer.pow(2, task.attempts - 1)
      Process.send_after(self(), {:retry, task.id}, delay)

      %{state | waiting: Map.put(state.waiting, task.id, task)}
      |> record(:retry_scheduled, task.id, %{attempt: task.attempts, delay_ms: delay, reason: reason})
    else
      fail(state, task, {:exhausted, reason})
    end
  end

  defp fail(state, task, reason) do
    Logger.error("TaskQueue: tarefa #{task.id} falhou definitivamente: #{inspect(reason)}")

    %{state | failed: [{task.id, reason} | state.failed]}
    |> record(:failed, task.id, %{attempt: task.attempts, reason: inspect(reason)})
  end

  # Registra um evento no histórico, mantendo apenas os `history_limit` mais recentes.
  defp record(state, event, id, details) do
    entry = %{
      at: System.system_time(:millisecond),
      event: event,
      task_id: id,
      details: details
    }

    %{state | history: Enum.take([entry | state.history], state.config.history_limit)}
  end

  defp snapshot(state) do
    %{
      pending: :queue.len(state.pending),
      running: map_size(state.running),
      waiting: map_size(state.waiting),
      failed: length(state.failed)
    }
  end
end
