# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir comunicação entre diferentes processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Gerenciador de fila de tarefas com histórico, classificação de erros,
  consulta de estado, comunicação entre processos e idempotência.

  Hierarquia de processos:

      TaskQueue (Supervisor, :rest_for_one)
      ├── TaskQueue.WorkerSupervisor (DynamicSupervisor)
      │   └── TaskQueue.Worker (um por tarefa, :temporary)
      └── TaskQueue.Manager (GenServer: estado, histórico e assinantes)

  Classificação de erros:

      {:validation, reasons} -> entrada inválida ou regra violada (ex.: id duplicado).
                                A tarefa nunca é executada.
      {:operational, reason} -> falha durante a execução (exceção, timeout,
                                retorno {:error, _}, crash do worker etc.).

  Uso básico:

      {:ok, _} = TaskQueue.start_link()
      {:ok, "t1"} = TaskQueue.submit("t1", fn -> {:ok, 42} end)
      {:ok, 42} = TaskQueue.await("t1")
      {:error, {:validation, _}} = TaskQueue.submit("t1", fn -> {:ok, 1} end)
  """

  use Supervisor

  alias TaskQueue.Manager

  @terminal [:completed, :failed]
  @default_await_timeout 5_000

  @type id :: String.t()
  @type error :: {:validation, term()} | {:operational, term()}

  ## API pública

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Submete uma tarefa de forma assíncrona.
  Retorna `{:ok, id}` quando aceita ou `{:error, error}` quando rejeitada.
  """
  @spec submit(id(), (-> term()), keyword()) :: {:ok, id()} | {:error, error()}
  def submit(id, fun, opts \\ []) do
    case TaskQueue.Validator.validate(id, fun, opts) do
      {:ok, task} ->
        Manager.call({:submit, task})

      {:error, error} = invalid ->
        # Erros de validação também entram no histórico.
        :ok = Manager.call({:reject, id, error})
        invalid
    end
  end

  @doc "Consulta o estado atual de uma operação."
  @spec status(id()) :: {:ok, map()} | {:error, :not_found}
  def status(id), do: Manager.call({:status, id})

  @doc "Lista as operações em andamento."
  @spec in_progress() :: [map()]
  def in_progress, do: Manager.call(:in_progress)

  @doc "Retorna o histórico de eventos em ordem cronológica. Aceita :all ou um id."
  @spec history(:all | id()) :: [map()]
  def history(filter \\ :all), do: Manager.call({:history, filter})

  @doc """
  Inscreve o processo chamador para receber mensagens `{:task_queue, event}`
  sempre que um evento ocorrer. Comunicação assíncrona entre processos.
  """
  def subscribe, do: Manager.call({:subscribe, self()})

  @doc "Remove a inscrição do processo chamador."
  def unsubscribe, do: Manager.call({:unsubscribe, self()})

  @doc """
  Aguarda o término de uma operação.
  Retorna `{:ok, result}`, `{:error, error}`, `{:error, :timeout}` ou `{:error, :not_found}`.
  """
  def await(id, timeout \\ @default_await_timeout) do
    # Assina antes de consultar o status para não perder eventos (sem race condition).
    fresh? = subscribe() == :subscribed
    deadline = if timeout == :infinity, do: :infinity, else: now_ms() + timeout

    try do
      wait_for_completion(id, deadline)
    after
      if fresh? do
        unsubscribe()
        flush_events()
      end
    end
  end

  ## Supervisor

  @impl true
  def init(_opts) do
    children = [
      {DynamicSupervisor, name: TaskQueue.WorkerSupervisor, strategy: :one_for_one},
      Manager
    ]

    # Se o DynamicSupervisor cair, o Manager também reinicia para manter o estado consistente.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  ## Funções privadas de espera

  defp wait_for_completion(id, deadline) do
    case status(id) do
      {:ok, %{status: :completed, result: result}} -> {:ok, result}
      {:ok, %{status: :failed, error: error}} -> {:error, error}
      {:ok, _running} -> receive_terminal_event(id, deadline)
      {:error, :not_found} -> {:error, :not_found}
    end
  end

  defp receive_terminal_event(id, deadline) do
    receive do
      {:task_queue, %{id: ^id, type: type}} when type in @terminal ->
        wait_for_completion(id, deadline)
    after
      remaining(deadline) -> {:error, :timeout}
    end
  end

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - now_ms(), 0)

  defp now_ms, do: System.monotonic_time(:millisecond)

  # Descarta eventos residuais deixados na mailbox do chamador após o await.
  defp flush_events do
    receive do
      {:task_queue, _event} -> flush_events()
    after
      0 -> :ok
    end
  end
end

defmodule TaskQueue.Validator do
  @moduledoc """
  Validação de entrada. Todos os erros retornados aqui são `{:validation, _}`.
  """

  @default_timeout 5_000

  @spec validate(term(), term(), term()) ::
          {:ok, map()} | {:error, {:validation, [String.t()]}}
  def validate(id, fun, opts) do
    keyword? = is_list(opts) and Keyword.keyword?(opts)
    timeout = if keyword?, do: Keyword.get(opts, :timeout, @default_timeout), else: nil

    messages =
      [
        rule(is_binary(id) and id != "", "id deve ser uma string não vazia"),
        rule(is_function(fun, 0), "fun deve ser uma função de aridade 0"),
        rule(keyword?, "opts deve ser uma keyword list"),
        rule(
          not keyword? or valid_timeout?(timeout),
          "timeout deve ser inteiro positivo ou :infinity"
        )
      ]
      |> Enum.reject(&is_nil/1)

    case messages do
      [] -> {:ok, %{id: id, fun: fun, timeout: timeout}}
      _ -> {:error, {:validation, messages}}
    end
  end

  defp rule(true, _message), do: nil
  defp rule(false, message), do: message

  defp valid_timeout?(:infinity), do: true
  defp valid_timeout?(timeout), do: is_integer(timeout) and timeout > 0
end

defmodule TaskQueue.Worker do
  @moduledoc """
  Processo temporário que executa uma única tarefa e reporta o resultado
  ao Manager. É iniciado pelo DynamicSupervisor e não é reiniciado automaticamente.
  """

  use GenServer

  def child_spec(args) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [args]}, restart: :temporary}
  end

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args) do
    # A execução ocorre em handle_continue para não bloquear o init
    # (o Manager chama start_child dentro de handle_call).
    {:ok, args, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, %{manager: manager, id: id, fun: fun, timeout: timeout} = state) do
    outcome = execute(fun, timeout)
    GenServer.cast(manager, {:finished, id, self(), outcome})
    {:stop, :normal, state}
  end

  defp execute(fun, timeout) do
    # O wrapper captura exceções e throws, transformando-os em dados.
    task =
      Task.async(fn ->
        try do
          {:returned, fun.()}
        rescue
          exception -> {:raised, Exception.message(exception)}
        catch
          kind, value -> {:thrown, {kind, value}}
        end
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:returned, {:ok, value}}} -> {:ok, value}
      {:ok, {:returned, {:error, reason}}} -> {:error, {:operational, reason}}
      {:ok, {:returned, other}} -> {:error, {:operational, {:invalid_return, other}}}
      {:ok, {:raised, message}} -> {:error, {:operational, {:exception, message}}}
      {:ok, {:thrown, thrown}} -> {:error, {:operational, {:thrown, thrown}}}
      {:exit, reason} -> {:error, {:operational, {:exit, reason}}}
      nil -> {:error, {:operational, :timeout}}
    end
  end
end

defmodule TaskQueue.Manager do
  @moduledoc false
  # Dono do estado: tarefas, histórico, monitores e assinantes.
  # Todas as mutações passam por aqui, o que serializa as operações.

  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def call(message), do: GenServer.call(__MODULE__, message)

  @impl true
  def init(_arg) do
    {:ok,
     %{
       # id => entrada da tarefa
       tasks: %{},
       # ref => {:worker, id} | {:subscriber, pid}
       monitors: %{},
       # pid => ref
       subscribers: %{},
       # eventos em ordem inversa
       history: [],
       seq: 0
     }}
  end

  ## Chamadas síncronas

  @impl true
  def handle_call({:submit, task}, _from, state) do
    %{id: id} = task

    case Map.fetch(state.tasks, id) do
      {:ok, %{status: status}} ->
        # Idempotência: uma operação já aceita nunca é executada novamente.
        error = {:validation, {:duplicate, id, status}}
        {:reply, {:error, error}, log(state, id, :rejected, %{error: error})}

      :error ->
        spec =
          {TaskQueue.Worker,
           %{manager: self(), id: id, fun: task.fun, timeout: task.timeout}}

        case DynamicSupervisor.start_child(TaskQueue.WorkerSupervisor, spec) do
          {:ok, pid} ->
            ref = Process.monitor(pid)

            entry = %{
              id: id,
              status: :running,
              result: nil,
              error: nil,
              started_at: now(),
              finished_at: nil,
              pid: pid,
              ref: ref
            }

            state = %{
              state
              | tasks: Map.put(state.tasks, id, entry),
                monitors: Map.put(state.monitors, ref, {:worker, id})
            }

            {:reply, {:ok, id}, log(state, id, :started, %{})}

          {:error, reason} ->
            # Não foi executada, então não é registrada como tarefa (pode ser reenviada).
            error = {:operational, {:spawn_failed, reason}}
            {:reply, {:error, error}, log(state, id, :start_failed, %{error: error})}
        end
    end
  end

  def handle_call({:reject, id, error}, _from, state) do
    {:reply, :ok, log(state, id, :rejected, %{error: error})}
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.tasks, id) do
        {:ok, entry} -> {:ok, info(entry)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    running = for {_id, %{status: :running} = entry} <- state.tasks, do: info(entry)
    {:reply, running, state}
  end

  def handle_call({:history, filter}, _from, state) do
    events =
      state.history
      |> Enum.reverse()
      |> Enum.filter(&matches?(&1, filter))

    {:reply, events, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :already_subscribed, state}
    else
      ref = Process.monitor(pid)

      state = %{
        state
        | subscribers: Map.put(state.subscribers, pid, ref),
          monitors: Map.put(state.monitors, ref, {:subscriber, pid})
      }

      {:reply, :subscribed, state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])

        state = %{
          state
          | subscribers: subscribers,
            monitors: Map.delete(state.monitors, ref)
        }

        {:reply, :ok, state}
    end
  end

  ## Mensagens assíncronas

  @impl true
  def handle_cast({:finished, id, pid, outcome}, state) do
    # Ignora relatórios de workers que não correspondem à execução atual.
    with %{status: :running, pid: ^pid} = entry <- Map.get(state.tasks, id) do
      case outcome do
        {:ok, value} -> {:noreply, settle(state, entry, :completed, value, nil)}
        {:error, error} -> {:noreply, settle(state, entry, :failed, nil, error)}
      end
    else
      _ -> {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {{:worker, id}, monitors} ->
        # Worker morreu antes de reportar: falha operacional.
        state = %{state | monitors: monitors}

        case Map.get(state.tasks, id) do
          %{status: :running} = entry ->
            error = {:operational, {:worker_down, reason}}
            {:noreply, settle(state, entry, :failed, nil, error)}

          _ ->
            {:noreply, state}
        end

      {{:subscriber, pid}, monitors} ->
        subscribers = Map.delete(state.subscribers, pid)
        {:noreply, %{state | monitors: monitors, subscribers: subscribers}}

      {nil, _monitors} ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  ## Funções privadas

  defp settle(state, entry, status, result, error) do
    Process.demonitor(entry.ref, [:flush])

    updated = %{
      entry
      | status: status,
        result: result,
        error: error,
        finished_at: now(),
        pid: nil,
        ref: nil
    }

    state = %{
      state
      | tasks: Map.put(state.tasks, entry.id, updated),
        monitors: Map.delete(state.monitors, entry.ref)
    }

    log(state, entry.id, status, %{result: result, error: error})
  end

  # Registra o evento no histórico e notifica os assinantes.
  defp log(state, id, type, details) do
    seq = state.seq + 1
    event = %{seq: seq, id: id, type: type, at: now(), details: details}

    Enum.each(Map.keys(state.subscribers), &send(&1, {:task_queue, event}))

    %{state | seq: seq, history: [event | state.history]}
  end

  defp info(entry) do
    Map.take(entry, [:id, :status, :result, :error, :started_at, :finished_at])
  end

  defp matches?(_event, :all), do: true
  defp matches?(event, id), do: event.id == id

  defp now, do: DateTime.utc_now()
end
