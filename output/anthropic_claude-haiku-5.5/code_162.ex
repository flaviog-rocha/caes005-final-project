# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de fila de tarefas por entidade, usando somente a biblioteca padrão.
# Para executar: `elixir task_queue.exs` (ou carregue com `Code.require_file/1` no iex).

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas com processamento independente por entidade.

  * Cada entidade possui seu próprio processo (GenServer) registrado via `Registry`,
    supervisionado por `DynamicSupervisor`. Entidades diferentes nunca bloqueiam umas às outras.
  * Erros são separados em:
      - `:validation`   -> entrada inválida (erro do chamador, não deve ser repetido automaticamente);
      - `:operational`  -> falha de execução/infra (handler falhou, storage indisponível, processo caiu).
  * O estado das operações é consultável em tempo real via `status/1` e `job_status/2`.
  * Notificações são enviadas a assinantes (`subscribe/1`) quando condições são atingidas:
      - `{:backlog_threshold_reached, tamanho}`
      - `{:job_failed_permanently, id, erro}`
      - `{:persistence_failed, id, erro}`
  """

  alias TaskQueue.{Error, Notifier, Worker}

  @default_storage {TaskQueue.Storage.Memory, TaskQueue.Storage}

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc "Inicia a infraestrutura (registries, supervisores e storage em memória)."
  def start_link(_opts \\ []) do
    children = [
      {Registry, keys: :unique, name: TaskQueue.Registry},
      {Registry, keys: :duplicate, name: TaskQueue.Notifications},
      {DynamicSupervisor, strategy: :one_for_one, name: TaskQueue.WorkerSup},
      {Task.Supervisor, name: TaskQueue.TaskSup},
      {TaskQueue.Storage.Memory, name: TaskQueue.Storage}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: TaskQueue.Supervisor)
  end

  @doc false
  def storage, do: Application.get_env(:task_queue, :storage, @default_storage)

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Garante que a entidade esteja em processamento.

  Opções:
    * `:max_backlog` - número de tarefas na fila a partir do qual é emitida notificação.
  """
  def ensure_entity(entity, handler, opts \\ []) do
    cond do
      not valid_entity?(entity) ->
        {:error, Error.validation([{:entity, "deve ser string não vazia"}])}

      not is_function(handler, 1) ->
        {:error, Error.validation([{:handler, "deve ser função de aridade 1"}])}

      true ->
        args = [
          entity: entity,
          handler: handler,
          max_backlog: Keyword.get(opts, :max_backlog)
        ]

        case DynamicSupervisor.start_child(TaskQueue.WorkerSup, {Worker, args}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, Error.operational({:start_failed, reason})}
        end
    end
  end

  @doc "Enfileira uma tarefa para a entidade. Retorna `{:ok, id}` ou `{:error, %Error{}}`."
  def enqueue(entity, attrs) do
    call(entity, {:enqueue, attrs})
  end

  @doc "Retorna o panorama atual da entidade: fila, operações em andamento e contadores."
  def status(entity) do
    call(entity, :status)
  end

  @doc """
  Retorna o estado de uma tarefa específica. Se não estiver em memória
  (ex.: já finalizada e descarregada), consulta o storage.
  """
  def job_status(entity, id) do
    case call(entity, {:job, id}) do
      {:ok, summary} -> {:ok, summary}
      {:error, :not_found} -> fetch_from_storage(entity, id)
      {:error, _} = error -> error
    end
  end

  @doc "Assina notificações da entidade. O processo chamador receberá `{:task_queue, entity, evento}`."
  def subscribe(entity), do: Notifier.subscribe(entity)

  @doc "Cancela a assinatura de notificações da entidade."
  def unsubscribe(entity), do: Notifier.unsubscribe(entity)

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  defp valid_entity?(entity), do: is_binary(entity) and entity != ""

  defp call(entity, message) do
    case Registry.lookup(TaskQueue.Registry, entity) do
      [{pid, _}] ->
        try do
          GenServer.call(pid, message)
        catch
          :exit, reason -> {:error, Error.operational({:worker_unavailable, reason})}
        end

      [] ->
        {:error, Error.operational(:unknown_entity)}
    end
  end

  defp fetch_from_storage(entity, id) do
    {mod, server} = storage()

    case mod.fetch(server, entity, id) do
      {:ok, record} -> {:ok, record}
      {:error, :not_found} -> {:error, Error.validation([{:id, "não encontrado"}])}
      {:error, reason} -> {:error, Error.operational({:storage, reason})}
    end
  end
end

# -----------------------------------------------------------------------------
# Erros: a distinção entre validação e falha operacional é explícita no `kind`.
# -----------------------------------------------------------------------------
defmodule TaskQueue.Error do
  @moduledoc """
  Estrutura de erro.

  * `kind: :validation`  -> dados de entrada inválidos. Não devem ser reexecutados sem correção.
  * `kind: :operational` -> falha durante a execução ou na infraestrutura. Pode ser transitória.
  """

  defstruct [:kind, :details]

  @type t :: %__MODULE__{kind: :validation | :operational, details: term()}

  @spec validation(list({atom(), String.t()})) :: t()
  def validation(details), do: %__MODULE__{kind: :validation, details: details}

  @spec operational(term()) :: t()
  def operational(reason), do: %__MODULE__{kind: :operational, details: reason}
end

# -----------------------------------------------------------------------------
# Job: representação validada de uma tarefa.
# -----------------------------------------------------------------------------
defmodule TaskQueue.Job do
  @moduledoc "Tarefa validada, pronta para ser enfileirada."

  alias TaskQueue.Error

  defstruct [:id, :entity, payload: %{}, max_attempts: 3]

  @type t :: %__MODULE__{
          id: String.t(),
          entity: String.t(),
          payload: map(),
          max_attempts: pos_integer()
        }

  @doc "Valida os atributos e constrói o job. Retorna erros de validação agregados."
  @spec new(String.t(), term()) :: {:ok, t()} | {:error, Error.t()}
  def new(entity, attrs) when is_map(attrs) do
    id = Map.get(attrs, :id)
    payload = Map.get(attrs, :payload, %{})
    max_attempts = Map.get(attrs, :max_attempts, 3)

    errors =
      [
        check(is_binary(id) and id != "", :id, "deve ser string não vazia"),
        check(is_map(payload), :payload, "deve ser um map"),
        check(
          is_integer(max_attempts) and max_attempts > 0,
          :max_attempts,
          "deve ser inteiro positivo"
        )
      ]
      |> Enum.reject(&is_nil/1)

    case errors do
      [] ->
        {:ok, %__MODULE__{id: id, entity: entity, payload: payload, max_attempts: max_attempts}}

      _ ->
        {:error, Error.validation(errors)}
    end
  end

  def new(_entity, _attrs) do
    {:error, Error.validation([{:attrs, "deve ser um map"}])}
  end

  defp check(true, _field, _msg), do: nil
  defp check(false, field, msg), do: {field, msg}
end

# -----------------------------------------------------------------------------
# Storage: contrato de persistência. Permite trocar a implementação
# (ETS, Ecto, arquivo, etc.) sem alterar a lógica da fila.
# -----------------------------------------------------------------------------
defmodule TaskQueue.Storage do
  @moduledoc "Contrato (behaviour) para o banco de dados fictício."

  @callback save(server :: term(), record :: map()) :: :ok | {:error, term()}
  @callback fetch(server :: term(), entity :: String.t(), id :: String.t()) ::
              {:ok, map()} | {:error, :not_found} | {:error, term()}
end

# -----------------------------------------------------------------------------
# Implementação em memória do storage, usando Agent (biblioteca padrão).
# -----------------------------------------------------------------------------
defmodule TaskQueue.Storage.Memory do
  @moduledoc "Banco fictício em memória. Falhas de acesso são convertidas em erro operacional."

  @behaviour TaskQueue.Storage

  def start_link(opts) do
    Agent.start_link(fn -> %{} end, name: Keyword.fetch!(opts, :name))
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @impl true
  def save(server, record) do
    Agent.update(server, &Map.put(&1, {record.entity, record.id}, record))
    :ok
  catch
    :exit, reason -> {:error, {:unavailable, reason}}
  end

  @impl true
  def fetch(server, entity, id) do
    case Agent.get(server, &Map.fetch(&1, {entity, id})) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  catch
    :exit, reason -> {:error, {:unavailable, reason}}
  end
end

# -----------------------------------------------------------------------------
# Notificações: pub/sub local baseado em Registry com chaves duplicadas.
# -----------------------------------------------------------------------------
defmodule TaskQueue.Notifier do
  @moduledoc "Publica eventos de condição para os assinantes de uma entidade."

  def subscribe(entity) do
    case Registry.register(TaskQueue.Notifications, entity, nil) do
      {:ok, _owner} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def unsubscribe(entity) do
    Registry.unregister(TaskQueue.Notifications, entity)
  end

  def notify(entity, event) do
    Registry.dispatch(TaskQueue.Notifications, entity, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:task_queue, entity, event})
    end)

    :ok
  end
end

# -----------------------------------------------------------------------------
# Worker: processo de uma entidade. Processa a fila sequencialmente e
# isoladamente, executando cada tarefa em um Task supervisionado.
# -----------------------------------------------------------------------------
defmodule TaskQueue.Worker do
  @moduledoc false

  use GenServer, restart: :transient

  alias TaskQueue.{Error, Job, Notifier}

  @in_progress [:queued, :running, :retrying]

  def start_link(opts) do
    entity = Keyword.fetch!(opts, :entity)
    GenServer.start_link(__MODULE__, opts, name: via(entity))
  end

  defp via(entity), do: {:via, Registry, {TaskQueue.Registry, entity}}

  @impl true
  def init(opts) do
    state = %{
      entity: Keyword.fetch!(opts, :entity),
      handler: Keyword.fetch!(opts, :handler),
      max_backlog: Keyword.get(opts, :max_backlog),
      queue: :queue.new(),
      # tarefa em execução: {job, ref_do_task} ou nil
      current: nil,
      # id => operação (estado atual de cada tarefa conhecida pelo worker)
      ops: %{},
      alerted: false
    }

    {:ok, state}
  end

  # -- Chamadas ---------------------------------------------------------------

  @impl true
  def handle_call({:enqueue, attrs}, _from, state) do
    with {:ok, job} <- Job.new(state.entity, attrs),
         :ok <- ensure_new_id(state, job.id),
         op = new_op(job),
         :ok <- persist(state, op) do
      state = %{state | ops: Map.put(state.ops, job.id, op), queue: :queue.in(job, state.queue)}
      state = state |> check_backlog() |> dispatch()
      {:reply, {:ok, job.id}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call(:status, _from, state) do
    ops = Map.values(state.ops)

    reply = %{
      entity: state.entity,
      queue_length: :queue.len(state.queue),
      in_progress: for(op <- ops, op.status in @in_progress, do: summary(op)),
      completed_count: Enum.count(ops, &(&1.status == :completed)),
      failed_count: Enum.count(ops, &(&1.status == :failed))
    }

    {:reply, {:ok, reply}, state}
  end

  def handle_call({:job, id}, _from, state) do
    case Map.fetch(state.ops, id) do
      {:ok, op} -> {:reply, {:ok, summary(op)}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  # -- Mensagens de execução --------------------------------------------------

  @impl true
  def handle_info({ref, result}, %{current: {job, ref}} = state) do
    Process.demonitor(ref, [:flush])
    finish(%{state | current: nil}, job, result)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{current: {job, ref}} = state) do
    fail(%{state | current: nil}, job, Error.operational({:crashed, reason}))
  end

  def handle_info(:dispatch, state), do: {:noreply, dispatch(state)}
  def handle_info(_other, state), do: {:noreply, state}

  # -- Processamento ----------------------------------------------------------

  defp dispatch(%{current: nil} = state) do
    case :queue.out(state.queue) do
      {{:value, job}, rest} ->
        state = check_backlog(%{state | queue: rest})
        attempts = Map.fetch!(state.ops, job.id).attempts + 1
        state = transition(state, job.id, %{status: :running, attempts: attempts})

        handler = state.handler

        %Task{ref: ref} =
          Task.Supervisor.async_nolink(TaskQueue.TaskSup, fn -> handler.(job) end)

        %{state | current: {job, ref}}

      {:empty, _} ->
        state
    end
  end

  defp dispatch(state), do: state

  defp finish(state, job, {:ok, result}) do
    state = transition(state, job.id, %{status: :completed, last_error: nil, result: result})
    {:noreply, dispatch(state)}
  end

  defp finish(state, job, {:error, reason}) do
    fail(state, job, Error.operational({:handler_failed, reason}))
  end

  defp finish(state, job, other) do
    fail(state, job, Error.operational({:unexpected_return, other}))
  end

  defp fail(state, job, error) do
    attempts = Map.fetch!(state.ops, job.id).attempts

    if attempts < job.max_attempts do
      # Falha operacional transitória: reenfileira ao final da fila.
      state = transition(state, job.id, %{status: :retrying, last_error: error})
      state = check_backlog(%{state | queue: :queue.in(job, state.queue)})
      {:noreply, dispatch(state)}
    else
      # Condição de notificação: falha definitiva.
      state = transition(state, job.id, %{status: :failed, last_error: error})
      notify(state, {:job_failed_permanently, job.id, error})
      {:noreply, dispatch(state)}
    end
  end

  # -- Backlog ----------------------------------------------------------------

  defp check_backlog(%{max_backlog: nil} = state), do: state

  defp check_backlog(state) do
    size = :queue.len(state.queue)

    cond do
      size >= state.max_backlog and not state.alerted ->
        notify(state, {:backlog_threshold_reached, size})
        %{state | alerted: true}

      size < state.max_backlog and state.alerted ->
        %{state | alerted: false}

      true ->
        state
    end
  end

  # -- Estado e persistência --------------------------------------------------

  defp new_op(job) do
    %{
      id: job.id,
      job: job,
      status: :queued,
      attempts: 0,
      last_error: nil,
      result: nil,
      updated_at: now()
    }
  end

  defp transition(state, id, changes) do
    op =
      state.ops
      |> Map.fetch!(id)
      |> Map.merge(changes)
      |> Map.put(:updated_at, now())

    state = %{state | ops: Map.put(state.ops, id, op)}
    persist_quietly(state, op)
  end

  # Falha de persistência em transição não interrompe a fila; apenas notifica.
  defp persist_quietly(state, op) do
    case persist(state, op) do
      :ok ->
        state

      {:error, error} ->
        notify(state, {:persistence_failed, op.id, error})
        state
    end
  end

  defp persist(state, op) do
    {mod, server} = TaskQueue.storage()

    record = %{
      entity: state.entity,
      id: op.id,
      status: op.status,
      attempts: op.attempts,
      last_error: op.last_error,
      payload: op.job.payload,
      updated_at: op.updated_at
    }

    case mod.save(server, record) do
      :ok -> :ok
      {:error, reason} -> {:error, Error.operational({:persistence_failed, reason})}
    end
  end

  defp ensure_new_id(state, id) do
    if Map.has_key?(state.ops, id) do
      {:error, Error.validation([{:id, "já existe nesta entidade"}])}
    else
      :ok
    end
  end

  defp summary(op) do
    %{
      id: op.id,
      status: op.status,
      attempts: op.attempts,
      last_error: op.last_error,
      result: op.result,
      updated_at: op.updated_at
    }
  end

  defp notify(state, event), do: Notifier.notify(state.entity, event)

  defp now, do: DateTime.utc_now()
end

# -----------------------------------------------------------------------------
# Exemplo opcional de uso com Ecto (não compilado por padrão, pois Ecto não é obrigatório).
#
# defmodule TaskQueue.Repo.Migrations.CreateTaskQueueRecords do
#   use Ecto.Migration
#
#   def change do
#     create table(:task_queue_records, primary_key: false) do
#       add :entity, :string, null: false, primary_key: true
#       add :id, :string, null: false, primary_key: true
#       add :status, :string, null: false
#       add :attempts, :integer, null: false, default: 0
#       add :last_error, :map
#       add :payload, :map
#       add :updated_at, :utc_datetime_usec
#     end
#   end
# end
#
# Um adaptador Ecto implementaria `TaskQueue.Storage` com:
#   save/2  -> Repo.insert(record, on_conflict: :replace_all, conflict_target: [:entity, :id])
#   fetch/3 -> Repo.get_by(table, entity: entity, id: id) |> normalizar retorno
# e seria configurado com: config :task_queue, storage: {MyApp.Storage.Ecto, MyApp.Repo}
# -----------------------------------------------------------------------------
