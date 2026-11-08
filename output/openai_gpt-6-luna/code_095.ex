# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  use Supervisor

  @worker NotificationManager.Worker
  @operations :notification_manager_operations
  @history :notification_manager_history
  @notifications :notification_manager_notifications
  @meta :notification_manager_meta

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      NotificationManager.TableKeeper,
      {NotificationManager.Worker, []}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 5)
  end

  def create_operation(attrs), do: GenServer.call(@worker, {:create, attrs})
  def update_operation(id, attrs), do: GenServer.call(@worker, {:update, id, attrs})
  def status(id), do: GenServer.call(@worker, {:status, id})
  def operations, do: GenServer.call(@worker, :operations)
  def history, do: GenServer.call(@worker, {:history, :all})
  def history(id), do: GenServer.call(@worker, {:history, id})
  def notifications, do: GenServer.call(@worker, :notifications)
end

defmodule NotificationManager.TableKeeper do
  use GenServer

  @operations :notification_manager_operations
  @history :notification_manager_history
  @notifications :notification_manager_notifications
  @meta :notification_manager_meta

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    :ets.new(@operations, [:named_table, :set, :public, read_concurrency: true])
    :ets.new(@history, [:named_table, :ordered_set, :public, read_concurrency: true])
    :ets.new(@notifications, [:named_table, :ordered_set, :public, read_concurrency: true])
    :ets.new(@meta, [:named_table, :set, :public])
    :ets.insert(@meta, {:sequence, 0})

    {:ok, %{}}
  end
end

defmodule NotificationManager.Worker do
  use GenServer

  @operations :notification_manager_operations
  @history :notification_manager_history
  @notifications :notification_manager_notifications
  @meta :notification_manager_meta

  @create_keys [:id, :threshold]
  @update_keys [:progress, :status]
  @statuses [:pending, :running, :completed, :failed]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, id, threshold} <- validate_create(attrs),
         false <- :ets.member(@operations, id) do
      now = now()

      operation = %{
        id: id,
        status: :pending,
        progress: 0,
        threshold: threshold,
        threshold_notified: false,
        created_at: now,
        updated_at: now
      }

      :ets.insert(@operations, {id, operation})
      record_history(id, :created, operation)

      {:reply, {:ok, operation}, state}
    else
      true -> {:reply, {:error, :operation_already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with :ok <- validate_id(id),
         {:ok, changes} <- validate_update(attrs),
         [{^id, current}] <- :ets.lookup(@operations, id),
         :ok <- validate_transition(current, changes) do
      updated =
        current
        |> Map.merge(changes)
        |> Map.put(:updated_at, now())

      threshold_reached =
        not current.threshold_notified and updated.progress >= updated.threshold

      updated =
        if threshold_reached do
          Map.put(updated, :threshold_notified, true)
        else
          updated
        end

      :ets.insert(@operations, {id, updated})
      record_history(id, :updated, updated)

      if threshold_reached do
        record_notification(id, :threshold_reached, updated)
      end

      if current.status != updated.status and updated.status in [:completed, :failed] do
        record_notification(id, updated.status, updated)
      end

      {:reply, {:ok, updated}, state}
    else
      [] -> {:reply, {:error, :operation_not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case :ets.lookup(@operations, id) do
        [{^id, operation}] -> {:ok, operation}
        [] -> {:error, :operation_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:operations, _from, state) do
    result =
      @operations
      |> :ets.tab2list()
      |> Enum.map(fn {_id, operation} -> operation end)
      |> Enum.sort_by(& &1.created_at)

    {:reply, result, state}
  end

  def handle_call({:history, id}, _from, state) do
    result =
      @history
      |> :ets.tab2list()
      |> Enum.filter(fn {_sequence, operation_id, _action, _snapshot, _timestamp} ->
        id == :all or operation_id == id
      end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {_sequence, operation_id, action, snapshot, timestamp} ->
        %{
          operation_id: operation_id,
          action: action,
          snapshot: snapshot,
          timestamp: timestamp
        }
      end)

    {:reply, result, state}
  end

  def handle_call(:notifications, _from, state) do
    result =
      @notifications
      |> :ets.tab2list()
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {_sequence, notification} -> notification end)

    {:reply, result, state}
  end

  defp validate_create(attrs) when is_map(attrs) do
    with :ok <- validate_keys(attrs, @create_keys),
         {:ok, id} <- required_id(attrs),
         threshold when is_integer(threshold) and threshold in 1..100 <-
           Map.get(attrs, :threshold, 100) do
      {:ok, id, threshold}
    else
      nil -> {:error, :missing_id}
      threshold when not is_integer(threshold) -> {:error, :invalid_threshold}
      _ -> {:error, :invalid_threshold}
    end
  end

  defp validate_create(_), do: {:error, :invalid_attributes}

  defp required_id(attrs) do
    case Map.fetch(attrs, :id) do
      {:ok, id} ->
        case validate_id(id) do
          :ok -> {:ok, id}
          error -> error
        end

      :error ->
        {:error, :missing_id}
    end
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(id) when is_integer(id), do: :ok
  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_update(attrs) when is_map(attrs) do
    with :ok <- validate_keys(attrs, @update_keys),
         :ok <- validate_progress(Map.get(attrs, :progress)),
         :ok <- validate_status(Map.get(attrs, :status)) do
      {:ok, attrs}
    end
  end

  defp validate_update(_), do: {:error, :invalid_attributes}

  defp validate_progress(nil), do: :ok
  defp validate_progress(value) when is_integer(value) and value in 0..100, do: :ok
  defp validate_progress(_), do: {:error, :invalid_progress}

  defp validate_status(nil), do: :ok
  defp validate_status(value) when value in @statuses, do: :ok
  defp validate_status(_), do: {:error, :invalid_status}

  defp validate_keys(attrs, allowed) do
    if Enum.all?(Map.keys(attrs), &(&1 in allowed)) do
      :ok
    else
      {:error, :unknown_attribute}
    end
  end

  defp validate_transition(current, changes) do
    if current.status in [:completed, :failed] do
      {:error, :operation_finished}
    else
      progress = Map.get(changes, :progress, current.progress)
      status = Map.get(changes, :status, current.status)

      cond do
        progress < current.progress ->
          {:error, :progress_cannot_decrease}

        not allowed_transition?(current.status, status) ->
          {:error, :invalid_status_transition}

        true ->
          :ok
      end
    end
  end

  defp allowed_transition?(status, status), do: true
  defp allowed_transition?(:pending, next), do: next in [:running, :completed, :failed]
  defp allowed_transition?(:running, next), do: next in [:completed, :failed]
  defp allowed_transition?(_, _), do: false

  defp record_history(id, action, operation) do
    sequence = next_sequence()
    timestamp = now()
    :ets.insert(@history, {sequence, id, action, operation, timestamp})
  end

  defp record_notification(id, condition, operation) do
    sequence = next_sequence()

    notification = %{
      operation_id: id,
      condition: condition,
      status: operation.status,
      progress: operation.progress,
      timestamp: now()
    }

    :ets.insert(@notifications, {sequence, notification})
  end

  defp next_sequence do
    :ets.update_counter(@meta, :sequence, {2, 1}, {:sequence, 0})
  end

  defp now, do: System.system_time(:millisecond)
end
