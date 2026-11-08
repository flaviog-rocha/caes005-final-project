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

defmodule NotificationSystem do
  @moduledoc """
  Gerenciador de operações e notificações baseado em ETS.
  """

  @doc "Inicia o supervisor do sistema."
  def start_link do
    Supervisor.start_link(
      [
        NotificationSystem.Store,
        NotificationSystem.Manager
      ],
      strategy: :one_for_one,
      name: NotificationSystem.Supervisor
    )
  end

  @doc """
  Cria uma operação.

  Atributos aceitos: `:name` (obrigatório), `:notify_at` e `:progress`.
  Quando o progresso inicial já atinge o limite, uma notificação é gerada.
  """
  def create(attrs) when is_map(attrs) do
    with :ok <- validate_keys(attrs, [:name, :notify_at, :progress]),
         {:ok, name} <- validate_name(Map.get(attrs, :name)),
         {:ok, notify_at} <- validate_non_negative_integer(Map.get(attrs, :notify_at, 1), :notify_at),
         {:ok, progress} <- validate_non_negative_integer(Map.get(attrs, :progress, 0), :progress) do
      GenServer.call(NotificationSystem.Manager, {:create, name, notify_at, progress})
    end
  end

  def create(_), do: {:error, :invalid_attributes}

  @doc "Atualiza o progresso de uma operação em andamento."
  def advance(id, progress) do
    with :ok <- validate_id(id),
         {:ok, progress} <- validate_non_negative_integer(progress, :progress) do
      GenServer.call(NotificationSystem.Manager, {:advance, id, progress})
    end
  end

  @doc "Marca uma operação em andamento como concluída."
  def complete(id) do
    with :ok <- validate_id(id) do
      GenServer.call(NotificationSystem.Manager, {:complete, id})
    end
  end

  @doc "Consulta uma operação pelo identificador."
  def get(id) do
    with :ok <- validate_id(id) do
      GenServer.call(NotificationSystem.Manager, {:get, id})
    end
  end

  @doc "Lista as operações que ainda estão em andamento."
  def list_active do
    GenServer.call(NotificationSystem.Manager, :list_active)
  end

  @doc "Retorna o histórico de operações, em ordem cronológica."
  def history do
    GenServer.call(NotificationSystem.Manager, :history)
  end

  @doc "Retorna as notificações geradas, em ordem cronológica."
  def notifications do
    GenServer.call(NotificationSystem.Manager, :notifications)
  end

  defp validate_keys(attrs, allowed) do
    if Enum.all?(Map.keys(attrs), &(&1 in allowed)) do
      :ok
    else
      {:error, :unknown_attribute}
    end
  end

  defp validate_name(name) when is_binary(name) do
    trimmed = String.trim(name)

    if trimmed == "" do
      {:error, :invalid_name}
    else
      {:ok, trimmed}
    end
  end

  defp validate_name(_), do: {:error, :invalid_name}

  defp validate_non_negative_integer(value, _field)
       when is_integer(value) and value >= 0,
       do: {:ok, value}

  defp validate_non_negative_integer(_, field), do: {:error, {:invalid_value, field}}

  defp validate_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_id(_), do: {:error, :invalid_id}
end

defmodule NotificationSystem.Store do
  @moduledoc false
  use GenServer

  @operations :notification_system_operations
  @history :notification_system_history
  @notifications :notification_system_notifications

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def fetch_operation(id), do: GenServer.call(__MODULE__, {:fetch_operation, id})
  def operations, do: GenServer.call(__MODULE__, :operations)
  def history, do: GenServer.call(__MODULE__, :history)
  def notifications, do: GenServer.call(__MODULE__, :notifications)

  def commit(operation, action, details, notification \\ nil) do
    GenServer.call(__MODULE__, {:commit, operation, action, details, notification})
  end

  @impl true
  def init(:ok) do
    :ets.new(@operations, [:named_table, :set, :protected, read_concurrency: true])
    :ets.new(@history, [:named_table, :ordered_set, :protected, read_concurrency: true])
    :ets.new(@notifications, [:named_table, :ordered_set, :protected, read_concurrency: true])

    {:ok, %{next_sequence: 1}}
  end

  @impl true
  def handle_call({:fetch_operation, id}, _from, state) do
    reply =
      case :ets.lookup(@operations, id) do
        [{^id, operation}] -> {:ok, operation}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:operations, _from, state) do
    operations =
      @operations
      |> :ets.tab2list()
      |> Enum.map(fn {_id, operation} -> operation end)

    {:reply, operations, state}
  end

  def handle_call(:history, _from, state) do
    entries =
      @history
      |> :ets.tab2list()
      |> Enum.map(fn {_sequence, entry} -> entry end)

    {:reply, entries, state}
  end

  def handle_call(:notifications, _from, state) do
    entries =
      @notifications
      |> :ets.tab2list()
      |> Enum.map(fn {_sequence, notification} -> notification end)

    {:reply, entries, state}
  end

  def handle_call({:commit, operation, action, details, notification}, _from, state) do
    sequence = state.next_sequence
    timestamp = DateTime.utc_now()

    event = %{
      sequence: sequence,
      timestamp: timestamp,
      operation_id: operation.id,
      action: action,
      details: details
    }

    :ets.insert(@operations, {operation.id, operation})
    :ets.insert(@history, {sequence, event})

    if notification do
      generated_notification =
        Map.merge(notification, %{sequence: sequence, timestamp: timestamp})

      :ets.insert(@notifications, {sequence, generated_notification})
    end

    {:reply, {:ok, event}, %{state | next_sequence: sequence + 1}}
  end
end

defmodule NotificationSystem.Manager do
  @moduledoc false
  use GenServer

  alias NotificationSystem.Store

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    # O estado operacional fica no ETS, que pertence ao Store e sobrevive
    # à reinicialização deste processo pelo supervisor.
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, name, notify_at, progress}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])
    notification_generated = progress >= notify_at

    operation = %{
      id: id,
      name: name,
      status: :running,
      progress: progress,
      notify_at: notify_at,
      notified: notification_generated,
      created_at: DateTime.utc_now(),
      updated_at: DateTime.utc_now()
    }

    notification =
      if notification_generated do
        notification_for(operation)
      else
        nil
      end

    {:ok, _event} = Store.commit(operation, :created, %{progress: progress}, notification)
    {:reply, {:ok, operation}, state}
  end

  def handle_call({:advance, id, progress}, _from, state) do
    reply =
      with {:ok, operation} <- Store.fetch_operation(id),
           :ok <- ensure_running(operation),
           :ok <- ensure_progress_not_decreased(operation, progress) do
        notified = operation.notified or progress >= operation.notify_at

        updated_operation = %{
          operation
          | progress: progress,
            notified: notified,
            updated_at: DateTime.utc_now()
        }

        notification =
          if not operation.notified and progress >= operation.notify_at do
            notification_for(updated_operation)
          else
            nil
          end

        {:ok, _event} =
          Store.commit(
            updated_operation,
            :progress_updated,
            %{previous_progress: operation.progress, progress: progress},
            notification
          )

        {:ok, updated_operation}
      end

    {:reply, reply, state}
  end

  def handle_call({:complete, id}, _from, state) do
    reply =
      with {:ok, operation} <- Store.fetch_operation(id),
           :ok <- ensure_running(operation) do
        updated_operation = %{
          operation
          | status: :completed,
            updated_at: DateTime.utc_now()
        }

        {:ok, _event} =
          Store.commit(updated_operation, :completed, %{progress: operation.progress})

        {:ok, updated_operation}
      end

    {:reply, reply, state}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Store.fetch_operation(id), state}
  end

  def handle_call(:list_active, _from, state) do
    active =
      Store.operations()
      |> Enum.filter(&(&1.status == :running))
      |> Enum.sort_by(& &1.id)

    {:reply, active, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Store.history(), state}
  end

  def handle_call(:notifications, _from, state) do
    {:reply, Store.notifications(), state}
  end

  defp ensure_running(%{status: :running}), do: :ok
  defp ensure_running(_), do: {:error, :not_running}

  defp ensure_progress_not_decreased(operation, progress) do
    if progress >= operation.progress do
      :ok
    else
      {:error, :progress_cannot_decrease}
    end
  end

  defp notification_for(operation) do
    %{
      operation_id: operation.id,
      message: "A operação #{operation.name} atingiu o progresso #{operation.progress}.",
      condition: %{field: :progress, operator: :greater_than_or_equal, value: operation.notify_at}
    }
  end
end
