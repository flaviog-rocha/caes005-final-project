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

defmodule TaskQueue do
  @moduledoc """
  Fila de tarefas independente por entidade.

  Inicie com `TaskQueue.start_link/1`. Tarefas são funções de aridade zero.
  Falhas de validação retornam `{:error, {:validation, motivo}}`; falhas
  operacionais retornam `{:error, {:operational, motivo}}`.

  Os assinantes recebem mensagens no formato
  `{:task_queue, entidade, %{event: evento, ...}}`.
  """

  @registry __MODULE__.Registry
  @task_supervisor __MODULE__.TaskSupervisor
  @dynamic_supervisor __MODULE__.DynamicSupervisor
  @supervisor __MODULE__.Supervisor

  def start_link(opts \\ []) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {Task.Supervisor, name: @task_supervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor}
    ]

    Supervisor.start_link(children, Keyword.put_new(opts, :name, @supervisor))
  end

  @doc "Adiciona uma função à fila da entidade e retorna o identificador da operação."
  def enqueue(entity, task) do
    with :ok <- validate_entity(entity),
         :ok <- validate_task(task),
         {:ok, pid} <- ensure_entity(entity) do
      call(pid, {:enqueue, task})
    end
  end

  @doc "Retorna o estado da fila e das operações da entidade."
  def status(entity) do
    with :ok <- validate_entity(entity),
         {:ok, pid} <- ensure_entity(entity) do
      call(pid, :status)
    end
  end

  @doc "Consulta uma operação pelo identificador."
  def operation_status(entity, operation_id) do
    with :ok <- validate_entity(entity),
         :ok <- validate_operation_id(operation_id),
         {:ok, pid} <- ensure_entity(entity) do
      call(pid, {:operation_status, operation_id})
    end
  end

  @doc "Assina as notificações de uma entidade."
  def subscribe(entity, subscriber \\ self()) do
    with :ok <- validate_entity(entity),
         :ok <- validate_subscriber(subscriber),
         {:ok, pid} <- ensure_entity(entity) do
      call(pid, {:subscribe, subscriber})
    end
  end

  defp validate_entity(nil), do: {:error, {:validation, :entity_required}}
  defp validate_entity(_entity), do: :ok

  defp validate_task(task) when is_function(task, 0), do: :ok
  defp validate_task(_task), do: {:error, {:validation, :task_must_be_zero_arity_function}}

  defp validate_operation_id(nil), do: {:error, {:validation, :operation_id_required}}
  defp validate_operation_id(_id), do: :ok

  defp validate_subscriber(pid) when is_pid(pid), do: :ok
  defp validate_subscriber(_pid), do: {:error, {:validation, :subscriber_must_be_pid}}

  defp ensure_entity(entity) do
    case Registry.lookup(@registry, entity) do
      [{pid, _value}] ->
        {:ok, pid}

      [] ->
        start_entity(entity)
    end
  rescue
    error ->
      {:error, {:operational, error}}
  catch
    :exit, reason ->
      {:error, {:operational, reason}}
  end

  defp start_entity(entity) do
    case DynamicSupervisor.start_child(
           @dynamic_supervisor,
           {TaskQueue.Entity, {entity, @task_supervisor}}
         ) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, _reason} = error ->
        case Registry.lookup(@registry, entity) do
          [{pid, _value}] -> {:ok, pid}
          [] -> {:error, {:operational, error}}
        end
    end
  end

  defp call(pid, message) do
    GenServer.call(pid, message)
  rescue
    error ->
      {:error, {:operational, error}}
  catch
    :exit, reason ->
      {:error, {:operational, reason}}
  end
end

defmodule TaskQueue.Entity do
  use GenServer

  defstruct entity: nil,
            task_supervisor: nil,
            queue: nil,
            current: nil,
            operations: %{},
            subscribers: %{},
            monitors: %{}

  def child_spec({entity, task_supervisor} = args) do
    %{
      id: {__MODULE__, entity},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link({entity, _task_supervisor} = args) do
    GenServer.start_link(
      __MODULE__,
      args,
      name: {:via, Registry, {TaskQueue.Registry, entity}}
    )
  end

  @impl true
  def init({entity, task_supervisor}) do
    {:ok,
     %__MODULE__{
       entity: entity,
       task_supervisor: task_supervisor,
       queue: :queue.new()
     }}
  end

  @impl true
  def handle_call({:enqueue, task}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])
    now = DateTime.utc_now()

    operation = %{
      id: id,
      status: :queued,
      enqueued_at: now
    }

    item = %{id: id, function: task}

    state =
      state
      |> Map.put(:queue, :queue.in(item, state.queue))
      |> put_in([Access.key(:operations), id], operation)
      |> emit(:queued, id)

    {:reply, {:ok, id}, dispatch(state)}
  end

  def handle_call(:status, _from, state) do
    response = %{
      entity: state.entity,
      current: current_operation(state),
      queued: state.queue |> :queue.to_list() |> Enum.map(& &1.id),
      operations: state.operations
    }

    {:reply, {:ok, response}, state}
  end

  def handle_call({:operation_status, id}, _from, state) do
    reply =
      case Map.fetch(state.operations, id) do
        {:ok, operation} -> {:ok, operation}
        :error -> {:error, {:validation, :unknown_operation}}
      end

    {:reply, reply, state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    if Map.has_key?(state.subscribers, subscriber) do
      {:reply, :ok, state}
    else
      reference = Process.monitor(subscriber)

      state = %{
        state
        | subscribers: Map.put(state.subscribers, subscriber, reference),
          monitors: Map.put(state.monitors, reference, subscriber)
      }

      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({reference, result}, %{current: %{reference: reference}} = state) do
    Process.demonitor(reference, [:flush])

    state =
      case result do
        {:error, reason} ->
          finish_current(state, :failed, %{reason: reason})

        value ->
          finish_current(state, :succeeded, %{result: value})
      end

    {:noreply, dispatch(state)}
  end

  def handle_info(
        {:DOWN, reference, :process, _pid, reason},
        %{current: %{reference: reference}} = state
      ) do
    state =
      finish_current(state, :failed, %{reason: {:task_exit, reason}})

    {:noreply, dispatch(state)}
  end

  def handle_info({:DOWN, reference, :process, _pid, _reason}, state) do
    case Map.pop(state.monitors, reference) do
      {nil, _monitors} ->
        {:noreply, state}

      {subscriber, monitors} ->
        {:noreply,
         %{
           state
           | monitors: monitors,
             subscribers: Map.delete(state.subscribers, subscriber)
         }}
    end
  end

  defp dispatch(%{current: current} = state) when not is_nil(current), do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, item}, queue} ->
        state = %{state | queue: queue}

        launch_task(state, item)
    end
  end

  defp launch_task(state, item) do
    launch =
      try do
        Task.Supervisor.async_nolink(state.task_supervisor, fn ->
          item.function.()
        end)
      rescue
        error -> {:error, {:error, error}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case launch do
      %Task{} = task ->
        now = DateTime.utc_now()

        operation =
          state.operations
          |> Map.fetch!(item.id)
          |> Map.merge(%{status: :running, started_at: now})

        state =
          state
          |> put_in([Access.key(:operations), item.id], operation)
          |> Map.put(:current, %{
            id: item.id,
            reference: task.ref,
            started_at: now
          })
          |> emit(:started, item.id)

        state

      {:error, reason} ->
        state
        |> complete(item.id, :failed, %{reason: reason})
        |> dispatch()
    end
  end

  defp finish_current(%{current: current} = state, status, details) do
    state
    |> Map.put(:current, nil)
    |> complete(current.id, status, details)
  end

  defp complete(state, id, status, details) do
    operation =
      state.operations
      |> Map.fetch!(id)
      |> Map.merge(details)
      |> Map.merge(%{status: status, finished_at: DateTime.utc_now()})

    state =
      state
      |> put_in([Access.key(:operations), id], operation)
      |> emit(status, id, details)

    if :queue.is_empty(state.queue) do
      emit(state, :queue_empty, id)
    else
      state
    end
  end

  defp current_operation(%{current: nil}), do: nil

  defp current_operation(state) do
    Map.get(state.operations, state.current.id)
  end

  defp emit(state, event, task_id, details \\ %{}) do
    message = %{
      event: event,
      task_id: task_id,
      at: DateTime.utc_now(),
      details: details
    }

    Enum.each(state.subscribers, fn {subscriber, _reference} ->
      send(subscriber, {:task_queue, state.entity, message})
    end)

    state
  end
end
