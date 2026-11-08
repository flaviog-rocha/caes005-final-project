# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker.Macros do
  @moduledoc false

  defmacro log_transition(entity_id, operation_id, status) do
    quote do
      Logger.info(
        "Delivery operation #{inspect(unquote(operation_id))} for entity " <>
          "#{inspect(unquote(entity_id))}: #{unquote(status)}"
      )
    end
  end
end

defmodule DeliveryTracker do
  use GenServer
  require Logger
  require DeliveryTracker.Macros

  @moduledoc """
  Gerencia operações de entrega, mantendo seu estado atual e histórico.
  """

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Enfileira uma operação de aridade zero associada a uma entidade.
  """
  def submit(server, entity_id, operation, metadata \\ %{})
      when is_function(operation, 0) do
    GenServer.call(server, {:submit, entity_id, operation, metadata})
  end

  def submit(_server, _entity_id, _operation, _metadata), do: {:error, :invalid_operation}

  @doc "Retorna as operações enfileiradas ou em execução."
  def current(server, entity_id \\ :all) do
    GenServer.call(server, {:current, entity_id})
  end

  @doc "Retorna somente as operações em execução."
  def in_progress(server, entity_id \\ :all) do
    GenServer.call(server, {:in_progress, entity_id})
  end

  @doc "Consulta uma operação pelo identificador."
  def operation(server, operation_id) do
    GenServer.call(server, {:operation, operation_id})
  end

  @doc "Retorna o histórico em ordem cronológica."
  def history(server) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(opts) do
    limit = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if is_integer(limit) and limit > 0 do
      case Task.Supervisor.start_link() do
        {:ok, task_supervisor} ->
          {:ok,
           %{
             max_concurrency: limit,
             task_supervisor: task_supervisor,
             queue: :queue.new(),
             running: %{},
             operations: %{},
             history: []
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, {:invalid_max_concurrency, limit}}
    end
  end

  @impl true
  def handle_call({:submit, entity_id, operation, metadata}, _from, state) do
    operation_id = make_ref()
    submitted_at = DateTime.utc_now()

    job = %{
      id: operation_id,
      entity_id: entity_id,
      operation: operation,
      metadata: metadata,
      submitted_at: submitted_at
    }

    record = %{
      operation_id: operation_id,
      entity_id: entity_id,
      status: :queued,
      metadata: metadata,
      submitted_at: submitted_at,
      started_at: nil,
      completed_at: nil
    }

    state =
      state
      |> put_in([:operations, operation_id], record)
      |> Map.update!(:queue, &:queue.in(job, &1))
      |> add_event(:submitted, job, %{metadata: metadata})
      |> dispatch()

    {:reply, {:ok, operation_id}, state}
  end

  def handle_call({:current, entity_id}, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running]))
      |> filter_entity(entity_id)
      |> sort_operations()

    {:reply, operations, state}
  end

  def handle_call({:in_progress, entity_id}, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status == :running))
      |> filter_entity(entity_id)
      |> sort_operations()

    {:reply, operations, state}
  end

  def handle_call({:operation, operation_id}, _from, state) do
    {:reply, Map.fetch(state.operations, operation_id), state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info({reference, result}, state) when is_reference(reference) do
    case Map.pop(state.running, reference) do
      {nil, _running} ->
        {:noreply, state}

      {job, running} ->
        Process.demonitor(reference, [:flush])

        state =
          %{state | running: running}
          |> finish_operation(job, :completed, %{result: result})
          |> dispatch()

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, reference, :process, _pid, reason}, state) do
    case Map.pop(state.running, reference) do
      {nil, _running} ->
        {:noreply, state}

      {job, running} ->
        state =
          %{state | running: running}
          |> finish_operation(job, :failed, %{error: inspect(reason)})
          |> dispatch()

        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, job}, queue} ->
          task =
            Task.Supervisor.async_nolink(state.task_supervisor, fn ->
              job.operation.()
            end)

          started_at = DateTime.utc_now()

          running_job = Map.put(job, :started_at, started_at)

          record =
            state.operations
            |> Map.fetch!(job.id)
            |> Map.merge(%{status: :running, started_at: started_at})

          state =
            %{state | queue: queue}
            |> put_in([:running, task.ref], running_job)
            |> put_in([:operations, job.id], record)
            |> add_event(:started, running_job, %{})

          dispatch(state)

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp finish_operation(state, job, status, details) do
    completed_at = DateTime.utc_now()

    record =
      state.operations
      |> Map.fetch!(job.id)
      |> Map.merge(%{status: status, completed_at: completed_at})
      |> Map.merge(details)

    state
    |> put_in([:operations, job.id], record)
    |> add_event(status, job, Map.merge(details, %{completed_at: completed_at}))
  end

  defp add_event(state, type, job, details) do
    event =
      Map.merge(
        %{
          type: type,
          operation_id: job.id,
          entity_id: job.entity_id,
          timestamp: DateTime.utc_now()
        },
        details
      )

    DeliveryTracker.Macros.log_transition(job.entity_id, job.id, type)
    %{state | history: [event | state.history]}
  end

  defp filter_entity(operations, :all), do: operations
  defp filter_entity(operations, entity_id), do: Enum.filter(operations, &(&1.entity_id == entity_id))

  defp sort_operations(operations) do
    Enum.sort_by(operations, &DateTime.to_unix(&1.submitted_at, :microsecond))
  end
end
