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
  defmacro tracked(event, do: body) do
    quote do
      DeliveryTracker.log_event(unquote(event))
      unquote(body)
    end
  end
end

defmodule DeliveryTracker do
  use GenServer
  require Logger
  require DeliveryTracker.Macros

  defstruct max_concurrency: 4,
            next_id: 1,
            operations: %{},
            entity_operations: %{},
            history: [],
            queue: :queue.new(),
            running: %{}

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def submit(entity, payload, worker \\ nil) do
    submit_to(__MODULE__, entity, payload, worker)
  end

  def submit_to(server, entity, payload, worker \\ nil) do
    worker = worker || fn _payload -> :ok end
    GenServer.call(server, {:submit, entity, payload, worker})
  end

  def status(entity), do: status(__MODULE__, entity)

  def status(server, entity) do
    GenServer.call(server, {:status, entity})
  end

  def in_progress, do: in_progress(__MODULE__, :all)
  def in_progress(entity), do: in_progress(__MODULE__, entity)

  def in_progress(server, entity) do
    GenServer.call(server, {:in_progress, entity})
  end

  def history, do: history(__MODULE__, :all)
  def history(entity), do: history(__MODULE__, entity)

  def history(server, entity) do
    GenServer.call(server, {:history, entity})
  end

  def log_event(event) do
    Logger.info("Delivery tracker event: #{inspect(event)}")
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    {:ok, %__MODULE__{max_concurrency: max_concurrency}}
  end

  @impl true
  def handle_call({:submit, entity, payload, worker}, _from, state)
      when is_function(worker, 1) do
    id = state.next_id

    operation = %{
      id: id,
      entity: entity,
      payload: payload,
      status: :queued,
      result: nil,
      error: nil,
      worker: worker
    }

    state =
      state
      |> put_in([Access.key(:operations), id], operation)
      |> put_in(
        [Access.key(:entity_operations), entity],
        Map.get(state.entity_operations, entity, []) ++ [id]
      )
      |> Map.update!(:next_id, &(&1 + 1))
      |> Map.update!(:queue, &:queue.in(id, &1))
      |> record_event(id, :submitted, %{})
      |> dispatch()

    {:reply, id, state}
  end

  def handle_call({:status, entity}, _from, state) do
    operations =
      state.entity_operations
      |> Map.get(entity, [])
      |> Enum.map(&public_operation(Map.fetch!(state.operations, &1)))

    {:reply, operations, state}
  end

  def handle_call({:in_progress, entity}, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.filter(fn operation ->
        operation.status in [:queued, :running] and
          (entity == :all or operation.entity == entity)
      end)
      |> Enum.sort_by(& &1.id)
      |> Enum.map(&public_operation/1)

    {:reply, operations, state}
  end

  def handle_call({:history, entity}, _from, state) do
    events =
      state.history
      |> Enum.reverse()
      |> Enum.filter(fn event -> entity == :all or event.entity == entity end)

    {:reply, events, state}
  end

  @impl true
  def handle_info({ref, result}, state) when is_map_key(state.running, ref) do
    %{id: id} = Map.fetch!(state.running, ref)
    Process.demonitor(ref, [:flush])

    operation =
      state.operations
      |> Map.fetch!(id)
      |> Map.merge(%{status: :completed, result: result})

    state =
      state
      |> put_in([Access.key(:operations), id], operation)
      |> update_in([Access.key(:running)], &Map.delete(&1, ref))
      |> record_event(id, :completed, %{result: result})
      |> dispatch()

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.running, ref) do
    %{id: id} = Map.fetch!(state.running, ref)

    operation =
      state.operations
      |> Map.fetch!(id)
      |> Map.merge(%{status: :failed, error: reason})

    state =
      state
      |> put_in([Access.key(:operations), id], operation)
      |> update_in([Access.key(:running)], &Map.delete(&1, ref))
      |> record_event(id, :failed, %{reason: reason})
      |> dispatch()

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, id}, remaining_queue} ->
          operation = Map.fetch!(state.operations, id)
          worker = operation.worker
          payload = operation.payload

          task =
            Task.async_nolink(fn ->
              DeliveryTracker.Macros.tracked({:execute, id}, do: worker.(payload))
            end)

          operation = Map.put(operation, :status, :running)

          state =
            state
            |> Map.put(:queue, remaining_queue)
            |> put_in([Access.key(:operations), id], operation)
            |> update_in(
              [Access.key(:running)],
              &Map.put(&1, task.ref, %{id: id, pid: task.pid})
            )
            |> record_event(id, :started, %{})

          dispatch(state)

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp record_event(state, id, action, details) do
    operation = Map.fetch!(state.operations, id)

    event = %{
      id: id,
      entity: operation.entity,
      action: action,
      timestamp: System.system_time(:millisecond),
      details: details
    }

    Map.update!(state, :history, &[event | &1])
  end

  defp public_operation(operation) do
    Map.drop(operation, [:worker])
  end
end
