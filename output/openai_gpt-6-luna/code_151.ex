# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  @moduledoc """
  Gerenciador em memória de entregas e rastreamento.

  Cada operação exige uma chave de idempotência. O resultado da primeira
  execução é reutilizado em chamadas posteriores com a mesma chave. A
  idempotência e os registros são mantidos enquanto o processo estiver ativo.
  """

  use GenServer

  # Armazenamento em memória: não é necessário configurar um banco de dados.
  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    server_opts = Keyword.delete(opts, :max_concurrency)
    server_opts = Keyword.put_new(server_opts, :name, __MODULE__)

    GenServer.start_link(__MODULE__, max_concurrency, server_opts)
  end

  def execute(key, fun), do: execute(__MODULE__, key, fun)

  def execute(server, key, fun) when is_function(fun, 0) do
    GenServer.call(server, {:execute, key, fun}, :infinity)
  end

  def create(key, attrs), do: create(__MODULE__, key, attrs)

  def create(server, key, attrs) do
    execute(server, key, fn -> GenServer.call(server, {:create, attrs}) end)
  end

  def get(key, id), do: get(__MODULE__, key, id)

  def get(server, key, id) do
    execute(server, key, fn -> GenServer.call(server, {:get, id}) end)
  end

  def list(key), do: list(__MODULE__, key)

  def list(server, key) do
    execute(server, key, fn -> GenServer.call(server, :list) end)
  end

  def update(key, id, attrs), do: update(__MODULE__, key, id, attrs)

  def update(server, key, id, attrs) do
    execute(server, key, fn -> GenServer.call(server, {:update, id, attrs}) end)
  end

  def delete(key, id), do: delete(__MODULE__, key, id)

  def delete(server, key, id) do
    execute(server, key, fn -> GenServer.call(server, {:delete, id}) end)
  end

  def track(key, id, event), do: track(__MODULE__, key, id, event)

  def track(server, key, id, event) do
    execute(server, key, fn -> GenServer.call(server, {:track, id, event}) end)
  end

  @impl true
  def init(max_concurrency)
      when is_integer(max_concurrency) and max_concurrency > 0 do
    {:ok,
     %{
       max_concurrency: max_concurrency,
       deliveries: %{},
       operations: %{},
       queue: :queue.new(),
       active: %{}
     }}
  end

  def init(_), do: {:stop, :invalid_max_concurrency}

  @impl true
  def handle_call({:execute, key, fun}, from, state) do
    cond do
      is_nil(key) ->
        {:reply, {:error, :idempotency_key_required}, state}

      not is_function(fun, 0) ->
        {:reply, {:error, :invalid_operation}, state}

      match?(%{status: :done}, Map.get(state.operations, key)) ->
        {:reply, state.operations[key].result, state}

      match?(%{status: status} when status in [:queued, :running], Map.get(state.operations, key)) ->
        operation = state.operations[key]
        operation = %{operation | waiters: [from | operation.waiters]}
        {:noreply, put_in(state.operations[key], operation)}

      true ->
        operation = %{status: :queued, fun: fun, waiters: [from]}
        state = put_in(state.operations[key], operation)
        state = %{state | queue: :queue.in(key, state.queue)}
        {:noreply, schedule(state)}
    end
  end

  def handle_call({:create, attrs}, _from, state) when is_map(attrs) do
    id = Map.get(attrs, :id) || System.unique_integer([:positive])

    if Map.has_key?(state.deliveries, id) do
      {:reply, {:error, :already_exists}, state}
    else
      now = DateTime.utc_now()

      delivery =
        attrs
        |> Map.put(:id, id)
        |> Map.put_new(:status, :pending)
        |> Map.put_new(:tracking_events, [])
        |> Map.put(:inserted_at, now)
        |> Map.put(:updated_at, now)

      state = put_in(state.deliveries[id], delivery)
      {:reply, {:ok, delivery}, state}
    end
  end

  def handle_call({:create, _attrs}, _from, state),
    do: {:reply, {:error, :invalid_attributes}, state}

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.deliveries, id) do
      {:ok, delivery} -> {:reply, {:ok, delivery}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    {:reply, {:ok, Map.values(state.deliveries)}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) when is_map(attrs) do
    case Map.fetch(state.deliveries, id) do
      {:ok, delivery} ->
        updated =
          delivery
          |> Map.merge(Map.drop(attrs, [:id, :inserted_at, :tracking_events]))
          |> Map.put(:updated_at, DateTime.utc_now())

        {:reply, {:ok, updated}, put_in(state.deliveries[id], updated)}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:update, _id, _attrs}, _from, state),
    do: {:reply, {:error, :invalid_attributes}, state}

  def handle_call({:delete, id}, _from, state) do
    case Map.pop(state.deliveries, id) do
      {nil, _deliveries} ->
        {:reply, {:error, :not_found}, state}

      {delivery, deliveries} ->
        {:reply, {:ok, delivery}, %{state | deliveries: deliveries}}
    end
  end

  def handle_call({:track, id, event}, _from, state) when is_map(event) do
    case Map.fetch(state.deliveries, id) do
      {:ok, delivery} ->
        event = Map.put_new(event, :timestamp, DateTime.utc_now())

        updated =
          delivery
          |> Map.update!(:tracking_events, &(&1 ++ [event]))
          |> Map.put(:updated_at, DateTime.utc_now())
          |> maybe_update_status(event)

        {:reply, {:ok, updated}, put_in(state.deliveries[id], updated)}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:track, _id, _event}, _from, state),
    do: {:reply, {:error, :invalid_event}, state}

  @impl true
  def handle_info({:operation_result, key, result}, state) do
    case Map.get(state.operations, key) do
      %{status: :running, monitor: monitor} = operation ->
        Process.demonitor(monitor, [:flush])

        state = %{state | active: Map.delete(state.active, monitor)}
        state = finish_operation(state, key, operation, result)
        {:noreply, schedule(state)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.active, monitor) do
      {nil, _active} ->
        {:noreply, state}

      {key, active} ->
        operation = Map.fetch!(state.operations, key)
        result = {:error, {:operation_failed, reason}}
        state = %{state | active: active}
        state = finish_operation(state, key, operation, result)
        {:noreply, schedule(state)}
    end
  end

  defp schedule(state) when map_size(state.active) >= state.max_concurrency, do: state

  defp schedule(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, key}, queue} ->
        operation = Map.fetch!(state.operations, key)
        parent = self()

        {pid, monitor} =
          spawn_monitor(fn ->
            result = safely_apply(operation.fun)
            send(parent, {:operation_result, key, result})
          end)

        operation =
          operation
          |> Map.put(:status, :running)
          |> Map.put(:monitor, monitor)
          |> Map.put(:pid, pid)

        state = %{
          state
          | queue: queue,
            operations: Map.put(state.operations, key, operation),
            active: Map.put(state.active, monitor, key)
        }

        schedule(state)
    end
  end

  defp finish_operation(state, key, operation, result) do
    Enum.each(operation.waiters, &GenServer.reply(&1, result))
    put_in(state.operations[key], %{status: :done, result: result})
  end

  defp safely_apply(fun) do
    try do
      fun.()
    rescue
      error -> {:error, {:operation_failed, Exception.message(error)}}
    catch
      kind, reason -> {:error, {:operation_failed, {kind, reason}}}
    end
  end

  defp maybe_update_status(delivery, %{status: status}),
    do: Map.put(delivery, :status, status)

  defp maybe_update_status(delivery, _event), do: delivery
end
