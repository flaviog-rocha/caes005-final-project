# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager.Store do
  @moduledoc false
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def execute(store, operation) do
    GenServer.call(store, {:execute, operation}, :infinity)
  end

  @impl true
  def init(deliveries), do: {:ok, deliveries}

  @impl true
  def handle_call({:execute, operation}, _from, deliveries) do
    {result, updated_deliveries} = execute_operation(operation, deliveries)
    {:reply, result, updated_deliveries}
  end

  defp execute_operation({:create, attrs}, deliveries) when is_map(attrs) do
    case Map.get(attrs, :id) do
      nil ->
        {{:error, :id_required}, deliveries}

      id ->
        if Map.has_key?(deliveries, id) do
          {{:error, :already_exists}, deliveries}
        else
          delivery =
            attrs
            |> Map.put(:id, id)
            |> Map.put_new(:status, :pending)
            |> Map.put(:tracking, [])

          {{:ok, delivery}, Map.put(deliveries, id, delivery)}
        end
    end
  end

  defp execute_operation({:get, id}, deliveries) do
    case Map.fetch(deliveries, id) do
      {:ok, delivery} -> {{:ok, delivery}, deliveries}
      :error -> {{:error, :not_found}, deliveries}
    end
  end

  defp execute_operation({:update, id, attrs}, deliveries) when is_map(attrs) do
    case Map.fetch(deliveries, id) do
      {:ok, delivery} ->
        safe_attrs = Map.drop(attrs, [:id, :tracking])
        updated = Map.merge(delivery, safe_attrs)
        {{:ok, updated}, Map.put(deliveries, id, updated)}

      :error ->
        {{:error, :not_found}, deliveries}
    end
  end

  defp execute_operation({:delete, id}, deliveries) do
    case Map.pop(deliveries, id) do
      {nil, _deliveries} -> {{:error, :not_found}, deliveries}
      {delivery, updated_deliveries} -> {{:ok, delivery}, updated_deliveries}
    end
  end

  defp execute_operation({:track, id, event}, deliveries) when is_map(event) do
    case Map.fetch(deliveries, id) do
      {:ok, delivery} ->
        event = Map.put_new(event, :at, DateTime.utc_now())
        updated = Map.update!(delivery, :tracking, &(&1 ++ [event]))
        {{:ok, updated}, Map.put(deliveries, id, updated)}

      :error ->
        {{:error, :not_found}, deliveries}
    end
  end

  defp execute_operation(_operation, deliveries) do
    {{:error, :invalid_operation}, deliveries}
  end
end

defmodule DeliveryManager do
  @moduledoc """
  Gerencia entregas em memória, com operações idempotentes e limite de concorrência.
  """

  use GenServer

  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      GenServer.start_link(
        __MODULE__,
        max_concurrency,
        Keyword.take(opts, [:name])
      )
    else
      {:error, :invalid_max_concurrency}
    end
  end

  @doc """
  Executa uma operação usando uma chave de idempotência.

  Repetir a mesma chave e operação retorna o resultado anterior. Reutilizar
  a chave para outra operação retorna `{:error, :idempotency_key_conflict}`.
  """
  def submit(server, operation_id, operation) do
    GenServer.call(server, {:submit, operation_id, operation}, :infinity)
  end

  def create(server, operation_id, attrs),
    do: submit(server, operation_id, {:create, attrs})

  def get(server, operation_id, id),
    do: submit(server, operation_id, {:get, id})

  def update(server, operation_id, id, attrs),
    do: submit(server, operation_id, {:update, id, attrs})

  def delete(server, operation_id, id),
    do: submit(server, operation_id, {:delete, id})

  def track(server, operation_id, id, event),
    do: submit(server, operation_id, {:track, id, event})

  @impl true
  def init(max_concurrency) do
    with {:ok, store} <- DeliveryManager.Store.start_link() do
      {:ok,
       %{
         store: store,
         max_concurrency: max_concurrency,
         active: 0,
         queue: :queue.new(),
         pending: %{},
         completed: %{}
       }}
    end
  end

  @impl true
  def handle_call({:submit, operation_id, operation}, from, state) do
    cond do
      is_nil(operation_id) ->
        {:reply, {:error, :idempotency_key_required}, state}

      Map.has_key?(state.completed, operation_id) ->
        case Map.fetch!(state.completed, operation_id) do
          {^operation, result} -> {:reply, result, state}
          {_previous_operation, _result} ->
            {:reply, {:error, :idempotency_key_conflict}, state}
        end

      Map.has_key?(state.pending, operation_id) ->
        pending = Map.fetch!(state.pending, operation_id)

        if pending.operation == operation do
          updated = put_in(state.pending[operation_id].waiters, [from | pending.waiters])
          {:noreply, updated}
        else
          {:reply, {:error, :idempotency_key_conflict}, state}
        end

      true ->
        pending = %{operation: operation, waiters: [from]}

        updated =
          state
          |> put_in([:pending, operation_id], pending)
          |> update_in([:queue], &:queue.in(operation_id, &1))

        {:noreply, dispatch(updated)}
    end
  end

  @impl true
  def handle_info({:operation_done, operation_id, result}, state) do
    case Map.pop(state.pending, operation_id) do
      {nil, _pending} ->
        {:noreply, state}

      {pending, remaining_pending} ->
        Enum.each(pending.waiters, &GenServer.reply(&1, result))

        updated = %{
          state
          | pending: remaining_pending,
            completed:
              Map.put(state.completed, operation_id, {pending.operation, result}),
            active: max(state.active - 1, 0)
        }

        {:noreply, dispatch(updated)}
    end
  end

  defp dispatch(state) when state.active >= state.max_concurrency, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, operation_id}, queue} ->
        pending = Map.fetch!(state.pending, operation_id)
        store = state.store
        manager = self()

        case Task.start(fn ->
               result = safely_execute(store, pending.operation)
               send(manager, {:operation_done, operation_id, result})
             end) do
          {:ok, _pid} ->
            state
            |> Map.put(:queue, queue)
            |> Map.update!(:active, &(&1 + 1))
            |> dispatch()

          {:error, reason} ->
            Enum.each(pending.waiters, &GenServer.reply(&1, {:error, reason}))

            updated = %{
              state
              | queue: queue,
                pending: Map.delete(state.pending, operation_id),
                completed:
                  Map.put(
                    state.completed,
                    operation_id,
                    {pending.operation, {:error, reason}}
                  )
            }

            dispatch(updated)
        end
    end
  end

  defp safely_execute(store, operation) do
    DeliveryManager.Store.execute(store, operation)
  rescue
    error -> {:error, {:operation_failed, error}}
  catch
    kind, reason -> {:error, {:operation_failed, {kind, reason}}}
  end
end
