# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Permitir comunicação entre diferentes processos.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.Orders do
  @moduledoc """
  Gerenciamento de pedidos com persistência fictícia, retentativas e expiração.
  """

  use Supervisor

  alias Ecommerce.Orders.Manager
  alias Ecommerce.Orders.Store

  @type order_attrs :: %{
          required(:customer_id) => binary(),
          required(:items) => [map()]
        }

  def start_link(opts \\ []) do
    token = make_ref()
    Supervisor.start_link(__MODULE__, {token, opts}, Keyword.take(opts, [:name]))
  end

  @impl true
  def init({token, opts}) do
    store_name = {:global, {Store, token}}

    children = [
      {Store, name: store_name},
      {Manager,
       store: store_name,
       ttl_ms: Keyword.get(opts, :ttl_ms, 86_400_000),
       retry_delay_ms: Keyword.get(opts, :retry_delay_ms, 250),
       max_retries: Keyword.get(opts, :max_retries, 5)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec create_order(Supervisor.supervisor(), order_attrs()) ::
          {:ok, map()} | {:error, term()}
  def create_order(supervisor, attrs) do
    with {:ok, manager} <- child_pid(supervisor, Manager) do
      GenServer.call(manager, {:create_order, attrs})
    end
  end

  @spec get_order(Supervisor.supervisor(), binary()) :: {:ok, map()} | {:error, :not_found}
  def get_order(supervisor, order_id) do
    with {:ok, manager} <- child_pid(supervisor, Manager) do
      GenServer.call(manager, {:get_order, order_id})
    end
  end

  @spec get_persisted_order(Supervisor.supervisor(), binary()) ::
          {:ok, map()} | {:error, :not_found}
  def get_persisted_order(supervisor, order_id) do
    with {:ok, store} <- child_pid(supervisor, Store) do
      GenServer.call(store, {:get, order_id})
    end
  end

  @doc """
  Configura falhas temporárias simuladas no banco fictício.
  """
  def simulate_temporary_failures(supervisor, count)
      when is_integer(count) and count >= 0 do
    with {:ok, store} <- child_pid(supervisor, Store) do
      GenServer.call(store, {:fail_next, count})
    end
  end

  def simulate_temporary_failures(_supervisor, _count), do: {:error, :invalid_count}

  def validate_order(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    cond do
      not (is_binary(customer_id) and String.trim(customer_id) != "") ->
        {:error, {:invalid_field, :customer_id}}

      not (is_list(items) and items != []) ->
        {:error, {:invalid_field, :items}}

      not Enum.all?(items, &valid_item?/1) ->
        {:error, {:invalid_field, :items}}

      true ->
        {:ok, %{customer_id: customer_id, items: items}}
    end
  end

  def validate_order(_attrs), do: {:error, :invalid_order}

  defp valid_item?(item) when is_map(item) do
    product_id = Map.get(item, :product_id)
    quantity = Map.get(item, :quantity)
    unit_price_cents = Map.get(item, :unit_price_cents)

    is_binary(product_id) and String.trim(product_id) != "" and
      is_integer(quantity) and quantity > 0 and
      is_integer(unit_price_cents) and unit_price_cents >= 0
  end

  defp valid_item?(_item), do: false

  defp child_pid(supervisor, module) do
    case Enum.find(Supervisor.which_children(supervisor), fn
           {_id, pid, _type, [child_module]} -> child_module == module and is_pid(pid)
           _ -> false
         end) do
      {_id, pid, _type, _modules} -> {:ok, pid}
      nil -> {:error, :child_not_found}
    end
  end
end

defmodule Ecommerce.Orders.Store do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{}, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init(state), do: {:ok, %{records: state, failures_remaining: 0}}

  @impl true
  def handle_call({:fail_next, count}, _from, state) do
    {:reply, :ok, %{state | failures_remaining: count}}
  end

  def handle_call({:put, order, ttl_ms}, _from, %{failures_remaining: count} = state)
      when count > 0 do
    {:reply, {:error, :temporary_failure}, %{state | failures_remaining: count - 1}}
  end

  def handle_call({:put, order, ttl_ms}, _from, state) do
    now = System.monotonic_time(:millisecond)

    records =
      case Map.get(state.records, order.id) do
        %{deadline: deadline} = existing when deadline > now ->
          Map.put(state.records, order.id, %{existing | order: order})

        _expired_or_missing ->
          deadline = now + ttl_ms
          Process.send_after(self(), {:expire, order.id, deadline}, ttl_ms)

          Map.put(state.records, order.id, %{
            order: order,
            deadline: deadline
          })
      end

    {:reply, :ok, %{state | records: records}}
  end

  def handle_call({:get, order_id}, _from, state) do
    now = System.monotonic_time(:millisecond)

    case Map.get(state.records, order_id) do
      %{order: order, deadline: deadline} when deadline > now ->
        {:reply, {:ok, order}, state}

      %{deadline: deadline} ->
        {:reply, {:error, :not_found}, %{state | records: Map.delete(state.records, order_id)}}

      nil ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, order_id}, _from, state) do
    {:reply, :ok, %{state | records: Map.delete(state.records, order_id)}}
  end

  @impl true
  def handle_info({:expire, order_id, deadline}, state) do
    records =
      case Map.get(state.records, order_id) do
        %{deadline: ^deadline} -> Map.delete(state.records, order_id)
        _ -> state.records
      end

    {:noreply, %{state | records: records}}
  end
end

defmodule Ecommerce.Orders.Manager do
  @moduledoc false

  use GenServer

  alias Ecommerce.Orders
  alias Ecommerce.Orders.Store

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       store: Keyword.fetch!(opts, :store),
       ttl_ms: Keyword.fetch!(opts, :ttl_ms),
       retry_delay_ms: Keyword.fetch!(opts, :retry_delay_ms),
       max_retries: Keyword.fetch!(opts, :max_retries),
       orders: %{}
     }}
  end

  @impl true
  def handle_call({:create_order, attrs}, _from, state) do
    with {:ok, validated} <- Orders.validate_order(attrs),
         :ok <- validate_ttl(state.ttl_ms) do
      id = "order-#{System.unique_integer([:positive, :monotonic])}"
      now = DateTime.utc_now()

      order = %{
        id: id,
        customer_id: validated.customer_id,
        items: validated.items,
        total_cents: Enum.reduce(validated.items, 0, &(&1.quantity * &1.unit_price_cents + &2)),
        created_at: now,
        expires_at: DateTime.add(now, state.ttl_ms, :millisecond),
        persistence_status: :pending
      }

      expiry_timer = Process.send_after(self(), {:expire_order, id}, state.ttl_ms)
      entry = %{order: order, expiry_timer: expiry_timer, retry_token: nil, attempts: 0}

      {entry, state} =
        case persist(state.store, order, state.ttl_ms) do
          :ok ->
            updated = put_in(entry.order.persistence_status, :stored)
            {updated, put_in(state.orders[id], updated)}

          {:error, :temporary_failure} ->
            updated = schedule_retry(entry, id, 1, state.retry_delay_ms)
            {updated, put_in(state.orders[id], updated)}

          {:error, reason} ->
            updated = put_in(entry.order.persistence_status, {:failed, reason})
            {updated, put_in(state.orders[id], updated)}
        end

      {:reply, {:ok, entry.order}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_order, order_id}, _from, state) do
    case Map.get(state.orders, order_id) do
      %{order: order} -> {:reply, {:ok, order}, state}
      nil -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info({:retry_persist, order_id, attempt, token}, state) do
    case Map.get(state.orders, order_id) do
      %{retry_token: ^token, order: order} = entry ->
        case persist(state.store, order, state.ttl_ms) do
          :ok ->
            updated =
              entry
              |> Map.put(:retry_token, nil)
              |> put_in([:order, :persistence_status], :stored)

            {:noreply, put_in(state.orders[order_id], updated)}

          {:error, :temporary_failure} when attempt < state.max_retries ->
            updated = schedule_retry(entry, order_id, attempt + 1, state.retry_delay_ms)
            {:noreply, put_in(state.orders[order_id], updated)}

          {:error, :temporary_failure} ->
            updated =
              entry
              |> Map.put(:retry_token, nil)
              |> put_in([:order, :persistence_status], {:failed, :max_retries_exceeded})

            {:noreply, put_in(state.orders[order_id], updated)}

          {:error, reason} ->
            updated =
              entry
              |> Map.put(:retry_token, nil)
              |> put_in([:order, :persistence_status], {:failed, reason})

            {:noreply, put_in(state.orders[order_id], updated)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:expire_order, order_id}, state) do
    case Map.pop(state.orders, order_id) do
      {nil, _orders} ->
        {:noreply, state}

      {entry, orders} ->
        if entry.retry_token, do: Process.cancel_timer(entry.retry_token)
        GenServer.cast(state.store, {:delete, order_id})
        {:noreply, %{state | orders: orders}}
    end
  end

  defp persist(store, order, ttl_ms) do
    GenServer.call(store, {:put, order, ttl_ms})
  end

  defp schedule_retry(entry, order_id, attempt, delay_ms) do
    token = make_ref()
    Process.send_after(self(), {:retry_persist, order_id, attempt, token}, delay_ms)

    entry
    |> Map.put(:retry_token, token)
    |> Map.put(:attempts, attempt)
  end

  defp validate_ttl(ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0, do: :ok
  defp validate_ttl(_ttl_ms), do: {:error, :invalid_ttl}
end
