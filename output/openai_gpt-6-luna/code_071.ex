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

defmodule Ecommerce.Database do
  use GenServer

  # Banco de dados fictício em memória. Também expira os registros persistidos.
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def save(order), do: GenServer.call(__MODULE__, {:save, order})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list, do: GenServer.call(__MODULE__, :list)

  # Permite simular falhas temporárias para testar as novas tentativas.
  def fail_next(count) when is_integer(count) and count >= 0 do
    GenServer.call(__MODULE__, {:fail_next, count})
  end

  def fail_next(_count), do: {:error, :invalid_count}

  @impl true
  def init(_state) do
    {:ok, %{records: %{}, timers: %{}, transient_failures: 0}}
  end

  @impl true
  def handle_call({:save, order}, _from, state) do
    cond do
      state.transient_failures > 0 ->
        {:reply, {:error, :temporary}, %{state | transient_failures: state.transient_failures - 1}}

      order.expires_at <= System.system_time(:millisecond) ->
        {:reply, {:error, :expired}, state}

      true ->
        state = remove_record(state, order.id)
        token = make_ref()
        delay = max(order.expires_at - System.system_time(:millisecond), 0)
        timer = Process.send_after(self(), {:expire, order.id, token}, delay)

        next_state = %{
          state
          | records: Map.put(state.records, order.id, order),
            timers: Map.put(state.timers, order.id, {timer, token})
        }

        {:reply, :ok, next_state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.records, id) do
      {:ok, order} ->
        if order.expires_at <= System.system_time(:millisecond) do
          {:reply, {:error, :not_found}, remove_record(state, id)}
        else
          {:reply, {:ok, order}, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    now = System.system_time(:millisecond)

    {records, expired_ids} =
      Enum.split_with(state.records, fn {_id, order} -> order.expires_at > now end)

    next_state = Enum.reduce(expired_ids, state, fn {id, _order}, acc ->
      remove_record(acc, id)
    end)

    {:reply, Enum.map(records, fn {_id, order} -> order end), next_state}
  end

  def handle_call({:fail_next, count}, _from, state) do
    {:reply, :ok, %{state | transient_failures: count}}
  end

  @impl true
  def handle_cast({:delete, id}, state) do
    {:noreply, remove_record(state, id)}
  end

  @impl true
  def handle_info({:expire, id, token}, state) do
    case Map.get(state.timers, id) do
      {_timer, ^token} ->
        case Map.get(state.records, id) do
          %{expires_at: expires_at} when expires_at <= System.system_time(:millisecond) ->
            {:noreply, remove_record(state, id)}

          %{expires_at: expires_at} ->
            timer =
              Process.send_after(
                self(),
                {:expire, id, token},
                max(expires_at - System.system_time(:millisecond), 0)
              )

            {:noreply, %{state | timers: Map.put(state.timers, id, {timer, token})}}

          nil ->
            {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  defp remove_record(state, id) do
    timers =
      case Map.pop(state.timers, id) do
        {nil, timers} ->
          timers

        {{timer, _token}, timers} ->
          Process.cancel_timer(timer)
          timers
      end

    %{state | records: Map.delete(state.records, id), timers: timers}
  end
end

defmodule Ecommerce.OrderManager do
  use GenServer

  @default_ttl_ms 86_400_000
  @default_retry_base_ms 100
  @default_max_retries 3

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_order(attrs) do
    with {:ok, normalized} <- validate_order(attrs) do
      GenServer.call(__MODULE__, {:create, normalized})
    end
  end

  def get_order(id) when is_integer(id), do: GenServer.call(__MODULE__, {:get, id})
  def get_order(_id), do: {:error, :invalid_id}

  def list_orders, do: GenServer.call(__MODULE__, :list)

  def validate_order(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    cond do
      not valid_text?(customer_id) ->
        {:error, {:invalid_field, :customer_id}}

      not is_list(items) or items == [] ->
        {:error, {:invalid_field, :items}}

      true ->
        case validate_items(items) do
          :ok -> {:ok, %{customer_id: String.trim(customer_id), items: items}}
          error -> error
        end
    end
  end

  def validate_order(_attrs), do: {:error, :invalid_order}

  defp validate_items(items) do
    if Enum.all?(items, fn
         %{sku: sku, quantity: quantity} ->
           valid_text?(sku) and is_integer(quantity) and quantity > 0

         _ ->
           false
       end) do
      :ok
    else
      {:error, {:invalid_field, :items}}
    end
  end

  defp valid_text?(value) do
    is_binary(value) and String.trim(value) != ""
  end

  @impl true
  def init(opts) do
    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    retry_base_ms = Keyword.get(opts, :retry_base_ms, @default_retry_base_ms)
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)

    unless is_integer(ttl_ms) and ttl_ms > 0 and
             is_integer(retry_base_ms) and retry_base_ms >= 0 and
             is_integer(max_retries) and max_retries >= 0 do
      raise ArgumentError, "ttl_ms, retry_base_ms ou max_retries inválido"
    end

    {:ok,
     %{
       orders: %{},
       ttl_ms: ttl_ms,
       retry_base_ms: retry_base_ms,
       max_retries: max_retries
     }}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])
    now = System.system_time(:millisecond)
    token = make_ref()

    order = %{
      id: id,
      customer_id: attrs.customer_id,
      items: attrs.items,
      status: :pending,
      attempts: 0,
      created_at: now,
      expires_at: now + state.ttl_ms
    }

    entry = %{
      order: order,
      deadline: System.monotonic_time(:millisecond) + state.ttl_ms,
      token: token
    }

    Process.send_after(self(), {:expire, id, token}, state.ttl_ms)
    send(self(), {:process_order, id, token})

    next_state = %{state | orders: Map.put(state.orders, id, entry)}
    {:reply, {:ok, order}, next_state}
  end

  def handle_call({:get, id}, _from, state) do
    case Map.get(state.orders, id) do
      %{order: order} -> {:reply, {:ok, order}, state}
      nil -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    orders =
      state.orders
      |> Map.values()
      |> Enum.map(& &1.order)
      |> Enum.sort_by(& &1.id)

    {:reply, orders, state}
  end

  @impl true
  def handle_info({:process_order, id, token}, state) do
    case Map.get(state.orders, id) do
      %{token: ^token, deadline: deadline} when deadline <= System.monotonic_time(:millisecond) ->
        {:noreply, expire_order(state, id, token)}

      %{token: ^token, order: order} = entry ->
        attempt = order.attempts + 1
        processing_order = %{order | status: :processing, attempts: attempt}
        entry = %{entry | order: processing_order}
        state = %{state | orders: Map.put(state.orders, id, entry)}

        case Ecommerce.Database.save(%{processing_order | status: :completed}) do
          :ok ->
            completed_order = %{processing_order | status: :completed}
            updated_entry = %{entry | order: completed_order}
            {:noreply, %{state | orders: Map.put(state.orders, id, updated_entry)}}

          {:error, :temporary} when attempt <= state.max_retries ->
            retry_order(state, id, token, entry, attempt, :temporary)

          {:error, reason} ->
            failed_order = %{processing_order | status: :failed, last_error: reason}
            updated_entry = %{entry | order: failed_order}
            {:noreply, %{state | orders: Map.put(state.orders, id, updated_entry)}}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:expire, id, token}, state) do
    case Map.get(state.orders, id) do
      %{token: ^token, deadline: deadline} when deadline <= System.monotonic_time(:millisecond) ->
        {:noreply, expire_order(state, id, token)}

      %{token: ^token, deadline: deadline} ->
        delay = max(deadline - System.monotonic_time(:millisecond), 0)
        Process.send_after(self(), {:expire, id, token}, delay)
        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  defp retry_order(state, id, token, entry, attempt, reason) do
    delay = state.retry_base_ms * :erlang.bsl(1, min(attempt - 1, 10))
    retrying_order = %{entry.order | status: :retrying, last_error: reason}
    updated_entry = %{entry | order: retrying_order}

    Process.send_after(self(), {:process_order, id, token}, delay)

    {:noreply, %{state | orders: Map.put(state.orders, id, updated_entry)}}
  end

  defp expire_order(state, id, token) do
    case Map.get(state.orders, id) do
      %{token: ^token} ->
        GenServer.cast(Ecommerce.Database, {:delete, id})
        %{state | orders: Map.delete(state.orders, id)}

      _ ->
        state
    end
  end
end

defmodule Ecommerce.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Ecommerce.Database, []},
      {Ecommerce.OrderManager, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Ecommerce do
  def start_link(opts \\ []), do: Ecommerce.Supervisor.start_link(opts)

  def create_order(attrs), do: Ecommerce.OrderManager.create_order(attrs)
  def get_order(id), do: Ecommerce.OrderManager.get_order(id)
  def list_orders, do: Ecommerce.OrderManager.list_orders()

  def database_order(id), do: Ecommerce.Database.get(id)
  def database_orders, do: Ecommerce.Database.list()
  def simulate_temporary_failures(count), do: Ecommerce.Database.fail_next(count)
end
