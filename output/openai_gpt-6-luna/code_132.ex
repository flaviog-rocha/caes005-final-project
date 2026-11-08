# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Store.Retry do
  @moduledoc false

  def run(operation, attempts, delay_ms)
      when is_function(operation, 0) and is_integer(attempts) and attempts > 0 do
    case operation.() do
      {:error, :temporary} when attempts > 1 ->
        if delay_ms > 0, do: Process.sleep(delay_ms)
        run(operation, attempts - 1, delay_ms)

      result ->
        result
    end
  end
end

defmodule Store.Database do
  @moduledoc false
  use GenServer

  @default_products %{
    "coffee" => %{stock: 20, minimum: 5, reorder_quantity: 10},
    "tea" => %{stock: 15, minimum: 4, reorder_quantity: 8}
  }

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_all, do: GenServer.call(__MODULE__, :get_all)
  def put_all(products), do: GenServer.call(__MODULE__, {:put_all, products})
  def fail_next(calls), do: GenServer.call(__MODULE__, {:fail_next, calls})

  @impl true
  def init(opts) do
    products = Keyword.get(opts, :products, @default_products)
    failures = opts |> Keyword.get(:failures, 0) |> max(0)
    {:ok, %{products: products, failures: failures}}
  end

  @impl true
  def handle_call({:fail_next, calls}, _from, state) when is_integer(calls) and calls >= 0 do
    {:reply, :ok, %{state | failures: state.failures + calls}}
  end

  def handle_call(:get_all, _from, state) do
    if state.failures > 0 do
      {:reply, {:error, :temporary}, %{state | failures: state.failures - 1}}
    else
      {:reply, {:ok, state.products}, state}
    end
  end

  def handle_call({:put_all, products}, _from, state) do
    if state.failures > 0 do
      {:reply, {:error, :temporary}, %{state | failures: state.failures - 1}}
    else
      {:reply, :ok, %{state | products: products}}
    end
  end
end

defmodule Store.Inventory do
  @moduledoc false
  use GenServer
  require Logger

  alias Store.{Database, Retry}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def stock, do: GenServer.call(__MODULE__, :stock)
  def stock(product_id), do: GenServer.call(__MODULE__, {:stock, product_id})
  def sell(product_id, quantity), do: GenServer.call(__MODULE__, {:sell, product_id, quantity})
  def add_product(product_id, product), do: GenServer.call(__MODULE__, {:add_product, product_id, product})
  def restock_now, do: GenServer.call(__MODULE__, :restock)

  @impl true
  def init(opts) do
    attempts = Keyword.get(opts, :retry_attempts, 3)
    delay_ms = Keyword.get(opts, :retry_delay_ms, 100)

    case Retry.run(fn -> Database.get_all() end, attempts, delay_ms) do
      {:ok, products} ->
        interval = opts |> Keyword.get(:interval_ms, 60_000) |> max(1)
        schedule_restock(interval)

        {:ok,
         %{
           products: products,
           interval_ms: interval,
           retry_attempts: attempts,
           retry_delay_ms: delay_ms
         }}

      {:error, reason} ->
        {:stop, {:database_unavailable, reason}}
    end
  end

  @impl true
  def handle_call(:stock, _from, state) do
    {:reply, {:ok, state.products}, state}
  end

  def handle_call({:stock, product_id}, _from, state) do
    reply =
      case Map.fetch(state.products, product_id) do
        {:ok, product} -> {:ok, product}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:sell, product_id, quantity}, _from, state)
      when is_integer(quantity) and quantity > 0 do
    case Map.fetch(state.products, product_id) do
      {:ok, %{stock: current_stock} = product} when current_stock >= quantity ->
        updated_product = %{product | stock: current_stock - quantity}
        updated_products = Map.put(state.products, product_id, updated_product)

        case persist(updated_products, state) do
          :ok ->
            {:reply, {:ok, updated_product.stock}, %{state | products: updated_products}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:ok, _product} ->
        {:reply, {:error, :insufficient_stock}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:sell, _product_id, _quantity}, _from, state) do
    {:reply, {:error, :invalid_quantity}, state}
  end

  def handle_call({:add_product, product_id, product}, _from, state) do
    if valid_product?(product) do
      if Map.has_key?(state.products, product_id) do
        {:reply, {:error, :already_exists}, state}
      else
        updated_products = Map.put(state.products, product_id, product)

        case persist(updated_products, state) do
          :ok ->
            {:reply, :ok, %{state | products: updated_products}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
      end
    else
      {:reply, {:error, :invalid_product}, state}
    end
  end

  def handle_call(:restock, _from, state) do
    {result, updated_state} = do_restock(state)
    {:reply, result, updated_state}
  end

  @impl true
  def handle_info(:scheduled_restock, state) do
    case do_restock(state) do
      {{:error, reason}, updated_state} ->
        Logger.warning("A reposição periódica falhou: #{inspect(reason)}")
        schedule_restock(updated_state.interval_ms)
        {:noreply, updated_state}

      {_result, updated_state} ->
        schedule_restock(updated_state.interval_ms)
        {:noreply, updated_state}
    end
  end

  defp do_restock(state) do
    {updated_products, replenished} =
      Enum.reduce(state.products, {state.products, []}, fn {product_id, product},
                                                          {products, replenished} ->
        if product.stock <= product.minimum do
          updated_product = %{
            product
            | stock: product.stock + product.reorder_quantity
          }

          {
            Map.put(products, product_id, updated_product),
            [{product_id, updated_product.stock} | replenished]
          }
        else
          {products, replenished}
        end
      end)

    case replenished do
      [] ->
        {{:ok, []}, state}

      _ ->
        case persist(updated_products, state) do
          :ok ->
            {{:ok, Enum.reverse(replenished)}, %{state | products: updated_products}}

          {:error, reason} ->
            {{:error, reason}, state}
        end
    end
  end

  defp persist(products, state) do
    Retry.run(
      fn -> Database.put_all(products) end,
      state.retry_attempts,
      state.retry_delay_ms
    )
  end

  defp valid_product?(%{
         stock: stock,
         minimum: minimum,
         reorder_quantity: reorder_quantity
       }) do
    is_integer(stock) and stock >= 0 and
      is_integer(minimum) and minimum >= 0 and
      is_integer(reorder_quantity) and reorder_quantity > 0
  end

  defp valid_product?(_), do: false

  defp schedule_restock(interval_ms) do
    Process.send_after(self(), :scheduled_restock, interval_ms)
  end
end

defmodule Store do
  @moduledoc """
  Gerenciamento de estoque com reposição periódica e um banco de dados fictício.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def stock, do: Store.Inventory.stock()
  def stock(product_id), do: Store.Inventory.stock(product_id)
  def sell(product_id, quantity), do: Store.Inventory.sell(product_id, quantity)

  def add_product(product_id, product) do
    Store.Inventory.add_product(product_id, product)
  end

  def restock_now, do: Store.Inventory.restock_now()
  def fail_next_database_calls(calls), do: Store.Database.fail_next(calls)

  @impl true
  def init(opts) do
    database_opts = [
      products: Keyword.get(opts, :products, nil) || default_products(),
      failures: Keyword.get(opts, :database_failures, 0)
    ]

    inventory_opts = [
      interval_ms: Keyword.get(opts, :interval_ms, 60_000),
      retry_attempts: Keyword.get(opts, :retry_attempts, 3),
      retry_delay_ms: Keyword.get(opts, :retry_delay_ms, 100)
    ]

    children = [
      {Store.Database, database_opts},
      {Store.Inventory, inventory_opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp default_products do
    %{
      "coffee" => %{stock: 20, minimum: 5, reorder_quantity: 10},
      "tea" => %{stock: 15, minimum: 4, reorder_quantity: 8}
    }
  end
end
