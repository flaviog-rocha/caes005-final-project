# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Store.Inventory.Product do
  @moduledoc false

  defstruct sku: nil,
            quantity: 0,
            reorder_level: 0,
            reorder_requested: false

  def validate(%__MODULE__{} = product) do
    cond do
      not (is_binary(product.sku) and byte_size(product.sku) > 0) ->
        {:error, {:validation, :invalid_sku}}

      not (is_integer(product.quantity) and product.quantity >= 0) ->
        {:error, {:validation, :invalid_quantity}}

      not (is_integer(product.reorder_level) and product.reorder_level >= 0) ->
        {:error, {:validation, :invalid_reorder_level}}

      not is_boolean(product.reorder_requested) ->
        {:error, {:validation, :invalid_reorder_status}}

      true ->
        :ok
    end
  end

  def validate(_), do: {:error, {:validation, :invalid_product}}
end

defmodule Store.Inventory.Database do
  @moduledoc false
  use GenServer

  alias Store.Inventory.Product

  @operations [:insert, :get, :list, :adjust, :set_reorder]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def insert(database, product), do: GenServer.call(database, {:insert, product})
  def get(database, sku), do: GenServer.call(database, {:get, sku})
  def list(database), do: GenServer.call(database, :list)
  def adjust(database, sku, delta), do: GenServer.call(database, {:adjust, sku, delta})
  def set_reorder(database, sku, requested), do: GenServer.call(database, {:set_reorder, sku, requested})
  def fail_next(database, operation), do: GenServer.call(database, {:fail_next, operation})

  @impl true
  def init(opts) do
    products = Keyword.get(opts, :products, [])

    case seed_products(products) do
      {:ok, seeded} -> {:ok, %{products: seeded, failures: []}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:fail_next, operation}, _from, state) do
    if operation in @operations do
      {:reply, :ok, %{state | failures: state.failures ++ [operation]}}
    else
      {:reply, {:error, {:validation, :invalid_operation}}, state}
    end
  end

  def handle_call({:insert, product}, _from, state) do
    with :ok <- Product.validate(product),
         false <- Map.has_key?(state.products, product.sku) do
      case maybe_fail(state, :insert) do
        {:failed, reply, updated_state} ->
          {:reply, reply, updated_state}

        {:ok, updated_state} ->
          products = Map.put(updated_state.products, product.sku, product)
          {:reply, {:ok, product}, %{updated_state | products: products}}
      end
    else
      true -> {:reply, {:error, {:validation, :sku_already_exists}}, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:get, sku}, _from, state) do
    case maybe_fail(state, :get) do
      {:failed, reply, updated_state} ->
        {:reply, reply, updated_state}

      {:ok, updated_state} ->
        case Map.fetch(updated_state.products, sku) do
          {:ok, product} ->
            {:reply, {:ok, product}, updated_state}

          :error ->
            {:reply, {:error, {:operational, {:product_not_found, sku}}}, updated_state}
        end
    end
  end

  def handle_call(:list, _from, state) do
    case maybe_fail(state, :list) do
      {:failed, reply, updated_state} ->
        {:reply, reply, updated_state}

      {:ok, updated_state} ->
        {:reply, {:ok, Map.values(updated_state.products)}, updated_state}
    end
  end

  def handle_call({:adjust, sku, delta}, _from, state) do
    case maybe_fail(state, :adjust) do
      {:failed, reply, updated_state} ->
        {:reply, reply, updated_state}

      {:ok, updated_state} ->
        case Map.fetch(updated_state.products, sku) do
          {:ok, product} ->
            new_quantity = product.quantity + delta

            if new_quantity < 0 do
              {:reply, {:error, {:validation, :insufficient_stock}}, updated_state}
            else
              updated_product = %{product | quantity: new_quantity}
              products = Map.put(updated_state.products, sku, updated_product)
              {:reply, {:ok, updated_product}, %{updated_state | products: products}}
            end

          :error ->
            {:reply, {:error, {:operational, {:product_not_found, sku}}}, updated_state}
        end
    end
  end

  def handle_call({:set_reorder, sku, requested}, _from, state) do
    case maybe_fail(state, :set_reorder) do
      {:failed, reply, updated_state} ->
        {:reply, reply, updated_state}

      {:ok, updated_state} ->
        case Map.fetch(updated_state.products, sku) do
          {:ok, product} ->
            updated_product = %{product | reorder_requested: requested}
            products = Map.put(updated_state.products, sku, updated_product)
            {:reply, {:ok, updated_product}, %{updated_state | products: products}}

          :error ->
            {:reply, {:error, {:operational, {:product_not_found, sku}}}, updated_state}
        end
    end
  end

  defp seed_products(products) when is_list(products) do
    Enum.reduce_while(products, {:ok, %{}}, fn product, {:ok, acc} ->
      with :ok <- Product.validate(product),
           false <- Map.has_key?(acc, product.sku) do
        {:cont, {:ok, Map.put(acc, product.sku, product)}}
      else
        true -> {:halt, {:error, {:validation, :sku_already_exists}}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp seed_products(_), do: {:error, {:validation, :invalid_products}}

  defp maybe_fail(state, operation) do
    if operation in state.failures do
      failures = List.delete(state.failures, operation)

      {:failed,
       {:error, {:operational, {:database_unavailable, operation}}},
       %{state | failures: failures}}
    else
      {:ok, state}
    end
  end
end

defmodule Store.Inventory do
  @moduledoc """
  Gerencia produtos e executa periodicamente uma verificação de reposição.

  Erros de entrada e de domínio são retornados como
  `{:error, {:validation, motivo}}`. Falhas de banco de dados e produtos
  inexistentes são retornados como `{:error, {:operational, motivo}}`.
  """

  use GenServer

  alias Store.Inventory.{Database, Product}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def add_product(server, sku, quantity, reorder_level) do
    GenServer.call(server, {:add_product, %Product{
      sku: sku,
      quantity: quantity,
      reorder_level: reorder_level
    }})
  end

  def adjust_stock(server, sku, delta), do: GenServer.call(server, {:adjust_stock, sku, delta})
  def get_product(server, sku), do: GenServer.call(server, {:get_product, sku})
  def run_cycle(server), do: GenServer.call(server, :run_cycle)
  def fail_next_database_operation(server, operation), do: GenServer.call(server, {:fail_next, operation})

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    cond do
      not (is_integer(interval) and interval > 0) ->
        {:stop, {:validation, :invalid_interval}}

      not (is_integer(concurrency) and concurrency > 0) ->
        {:stop, {:validation, :invalid_max_concurrency}}

      true ->
        case Database.start_link(products: Keyword.get(opts, :products, [])) do
          {:ok, database} ->
            Process.send_after(self(), :tick, interval)

            {:ok, %{
              database: database,
              interval: interval,
              max_concurrency: concurrency,
              cycle_ref: nil
            }}

          {:error, reason} ->
            {:stop, reason}
        end
    end
  end

  @impl true
  def handle_call({:add_product, product}, _from, state) do
    reply =
      with :ok <- Product.validate(product) do
        Database.insert(state.database, product)
      end

    {:reply, reply, state}
  end

  def handle_call({:adjust_stock, sku, delta}, _from, state) do
    reply =
      cond do
        not valid_sku?(sku) ->
          {:error, {:validation, :invalid_sku}}

        not is_integer(delta) ->
          {:error, {:validation, :invalid_stock_adjustment}}

        true ->
          Database.adjust(state.database, sku, delta)
      end

    {:reply, reply, state}
  end

  def handle_call({:get_product, sku}, _from, state) do
    reply =
      if valid_sku?(sku) do
        Database.get(state.database, sku)
      else
        {:error, {:validation, :invalid_sku}}
      end

    {:reply, reply, state}
  end

  def handle_call({:fail_next, operation}, _from, state) do
    {:reply, Database.fail_next(state.database, operation), state}
  end

  def handle_call(:run_cycle, _from, state) do
    {:reply, safe_cycle(state.database, state.max_concurrency), state}
  end

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, state.interval)

    if state.cycle_ref == nil do
      ref = make_ref()
      parent = self()
      database = state.database
      concurrency = state.max_concurrency

      Task.start(fn ->
        result = safe_cycle(database, concurrency)
        send(parent, {:cycle_complete, ref, result})
      end)

      {:noreply, %{state | cycle_ref: ref}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:cycle_complete, ref, _result}, %{cycle_ref: ref} = state) do
    {:noreply, %{state | cycle_ref: nil}}
  end

  def handle_info({:cycle_complete, _ref, _result}, state), do: {:noreply, state}

  defp safe_cycle(database, concurrency) do
    cycle(database, concurrency)
  rescue
    error ->
      {:error, {:operational, {:cycle_failed, Exception.message(error)}}}
  catch
    :exit, reason -> {:error, {:operational, {:cycle_failed, reason}}}
    kind, reason -> {:error, {:operational, {:cycle_failed, {kind, reason}}}}
  end

  defp cycle(database, concurrency) do
    case safe_database_call(:list_products, fn -> Database.list(database) end) do
      {:ok, products} ->
        skus = Enum.map(products, & &1.sku)

        outcomes =
          skus
          |> Task.async_stream(
            fn sku -> process_product(database, sku) end,
            max_concurrency: concurrency,
            timeout: :infinity,
            ordered: true
          )
          |> Enum.zip(skus)
          |> Enum.map(fn
            {{:ok, {sku, result}}, _expected_sku} ->
              {sku, result}

            {{:exit, reason}, sku} ->
              {sku, {:error, {:operational, {:worker_failed, reason}}}}
          end)
          |> Map.new()

        {:ok, outcomes}

      {:error, _} = error ->
        error
    end
  end

  defp process_product(database, sku) do
    result =
      with {:ok, product} <- safe_database_call(:get_product, fn -> Database.get(database, sku) end),
           requested = product.quantity <= product.reorder_level,
           {:ok, updated_product} <-
             safe_database_call(:set_reorder, fn ->
               Database.set_reorder(database, sku, requested)
             end) do
        {:ok, updated_product}
      end

    {sku, result}
  rescue
    error ->
      {sku, {:error, {:operational, {:worker_failed, Exception.message(error)}}}}
  catch
    :exit, reason -> {sku, {:error, {:operational, {:worker_failed, reason}}}}
    kind, reason -> {sku, {:error, {:operational, {:worker_failed, {kind, reason}}}}}
  end

  defp safe_database_call(operation, function) do
    function.()
  rescue
    error ->
      {:error, {:operational, {:database_call_failed, operation, Exception.message(error)}}}
  catch
    :exit, reason ->
      {:error, {:operational, {:database_call_failed, operation, reason}}}

    kind, reason ->
      {:error, {:operational, {:database_call_failed, operation, {kind, reason}}}}
  end

  defp valid_sku?(sku), do: is_binary(sku) and byte_size(sku) > 0
end
