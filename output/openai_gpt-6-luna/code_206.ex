# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Inventory do
  @moduledoc """
  Gerenciamento de estoque com processos independentes por produto e persistência
  periódica em um banco de dados fictício.
  """

  use Supervisor

  alias Inventory.FakeDB
  alias Inventory.Product

  @default_checkpoint_ms 60_000

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__.Supervisor)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Inventory.Registry},
      {FakeDB, name: Inventory.Database},
      {DynamicSupervisor, strategy: :one_for_one, name: Inventory.Products}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Cria um produto. `checkpoint_ms` define o intervalo de persistência periódica."
  def create_product(attrs, opts \\ []) do
    with {:ok, product} <- validate_product(attrs),
         {:ok, checkpoint_ms} <- validate_options(opts),
         :ok <- insert_product(product) do
      case DynamicSupervisor.start_child(
             Inventory.Products,
             {Product, {product, checkpoint_ms}}
           ) do
        {:ok, _pid} ->
          {:ok, product}

        {:error, reason} ->
          _ = db_call(fn -> FakeDB.delete(product.sku) end)
          {:error, {:operational, {:start_product, reason}}}
      end
    end
  end

  @doc "Retorna o produto armazenado no banco fictício."
  def get_product(sku) do
    with :ok <- validate_sku(sku) do
      case db_call(fn -> FakeDB.fetch(sku) end) do
        {:ok, product} -> {:ok, product}
        {:error, :not_found} -> {:error, {:validation, :product_not_found}}
        error -> error
      end
    end
  end

  @doc "Lista os produtos armazenados no banco fictício."
  def list_products do
    db_call(fn -> FakeDB.list() end)
  end

  @doc "Vende uma quantidade positiva de unidades do produto."
  def sell(sku, quantity), do: change_stock(sku, quantity, :sell)

  @doc "Adiciona uma quantidade positiva de unidades ao estoque."
  def receive_stock(sku, quantity), do: change_stock(sku, quantity, :receive)

  @doc "Retorna o estado do processo do produto e o resultado da última persistência periódica."
  def product_status(sku) do
    with :ok <- validate_sku(sku) do
      product_call(sku, :status)
    end
  end

  defp change_stock(sku, quantity, operation) do
    with :ok <- validate_sku(sku),
         :ok <- validate_quantity(quantity) do
      product_call(sku, {operation, quantity})
    end
  end

  defp product_call(sku, message) do
    case Registry.lookup(Inventory.Registry, sku) do
      [{pid, _value}] ->
        safe_call(fn -> GenServer.call(pid, message) end)

      [] ->
        {:error, {:operational, :product_process_not_running}}
    end
  end

  defp validate_product(attrs) when is_map(attrs) do
    sku = Map.get(attrs, :sku)
    name = Map.get(attrs, :name)
    price = Map.get(attrs, :price)
    quantity = Map.get(attrs, :quantity)

    with :ok <- validate_sku(sku),
         :ok <- validate_name(name),
         :ok <- validate_price(price),
         :ok <- validate_initial_quantity(quantity) do
      {:ok,
       %{
         sku: String.trim(sku),
         name: String.trim(name),
         price: price,
         quantity: quantity,
         updated_at: now()
       }}
    end
  end

  defp validate_product(_), do: {:error, {:validation, :invalid_product}}

  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      interval = Keyword.get(opts, :checkpoint_ms, @default_checkpoint_ms)

      if is_integer(interval) and interval > 0 do
        {:ok, interval}
      else
        {:error, {:validation, :invalid_checkpoint_interval}}
      end
    else
      {:error, {:validation, :invalid_options}}
    end
  end

  defp validate_options(_), do: {:error, {:validation, :invalid_options}}

  defp validate_sku(sku) when is_binary(sku) do
    if String.trim(sku) == "" do
      {:error, {:validation, :invalid_sku}}
    else
      :ok
    end
  end

  defp validate_sku(_), do: {:error, {:validation, :invalid_sku}}

  defp validate_name(name) when is_binary(name) do
    if String.trim(name) == "" do
      {:error, {:validation, :invalid_name}}
    else
      :ok
    end
  end

  defp validate_name(_), do: {:error, {:validation, :invalid_name}}

  defp validate_price(price) when is_number(price) and price >= 0, do: :ok
  defp validate_price(_), do: {:error, {:validation, :invalid_price}}

  defp validate_initial_quantity(quantity) when is_integer(quantity) and quantity >= 0,
    do: :ok

  defp validate_initial_quantity(_), do: {:error, {:validation, :invalid_quantity}}

  defp validate_quantity(quantity) when is_integer(quantity) and quantity > 0, do: :ok
  defp validate_quantity(_), do: {:error, {:validation, :invalid_quantity}}

  defp insert_product(product) do
    case db_call(fn -> FakeDB.insert(product) end) do
      :ok -> :ok
      {:error, :already_exists} -> {:error, {:validation, :sku_already_exists}}
      error -> error
    end
  end

  defp db_call(fun) do
    try do
      fun.()
    catch
      :exit, reason -> {:error, {:operational, {:database_unavailable, reason}}}
    end
  end

  defp safe_call(fun) do
    try do
      fun.()
    catch
      :exit, reason -> {:error, {:operational, {:process_unavailable, reason}}}
    end
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule Inventory.FakeDB do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{}, name: Keyword.fetch!(opts, :name))
  end

  def insert(product), do: GenServer.call(Inventory.Database, {:insert, product})
  def fetch(sku), do: GenServer.call(Inventory.Database, {:fetch, sku})
  def list, do: GenServer.call(Inventory.Database, :list)
  def save(product), do: GenServer.call(Inventory.Database, {:save, product})
  def delete(sku), do: GenServer.call(Inventory.Database, {:delete, sku})

  @impl true
  def init(products), do: {:ok, products}

  @impl true
  def handle_call({:insert, product}, _from, products) do
    if Map.has_key?(products, product.sku) do
      {:reply, {:error, :already_exists}, products}
    else
      {:reply, :ok, Map.put(products, product.sku, product)}
    end
  end

  def handle_call({:fetch, sku}, _from, products) do
    case Map.fetch(products, sku) do
      {:ok, product} -> {:reply, {:ok, product}, products}
      :error -> {:reply, {:error, :not_found}, products}
    end
  end

  def handle_call(:list, _from, products) do
    {:reply, products |> Map.values() |> Enum.sort_by(& &1.sku), products}
  end

  def handle_call({:save, product}, _from, products) do
    if Map.has_key?(products, product.sku) do
      {:reply, :ok, Map.put(products, product.sku, product)}
    else
      {:reply, {:error, :not_found}, products}
    end
  end

  def handle_call({:delete, sku}, _from, products) do
    {:reply, :ok, Map.delete(products, sku)}
  end
end

defmodule Inventory.Product do
  @moduledoc false

  use GenServer

  alias Inventory.FakeDB

  def start_link({product, checkpoint_ms}) do
    GenServer.start_link(
      __MODULE__,
      {product, checkpoint_ms},
      name: {:via, Registry, {Inventory.Registry, product.sku}}
    )
  end

  @impl true
  def init({product, checkpoint_ms}) do
    stored_product =
      case FakeDB.fetch(product.sku) do
        {:ok, stored} -> stored
        _ -> product
      end

    schedule_checkpoint(checkpoint_ms)

    {:ok,
     %{
       product: stored_product,
       checkpoint_ms: checkpoint_ms,
       persistence: :ok
     }}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply,
     {:ok, %{product: state.product, persistence: state.persistence}},
     state}
  end

  def handle_call({:sell, quantity}, _from, state) do
    if quantity > state.product.quantity do
      {:reply, {:error, {:validation, :insufficient_stock}}, state}
    else
      update_quantity(state, state.product.quantity - quantity)
    end
  end

  def handle_call({:receive, quantity}, _from, state) do
    update_quantity(state, state.product.quantity + quantity)
  end

  @impl true
  def handle_info(:checkpoint, state) do
    persistence =
      case persist(state.product) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end

    schedule_checkpoint(state.checkpoint_ms)
    {:noreply, %{state | persistence: persistence}}
  end

  defp update_quantity(state, quantity) do
    product =
      state.product
      |> Map.put(:quantity, quantity)
      |> Map.put(:updated_at, System.system_time(:millisecond))

    case persist(product) do
      :ok ->
        {:reply, {:ok, product},
         %{state | product: product, persistence: :ok}}

      {:error, reason} ->
        error = {:operational, reason}
        {:reply, {:error, error}, %{state | persistence: {:error, error}}}
    end
  end

  defp persist(product) do
    try do
      case FakeDB.save(product) do
        :ok -> :ok
        {:error, reason} -> {:error, {:database_write_failed, reason}}
      end
    catch
      :exit, reason -> {:error, {:database_unavailable, reason}}
    end
  end

  defp schedule_checkpoint(interval) do
    Process.send_after(self(), :checkpoint, interval)
  end
end
