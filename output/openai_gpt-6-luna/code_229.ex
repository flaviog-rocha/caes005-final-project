# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir a execução de operações simultâneas.
# 4. Impedir atualizações parcialmente concluídas.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Commerce.OrderMacros do
  defmacro __using__(_opts) do
    quote do
      @enforce_keys [:id, :items, :status, :version]
      defstruct [:id, :items, :status, :version]

      import Commerce.OrderMacros

      def valid_status?(status),
        do: status in [:pending, :paid, :cancelled, :refunded]
    end
  end

  defmacro deftransition(from, event, to) do
    quote do
      def transition(unquote(from), unquote(event)), do: {:ok, unquote(to)}
    end
  end
end

defmodule Commerce.Order do
  use Commerce.OrderMacros

  deftransition :pending, :pay, :paid
  deftransition :pending, :cancel, :cancelled
  deftransition :paid, :refund, :refunded

  def transition(_status, _event), do: {:error, :invalid_transition}
end

defmodule Commerce.Order.Database do
  use GenServer

  @table :commerce_orders_table

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})
  def create(order), do: GenServer.call(__MODULE__, {:create, order})
  def transact(id, function), do: GenServer.call(__MODULE__, {:transact, id, function}, :infinity)

  @impl true
  def init(:ok), do: {:ok, nil}

  @impl true
  def handle_call({:fetch, id}, _from, state) do
    result =
      case :ets.lookup(@table, id) do
        [{^id, order}] -> {:ok, order}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:create, order}, _from, state) do
    result =
      case :ets.insert_new(@table, {order.id, order}) do
        true -> {:ok, order}
        false -> {:error, :already_exists}
      end

    {:reply, result, state}
  end

  def handle_call({:transact, id, function}, _from, state) do
    result =
      case :ets.lookup(@table, id) do
        [{^id, order}] ->
          run_transaction(id, order, function)

        [] ->
          {:error, :not_found}
      end

    {:reply, result, state}
  end

  defp run_transaction(id, order, function) do
    try do
      case function.(order) do
        {:ok, %Commerce.Order{id: ^id} = updated_order, value} ->
          # The record is written only after the complete transaction succeeds.
          true = :ets.insert(@table, {id, updated_order})
          {:ok, updated_order, value}

        {:error, _reason} = error ->
          error

        _other ->
          {:error, :invalid_transaction}
      end
    rescue
      _exception -> {:error, :transaction_failed}
    catch
      _kind, _reason -> {:error, :transaction_failed}
    end
  end
end

defmodule Commerce.Order.Worker do
  use GenServer

  alias Commerce.Order
  alias Commerce.Order.Database

  def start_link(id) do
    GenServer.start_link(__MODULE__, id,
      name: {:via, Registry, {Commerce.Order.Registry, id}}
    )
  end

  @impl true
  def init(id) do
    case Database.fetch(id) do
      {:ok, order} -> {:ok, order}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:get, _from, order), do: {:reply, {:ok, order}, order}

  def handle_call({:add_item, item}, _from, order) do
    result =
      Database.transact(order.id, fn current ->
        if current.status == :pending do
          updated = %{current | items: current.items ++ [item], version: current.version + 1}
          {:ok, updated, updated}
        else
          {:error, :order_not_editable}
        end
      end)

    reply_with_transaction(result, order)
  end

  def handle_call({:transition, event}, _from, order) do
    result =
      Database.transact(order.id, fn current ->
        case Order.transition(current.status, event) do
          {:ok, status} ->
            updated = %{current | status: status, version: current.version + 1}
            {:ok, updated, updated}

          {:error, reason} ->
            {:error, reason}
        end
      end)

    reply_with_transaction(result, order)
  end

  defp reply_with_transaction({:ok, updated, value}, _old_order),
    do: {:reply, {:ok, value}, updated}

  defp reply_with_transaction({:error, reason}, order),
    do: {:reply, {:error, reason}, order}
end

defmodule Commerce.OrderSystem do
  use Supervisor

  alias Commerce.Order
  alias Commerce.Order.Database

  @table :commerce_orders_table

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    # The supervisor owns this table, so it survives restarts of the database
    # and individual order workers.
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :set,
        :public,
        {:read_concurrency, true}
      ])
    end

    children = [
      Database,
      {Registry, keys: :unique, name: Commerce.Order.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Commerce.Order.Workers}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_order(id, items \\ []) when is_list(items) do
    order = %Order{id: id, items: items, status: :pending, version: 0}

    with {:ok, created} <- Database.create(order),
         :ok <- ensure_worker(id) do
      {:ok, created}
    end
  end

  def get_order(id), do: call_worker(id, :get)
  def add_item(id, item), do: call_worker(id, {:add_item, item})
  def transition(id, event), do: call_worker(id, {:transition, event})

  # Useful for testing recovery: the permanent dynamic child is restarted and
  # reloads its latest committed order from the database table.
  def simulate_worker_crash(id) do
    case Registry.lookup(Commerce.Order.Registry, id) do
      [{pid, _value}] ->
        Process.exit(pid, :kill)
        :ok

      [] ->
        {:error, :worker_not_found}
    end
  end

  defp call_worker(id, request) do
    with :ok <- ensure_worker(id) do
      case Registry.lookup(Commerce.Order.Registry, id) do
        [{pid, _value}] -> GenServer.call(pid, request)
        [] -> {:error, :worker_unavailable}
      end
    end
  end

  defp ensure_worker(id) do
    case Registry.lookup(Commerce.Order.Registry, id) do
      [{_pid, _value}] ->
        :ok

      [] ->
        spec = %{
          id: {Commerce.Order.Worker, id},
          start: {Commerce.Order.Worker, :start_link, [id]},
          restart: :permanent,
          shutdown: 5_000,
          type: :worker
        }

        case DynamicSupervisor.start_child(Commerce.Order.Workers, spec) do
          {:ok, _pid} ->
            :ok

          {:ok, _pid, _info} ->
            :ok

          {:error, {:already_started, _pid}} ->
            :ok

          {:error, {:already_registered, _pid}} ->
            :ok

          {:error, reason} ->
            case Registry.lookup(Commerce.Order.Registry, id) do
              [{_pid, _value}] -> :ok
              [] -> {:error, reason}
            end
        end
    end
  end
end
