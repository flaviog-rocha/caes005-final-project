# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Stock.OperationMacros do
  defmacro define_operation(function, action) do
    quote do
      def unquote(function)(entity, operation_id, product, quantity) do
        Stock.execute(entity, operation_id, unquote(action), product, quantity)
      end
    end
  end
end

defmodule Stock.Persistence do
  use GenServer

  @table Stock.Persistence.Table

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def table, do: @table

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:file, "stock.dets")
      |> Path.expand()

    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, [
           {:file, String.to_charlist(path)},
           {:type, :set}
         ]) do
      {:ok, _table} -> {:ok, path}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, _path) do
    :dets.close(@table)
  end
end

defmodule Stock.Entity do
  use GenServer

  def start_link(entity) do
    GenServer.start_link(__MODULE__, entity, name: via(entity))
  end

  def child_spec(entity) do
    %{
      id: {__MODULE__, entity},
      start: {__MODULE__, :start_link, [entity]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def via(entity), do: {:via, Registry, {Stock.Registry, entity}}

  @impl true
  def init(entity) do
    state =
      case :dets.lookup(Stock.Persistence.table(), entity) do
        [{^entity, persisted_state}] -> persisted_state
        [] -> %{inventory: %{}, operations: MapSet.new()}
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:execute, operation_id, action, product, quantity}, _from, state) do
    cond do
      is_nil(operation_id) ->
        {:reply, {:error, :invalid_operation_id}, state}

      MapSet.member?(state.operations, operation_id) ->
        current_quantity = Map.get(state.inventory, product, 0)
        {:reply, {:ok, :duplicate, current_quantity}, state}

      not is_integer(quantity) or quantity <= 0 ->
        {:reply, {:error, :invalid_quantity}, state}

      true ->
        apply_operation(state, operation_id, action, product, quantity)
    end
  end

  @impl true
  def handle_call(:inventory, _from, state) do
    {:reply, {:ok, state.inventory}, state}
  end

  @impl true
  def handle_call({:quantity, product}, _from, state) do
    {:reply, {:ok, Map.get(state.inventory, product, 0)}, state}
  end

  defp apply_operation(state, operation_id, :add, product, quantity) do
    inventory =
      Map.update(state.inventory, product, quantity, &(&1 + quantity))

    persist(state, operation_id, inventory, product)
  end

  defp apply_operation(state, operation_id, :subtract, product, quantity) do
    current_quantity = Map.get(state.inventory, product, 0)

    if current_quantity < quantity do
      {:reply, {:error, :insufficient_stock}, state}
    else
      inventory = Map.put(state.inventory, product, current_quantity - quantity)
      persist(state, operation_id, inventory, product)
    end
  end

  defp persist(state, operation_id, inventory, product) do
    new_state = %{
      inventory: inventory,
      operations: MapSet.put(state.operations, operation_id)
    }

    case :dets.insert(Stock.Persistence.table(), {self_entity(state), new_state}) do
      :ok ->
        case :dets.sync(Stock.Persistence.table()) do
          :ok ->
            {:reply, {:ok, :applied, Map.get(inventory, product, 0)}, new_state}

          {:error, reason} ->
            {:reply, {:error, {:persistence_error, reason}}, new_state}
        end

      {:error, reason} ->
        {:reply, {:error, {:persistence_error, reason}}, state}
    end
  end

  # The entity key is stored in the process dictionary during initialization.
  defp self_entity(_state), do: Process.get(:stock_entity)

  @impl true
  def handle_continue(:store_entity, entity) do
    Process.put(:stock_entity, entity)

    state =
      case :dets.lookup(Stock.Persistence.table(), entity) do
        [{^entity, persisted_state}] -> persisted_state
        [] -> %{inventory: %{}, operations: MapSet.new()}
      end

    {:noreply, state}
  end
end

defmodule Stock do
  use Supervisor
  use Stock.OperationMacros

  define_operation(:restock, :add)
  define_operation(:sell, :subtract)

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Stock.Supervisor)
  end

  @impl true
  def init(opts) do
    children = [
      {Stock.Persistence, opts},
      {Registry, keys: :unique, name: Stock.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Stock.EntitySupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def execute(entity, operation_id, action, product, quantity) do
    with {:ok, pid} <- ensure_entity(entity) do
      GenServer.call(
        pid,
        {:execute, operation_id, action, product, quantity},
        :infinity
      )
    end
  end

  def inventory(entity) do
    with {:ok, pid} <- ensure_entity(entity) do
      GenServer.call(pid, :inventory, :infinity)
    end
  end

  def quantity(entity, product) do
    with {:ok, pid} <- ensure_entity(entity) do
      GenServer.call(pid, {:quantity, product}, :infinity)
    end
  end

  defp ensure_entity(entity) do
    case Registry.lookup(Stock.Registry, entity) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Stock.EntitySupervisor,
               Stock.Entity.child_spec(entity)
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, _reason} = error ->
            case Registry.lookup(Stock.Registry, entity) do
              [{pid, _}] -> {:ok, pid}
              [] -> error
            end
        end
    end
  end
end
