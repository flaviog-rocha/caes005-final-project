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

defmodule Inventory.API do
  defmacro __using__(_opts) do
    quote do
      def receive_stock(entity, operation_id, quantity) do
        Inventory.Entity.execute(entity, operation_id, {:receive, quantity})
      end

      def sell_stock(entity, operation_id, quantity) do
        Inventory.Entity.execute(entity, operation_id, {:sell, quantity})
      end

      def stock(entity) do
        Inventory.Entity.get_stock(entity)
      end
    end
  end
end

defmodule Inventory.Store do
  use GenServer

  @table :inventory_dets

  def start_link(data_file) do
    GenServer.start_link(__MODULE__, data_file, name: __MODULE__)
  end

  @impl true
  def init(data_file) do
    File.mkdir_p!(Path.dirname(data_file))

    case :dets.open_file(@table,
           file: String.to_charlist(data_file),
           type: :set,
           repair: true
         ) do
      {:ok, @table} -> {:ok, nil}
      {:error, reason} -> {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end
end

defmodule Inventory.Entity do
  use GenServer

  @table :inventory_dets

  def start_link(entity) do
    GenServer.start_link(__MODULE__, entity,
      name: {:via, Registry, {Inventory.Registry, entity}}
    )
  end

  def child_spec(entity) do
    %{
      id: {__MODULE__, entity},
      start: {__MODULE__, :start_link, [entity]},
      restart: :permanent,
      type: :worker
    }
  end

  def execute(entity, operation_id, operation) do
    with {:ok, pid} <- ensure_started(entity) do
      GenServer.call(pid, {:execute, operation_id, operation})
    end
  end

  def get_stock(entity) do
    with {:ok, pid} <- ensure_started(entity) do
      GenServer.call(pid, :stock)
    end
  end

  @impl true
  def init(entity) do
    state =
      case :dets.lookup(@table, entity) do
        [{^entity, stock, operations}] when is_integer(stock) and is_map(operations) ->
          %{entity: entity, stock: stock, operations: operations}

        _ ->
          %{entity: entity, stock: 0, operations: %{}}
      end

    {:ok, state}
  end

  @impl true
  def handle_call(:stock, _from, state) do
    {:reply, {:ok, state.stock}, state}
  end

  def handle_call({:execute, operation_id, operation}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, {^operation, previous_stock}} ->
        {:reply, {:ok, previous_stock, :duplicate}, state}

      {:ok, {_different_operation, _previous_stock}} ->
        {:reply, {:error, :operation_id_conflict}, state}

      :error ->
        apply_operation(operation, state, operation_id)
    end
  end

  defp apply_operation({:receive, quantity}, state, operation_id)
       when is_integer(quantity) and quantity > 0 do
    persist_operation(state, operation_id, {:receive, quantity}, state.stock + quantity)
  end

  defp apply_operation({:sell, quantity}, state, operation_id)
       when is_integer(quantity) and quantity > 0 and quantity <= state.stock do
    persist_operation(state, operation_id, {:sell, quantity}, state.stock - quantity)
  end

  defp apply_operation({:sell, quantity}, state, _operation_id)
       when is_integer(quantity) and quantity > 0 do
    {:reply, {:error, :insufficient_stock}, state}
  end

  defp apply_operation(_operation, state, _operation_id) do
    {:reply, {:error, :invalid_operation}, state}
  end

  defp persist_operation(state, operation_id, operation, new_stock) do
    operations =
      Map.put(state.operations, operation_id, {operation, new_stock})

    record = {state.entity, new_stock, operations}

    with :ok <- :dets.insert(@table, record),
         :ok <- :dets.sync(@table) do
      new_state = %{state | stock: new_stock, operations: operations}
      {:reply, {:ok, new_stock, :applied}, new_state}
    else
      {:error, reason} ->
        {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  defp ensure_started(entity) do
    case Registry.lookup(Inventory.Registry, entity) do
      [{pid, _value}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               Inventory.Workers,
               {__MODULE__, entity}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, reason} ->
            case Registry.lookup(Inventory.Registry, entity) do
              [{pid, _value}] -> {:ok, pid}
              [] -> {:error, reason}
            end
        end
    end
  end
end

defmodule Inventory do
  use Supervisor
  use Inventory.API

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_file =
      Keyword.get(
        opts,
        :data_file,
        Path.join(File.cwd!(), "inventory.dets")
      )

    children = [
      {Inventory.Store, data_file},
      {Registry, keys: :unique, name: Inventory.Registry},
      {DynamicSupervisor,
       strategy: :one_for_one, name: Inventory.Workers}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
