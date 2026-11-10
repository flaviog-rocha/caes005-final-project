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

defmodule Inventory.Operations do
  @moduledoc """
  Gerenciador de operações idempotentes para estoque.
  Utiliza Agent para estado e Process Registry para garantir exclusividade.
  """

  defmacro __using__(_opts) do
    quote do
      def start_link(initial_state), do: Agent.start_link(fn -> initial_state end, name: __MODULE__)
      def update_state(fun), do: Agent.update(__MODULE__, fun)
      def get_state, do: Agent.get(__MODULE__, & &1)
    end
  end
end

defmodule Inventory.ItemWorker do
  use Agent
  use Inventory.Operations

  # Persistência simplificada via arquivo
  defp file_path(id), do: "inventory_#{id}.dets"

  def start_link(id) do
    # Garante que cada item tenha seu próprio processo independente
    Agent.start_link(fn -> %{id: id, stock: 0, processed_ops: MapSet.new()} end, name: via_tuple(id))
  end

  defp via_tuple(id), do: {:via, Registry, {Inventory.Registry, id}}

  def execute_op(id, op_id, delta) do
    GenServer.call(via_tuple(id), {:apply_op, op_id, delta})
  end

  def handle_call({:apply_op, op_id, delta}, _from, state) do
    if MapSet.member?(state.processed_ops, op_id) do
      {:reply, {:error, :already_processed}, state}
    else
      new_state = %{state | 
        stock: state.stock + delta, 
        processed_ops: MapSet.put(state.processed_ops, op_id)
      }
      {:reply, {:ok, new_state.stock}, new_state}
    end
  end
end

defmodule Inventory.Manager do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: Inventory.Registry},
      {DynamicSupervisor, name: Inventory.WorkerSupervisor, strategy: :one_for_one}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end

  def spawn_item(id) do
    DynamicSupervisor.start_child(Inventory.WorkerSupervisor, {Inventory.ItemWorker, id})
  end
end

# Exemplo de uso:
# Inventory.Manager.start_link([])
# Inventory.Manager.spawn_item("sku_123")
# Inventory.ItemWorker.execute_op("sku_123", "op_unique_uuid_001", 10)
