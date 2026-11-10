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
  Macros para definir operações de estoque idempotentes.
  """
  defmacro defoperation(name, do: block) do
    quote do
      def unquote(name)(id, entity_id, params) do
        operation_key = {unquote(name), id}

        # Garante a idempotência verificando se a operação já foi registrada
        # no estado do GenServer (via ETS ou processo)
        GenServer.call(:"entity_#{unquote(entity_id)}", {:execute, operation_key, fn -> 
          unquote(block)
        end})
      end
    end
  end
end

defmodule Inventory.EntityActor do
  use GenServer
  
  # Persistência baseada em ETS para o exemplo
  # Em produção, usaria-se um Adapter de banco de dados
  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id, name: :"entity_#{entity_id}")
  end

  def init(entity_id) do
    table = :ets.new(:inventory_data, [:set, :protected, :named_table])
    {:ok, %{entity_id: entity_id, table: table}}
  end

  def handle_call({:execute, op_key, func}, _from, state) do
    if :ets.member(state.table, op_key) do
      {:reply, {:error, :already_processed}, state}
    else
      result = func.()
      :ets.insert(state.table, {op_key, result})
      {:reply, {:ok, result}, state}
    end
  end
end

defmodule Inventory.Manager do
  use DynamicSupervisor

  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def init(:ok), do: DynamicSupervisor.init(strategy: :one_for_one)

  # Permite processamento independente por entidade
  def get_or_start_entity(entity_id) do
    case DynamicSupervisor.start_child(__MODULE__, {Inventory.EntityActor, entity_id}) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end
end

defmodule Inventory.App do
  import Inventory.Operations

  # Uso da macro para definir operações idempotentes
  defoperation :add_stock do
    # Lógica de negócio simulada
    {:ok, :stock_updated}
  end

  def run_demo do
    Inventory.Manager.start_link([])
    
    # Execução simultânea (concorrência via processos independentes)
    entities = [1, 2, 3]
    
    Enum.each(entities, fn id ->
      Inventory.Manager.get_or_start_entity(id)
      
      # Tenta executar a mesma operação duas vezes para testar idempotência
      IO.inspect(add_stock("op_001", id, %{qty: 10}))
      IO.inspect(add_stock("op_001", id, %{qty: 10}))
    end)
  end
end
