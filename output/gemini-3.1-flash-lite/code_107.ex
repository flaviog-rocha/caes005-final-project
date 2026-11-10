# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Fazer uso de macros.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionalSystem do
  @moduledoc """
  Sistema de troca de mensagens com garantias de atomicidade, 
  idempotência e gerenciamento de estado via GenServer e Supervisor.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [MessageStore]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defmodule MessageStore do
    use GenServer

    # Macros para garantir DRY e encapsulamento de operações
    defmacro def_op(name, args, do: body) do
      quote do
        def unquote(name)(unquote_splicing(args)) do
          GenServer.call(__MODULE__, {:execute, unquote(name), unquote(args), fn -> unquote(body) end})
        end
      end
    end

    def start_link(_), do: GenServer.start_link(__MODULE__, %{data: %{}, ops: MapSet.new()}, name: __MODULE__)

    @impl true
    def init(state), do: {:ok, state}

    # API de Operações
    def_op(create, [id, msg], do: Map.put(state.data, id, msg))
    def_op(update, [id, msg], do: Map.put(state.data, id, msg))
    def_op(delete, [id], do: Map.delete(state.data, id))

    def get(id), do: GenServer.call(__MODULE__, {:get, id})
    def get_ops, do: GenServer.call(__MODULE__, :get_ops)

    @impl true
    def handle_call({:get, id}, _from, state), do: {:reply, Map.get(state.data, id), state}
    
    def handle_call(:get_ops, _from, state), do: {:reply, state.ops, state}

    def handle_call({:execute, op_name, args, func}, {pid, _}, state) do
      op_key = {op_name, args}
      
      if MapSet.member?(state.ops, op_key) do
        {:reply, {:error, :already_executed}, state}
      else
        try do
          # Atomicidade: o estado só é atualizado se a função completar sem erros
          new_data = func.()
          new_state = %{state | data: new_data, ops: MapSet.put(state.ops, op_key)}
          {:reply, :ok, new_state}
        rescue
          _ -> {:reply, {:error, :transaction_failed}, state}
        end
      end
    end
  end
end

# Exemplo de uso:
# TransactionalSystem.start_link([])
# TransactionalSystem.MessageStore.create(:msg1, "Olá Mundo")
# TransactionalSystem.MessageStore.get(:msg1)
# TransactionalSystem.MessageStore.get_ops()
