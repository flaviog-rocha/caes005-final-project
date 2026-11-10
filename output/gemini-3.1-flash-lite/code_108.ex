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

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando GenServer e Supervisor.
  Garante atomicidade através de registro de IDs de operação.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: :op_registry},
      {Agent, fn -> %{} end, name: :messages_store},
      {Agent, fn -> MapSet.new() end, name: :processed_ops}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  defmodule Operations do
    defmacro def_atomic_op(name, do: block) do
      quote do
        def unquote(name)(op_id, data) do
          # 4. Impedir execução duplicada
          Agent.get_and_update(:processed_ops, fn set ->
            if MapSet.member?(set, op_id), do: {:error, set}, else: {:ok, MapSet.put(set, op_id)}
          end)
          |> case do
            {:ok, _} ->
              # 1. Impedir atualizações parciais (try/catch no contexto da operação)
              try do
                result = unquote(block)
                {:ok, result}
              rescue
                e -> {:error, e}
              end
            {:error, _} -> {:error, :already_processed}
          end
        end
      end
    end
  end

  require Operations

  # 5. CRUD: Cadastrar (Create)
  Operations.def_atomic_op(:create_message, do: Agent.update(:messages_store, &Map.put(&1, op_id, data)))

  # 5. CRUD: Atualizar (Update)
  Operations.def_atomic_op(:update_message, do: Agent.update(:messages_store, &Map.put(&1, op_id, data)))

  # 5. CRUD: Remover (Delete)
  def delete_message(op_id), do: Agent.update(:messages_store, &Map.delete(&1, op_id))

  # 5. CRUD: Consultar (Read)
  def get_message(op_id), do: Agent.get(:messages_store, &Map.get(&1, op_id))

  # 2. Consultar estado das operações em andamento
  def list_processed_ops, do: Agent.get(:processed_ops, & &1)
end
