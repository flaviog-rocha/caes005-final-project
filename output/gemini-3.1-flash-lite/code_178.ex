# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de mensagens com persistência via Agent (estado) e 
  supervisão para tolerância a falhas.
  """

  use Supervisor

  # Macro para definir operações básicas de CRUD encapsulando o Agent
  defmacro def_crud(key) do
    quote do
      def unquote(:"create_#{key}")(item) do
        Agent.update(__MODULE__, fn state ->
          Map.put(state, unquote(key), [item | Map.get(state, unquote(key), [])])
        end)
      end

      def unquote(:"get_#{key}") do
        Agent.get(__MODULE__, &Map.get(&1, unquote(key), []))
      end

      def unquote(:"update_#{key}")(old_val, new_val) do
        Agent.update(__MODULE__, fn state ->
          list = Map.get(state, unquote(key), [])
          Map.put(state, unquote(key), Enum.map(list, fn x -> if x == old_val, do: new_val, else: x end))
        end)
      end

      def unquote(:"delete_#{key}")(item) do
        Agent.update(__MODULE__, fn state ->
          list = Map.get(state, unquote(key), [])
          Map.put(state, unquote(key), List.delete(list, item))
        end)
      end
    end
  end

  # Implementação das operações via Macro
  def_crud(:messages)

  # Lógica de supervisão (Requirement 1: Recuperação de falhas)
  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {Agent, fn -> %{messages: []} end, name: __MODULE__}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Exemplo de uso:
# 1. Iniciar o sistema
# MessagingSystem.start_link([])

# 2. CRUD Concorrente (Requirement 2: Consistência via Agent - serialização de mensagens)
# MessagingSystem.create_messages(%{id: 1, text: "Olá, mundo!"})
# MessagingSystem.create_messages(%{id: 2, text: "Funcional é top"})

# 3. Consulta
# IO.inspect(MessagingSystem.get_messages())

# 4. Atualização
# MessagingSystem.update_messages(%{id: 1, text: "Olá, mundo!"}, %{id: 1, text: "Olá, Elixir!"})

# 5. Remoção
# MessagingSystem.delete_messages(%{id: 2, text: "Funcional é top"})
