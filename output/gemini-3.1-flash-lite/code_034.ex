# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando Agent para estado,
  Task para processamento assíncrono e Macros para DSL.
  """

  # Macro para facilitar a criação de novos usuários e envio de mensagens
  defmacro def_messaging_dsl do
    quote do
      def send_msg(from, to, content) do
        MessagingSystem.dispatch(from, to, content)
      end

      def get_history(user) do
        MessagingSystem.read_mailbox(user)
      end
    end
  end

  # Estado persistido em arquivo (simples para exemplo)
  @db_file "messages.dets"

  def start_link do
    # Inicializa o armazenamento (DETS é eficiente para persistência local)
    {:ok, _} = :dets.open_file(:mailbox_db, [file: @db_file, type: :bag])
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  # Garante consistência usando a serialização do Agent
  def dispatch(from, to, content) do
    Task.start(fn ->
      msg = %{from: from, to: to, content: content, timestamp: System.system_time(:second)}
      
      # Persistência atômica
      :dets.insert(:mailbox_db, {to, msg})
      
      # Atualiza estado em memória
      Agent.update(__MODULE__, fn state ->
        Map.update(state, to, [msg], &[msg | &1])
      end)
    end)
  end

  def read_mailbox(user) do
    # Retorna mensagens do cache em memória
    Agent.get(__MODULE__, fn state -> Map.get(state, user, []) end)
  end
end

# Demonstração de uso
defmodule App do
  require MessagingSystem
  MessagingSystem.def_messaging_dsl()

  def run do
    MessagingSystem.start_link()

    # Operações concorrentes via Task
    send_msg("Alice", "Bob", "Olá Bob!")
    send_msg("Charlie", "Bob", "Oi Bob, tudo bem?")

    # Pequeno delay para garantir que as Tasks terminem de processar
    Process.sleep(100)

    IO.inspect(get_history("Bob"), label: "Mensagens para Bob")
  end
end
