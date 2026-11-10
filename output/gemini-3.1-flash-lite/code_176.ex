# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando OTP e Macros para DSL de domínio.
  """

  # 1. Uso de Macros para definir regras de notificação
  defmacro define_notification(rule_name, condition: condition) do
    quote do
      def unquote(rule_name)(message) do
        if unquote(condition).(message) do
          IO.puts("[NOTIFICAÇÃO] Regra #{unquote(rule_name)} disparada para: #{message}")
        end
      end
    end
  end

  # 4. Diferenciação de Erros: Definição de exceções
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # Processo de Mensageria (GenServer)
  use GenServer

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true) # 3. Detectar encerramentos inesperados
    {:ok, %{}}
  end

  # Macros de Regras
  define_notification(:notify_urgent, condition: fn msg -> String.contains?(msg, "URGENTE") end)

  def send_message(text) do
    cond do
      text == "" -> raise ValidationError, message: "Mensagem vazia"
      text == "CRASH" -> raise OperationalError, message: "Falha de conexão com broker"
      true -> GenServer.cast(__MODULE__, {:msg, text})
    end
  end

  @impl true
  def handle_cast({:msg, text}, state) do
    notify_urgent(text)
    {:noreply, state}
  end

  # 3. Tratamento de encerramento de processos monitorados
  @impl true
  def handle_info({:EXIT, _from, reason}, state) do
    IO.puts("[SISTEMA] Processo filho encerrou inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end
end

# Exemplo de execução
defmodule Demo do
  def run do
    MessagingSystem.start_link([])

    # Teste de Validação (4)
    try do
      MessagingSystem.send_message("")
    rescue
      e in [MessagingSystem.ValidationError] -> IO.puts("Erro de Negócio: #{e.message}")
    end

    # Teste de Notificação (1 e 2)
    MessagingSystem.send_message("Isso é URGENTE!")

    # Teste de Falha Operacional (3 e 4)
    try do
      MessagingSystem.send_message("CRASH")
    rescue
      e in [MessagingSystem.OperationalError] -> IO.puts("Falha de Infra: #{e.message}")
    end
  end
end

Demo.run()
