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
  Sistema de mensagens com monitoramento de processos, validação de tipos
  de erro e macros para geração de hooks de notificação.
  """

  # 1. Macros para geração de notificações
  defmacro def_with_notification(name, args, do: block) do
    quote do
      def unquote(name)(unquote_splicing(args)) do
        result = unquote(block)
        case result do
          {:ok, data} ->
            IO.puts("[NOTIFICAÇÃO]: Sucesso na operação #{unquote(name)}")
            {:ok, data}
          error -> error
        end
      end
    end
  end

  # 4. Diferenciação de erros (Domain vs Operational)
  defmodule Errors do
    defexception [:message, :type]
    @impl true
    def message(e), do: "[#{e.type}] #{e.message}"
  end

  defmodule Manager do
    use GenServer

    # 3. Detecção de encerramentos inesperados (Monitoramento)
    def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

    def init(_), do: {:ok, %{}}

    def handle_call({:monitor, pid}, _from, state) do
      Process.monitor(pid)
      {:reply, :ok, state}
    end

    def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
      IO.puts("[ALERTA]: Processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
      {:noreply, state}
    end
  end

  # Implementação dos requisitos
  def send_message(content) when byte_size(content) > 100 do
    # Erro de validação (Domínio)
    {:error, %Errors{message: "Mensagem muito longa", type: :validation_error}}
  end
  def send_message(content) do
    {:ok, "Enviado: #{content}"}
  end

  # Uso da macro
  def_with_notification(process_message, [content], do: send_message(content))

  def run_example do
    {:ok, pid} = Manager.start_link([])
    
    # Exemplo de erro de validação
    IO.inspect(process_message("Olá"))
    IO.inspect(process_message(String.duplicate("a", 101)))

    # Exemplo de detecção de falha
    worker = spawn(fn -> raise "Falha operacional crítica!" end)
    GenServer.call(Manager, {:monitor, worker})
    
    :timer.sleep(100)
  end
end

MessagingSystem.run_example()
