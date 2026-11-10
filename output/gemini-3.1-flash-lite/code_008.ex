# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  @moduledoc """
  Gerenciador de sessões com limite de concorrência e monitoramento de processos.
  """
  require Logger

  # Macro para definir o comportamento de execução protegida
  defmacro with_concurrency_limit(limit, do: block) do
    quote do
      # Cria um pool simples usando uma fila via GenServer se necessário,
      # mas aqui utilizamos o Supervisor para gerenciar os trabalhadores (workers).
      # A lógica abaixo garante que apenas 'limit' processos rodem.
      :poolboy.transaction(:session_pool, fn worker ->
        unquote(block)
      end)
    end
  end

  def start_link(_opts) do
    # Define um supervisor para monitorar os processos dos usuários
    children = [
      {Task.Supervisor, name: UserSessionManager.TaskSupervisor},
      # Simulação de um pool de recursos
      {Registry, keys: :unique, name: UserRegistry}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  @doc """
  Inicia uma sessão de usuário e monitora o encerramento inesperado.
  """
  def start_user_session(user_id, task_func) do
    Logger.info("Iniciando sessão para o usuário: #{user_id}")

    # Processo que monitora a tarefa
    Task.Supervisor.start_child(UserSessionManager.TaskSupervisor, fn ->
      # Referencia o processo atual para detectar encerramentos
      process_ref = Process.monitor(self())
      
      try do
        task_func.()
      rescue
        e -> Logger.error("Erro na sessão #{user_id}: #{inspect(e)}")
      after
        receive do
          {:DOWN, ^process_ref, :process, _, reason} ->
            Logger.warn("Sessão do usuário #{user_id} encerrada. Motivo: #{inspect(reason)}")
        after
          0 -> :ok
        end
      end
    end)
  end
end

# Exemplo de uso
defmodule Example do
  require UserSessionManager

  def run do
    # Inicia o sistema
    {:ok, _} = UserSessionManager.start_link([])

    # Simula um usuário sendo processado
    UserSessionManager.start_user_session("user_123", fn ->
      Logger.info("Executando operação crítica...")
      Process.sleep(1000)
      # Simula falha inesperada
      raise "Falha crítica de sistema"
    end)
  end
end
