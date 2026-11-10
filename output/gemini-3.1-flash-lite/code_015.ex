# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  @moduledoc """
  Gerenciador de usuários e sessões utilizando ETS.
  - Validação de entrada.
  - Concorrência controlada via 'Task.async_stream' (limitando operações).
  - Comunicação via envio de mensagens entre processos.
  """

  @table_name :user_sessions
  @max_concurrency 10

  def init do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
  end

  # 1. Validação de dados
  defp validate_input(user_id, session_id) when is_binary(user_id) and is_binary(session_id) do
    {:ok, {user_id, session_id}}
  end
  defp validate_input(_, _), do: {:error, "Dados inválidos: user_id e session_id devem ser strings"}

  # Operação de escrita (com validação)
  def create_session(user_id, session_id) do
    with {:ok, data} <- validate_input(user_id, session_id) do
      :ets.insert(@table_name, {user_id, session_id})
      {:ok, "Sessão criada"}
    end
  end

  # 2. Comunicação entre processos
  # Envia uma mensagem para um processo específico solicitando dados
  def get_session_async(user_id, target_pid) do
    send(target_pid, {:fetch_session, user_id, self()})
  end

  # 3 & 4. Limitação e execução de operações simultâneas
  # Utiliza Task.async_stream para processar múltiplos registros com controle de concorrência
  def batch_process_sessions(sessions) do
    sessions
    |> Task.async_stream(
      fn {user_id, session_id} ->
        create_session(user_id, session_id)
      end,
      max_concurrency: @max_concurrency,
      ordered: false
    )
    |> Stream.run()
  end

  # Processo servidor para demonstrar a comunicação (pode ser usado como GenServer)
  def session_server_loop do
    receive do
      {:fetch_session, user_id, caller} ->
        result = :ets.lookup(@table_name, user_id)
        send(caller, {:session_data, result})
        session_server_loop()
    end
  end
end

# Exemplo de uso:
# UserSessionManager.init()
# UserSessionManager.create_session("user1", "abc-123")
# UserSessionManager.batch_process_sessions([{"u2", "s2"}, {"u3", "s3"}])
