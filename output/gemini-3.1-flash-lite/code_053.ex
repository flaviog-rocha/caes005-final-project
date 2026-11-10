# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  use Agent

  @doc """
  Gerenciador de notificações usando Agent.
  Implementa recuperação via supervisão (simulada pelo estado), 
  validação de dados, disparo de condições e histórico.
  """

  # --- API Pública ---

  def start_link(_opts) do
    # O estado do Agent persiste enquanto o processo estiver vivo.
    # Em produção, este processo seria supervisionado por um Supervisor.
    Agent.start_link(fn -> %{notifications: [], history: []} end, name: __MODULE__)
  end

  def send_notification(user_id, message) when is_binary(user_id) and is_binary(message) do
    # 2. Validação dos dados
    if String.length(message) > 0 do
      Agent.update(__MODULE__, fn state ->
        # 3. Condição: Notificar apenas se a mensagem for urgente
        updated_state = if String.contains?(message, "URGENTE") do
          handle_urgent_notification(state, user_id, message)
        else
          %{state | notifications: state.notifications ++ [%{user: user_id, msg: message}]}
        end

        # 4. Manter histórico
        %{updated_state | history: updated_state.history ++ [%{action: "send", user: user_id, ts: System.system_time(:second)}]}
      end)
    else
      {:error, :invalid_message}
    end
  end

  def get_history do
    Agent.get(__MODULE__, & &1.history)
  end

  # --- Lógica Interna ---

  defp handle_urgent_notification(state, user_id, message) do
    IO.puts("ALERTA: Notificação urgente disparada para #{user_id}")
    %{state | notifications: state.notifications ++ [%{user: user_id, msg: message, priority: :high}]}
  end

  # --- 1. Recuperação após falhas ---
  # O Elixir utiliza o princípio "Let it crash".
  # Se o processo cair, ao ser reiniciado pelo Supervisor, o estado seria perdido.
  # Para persistência real, integraríamos com :ets ou um banco de dados.
  # Abaixo, um callback de inicialização para garantir estado inicial.
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent # Garante que o processo seja reiniciado pelo Supervisor
    }
  end
end
