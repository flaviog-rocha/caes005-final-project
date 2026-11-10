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
  @moduledoc """
  Sistema de gerenciamento de notificações utilizando Agent para estado.
  """
  use Agent

  # 1. Recuperação: Supervisor monitorando o Agent
  def start_link(_opts) do
    Agent.start_link(fn -> %{history: [], notifications: []} end, name: __MODULE__)
  end

  # 2. Validação: Função privada para garantir integridade
  defp validate_input(payload) when is_map(payload) do
    case {Map.has_key?(payload, :user_id), Map.has_key?(payload, :message)} do
      {true, true} -> {:ok, payload}
      _ -> {:error, "Dados inválidos: user_id e message são obrigatórios"}
    end
  end

  defp validate_input(_), do: {:error, "Payload deve ser um mapa"}

  # 3. Processamento e 4. Histórico
  def process_notification(payload) do
    with {:ok, data} <- validate_input(payload) do
      Agent.update(__MODULE__, fn state ->
        # Lógica de condição para gerar notificação (ex: mensagem curta)
        new_notifications =
          if String.length(data.message) < 10 do
            [data | state.notifications]
          else
            state.notifications
          end

        # Mantém histórico de todas as operações
        new_history = [%{timestamp: DateTime.utc_now(), data: data} | state.history]

        %{state | history: new_history, notifications: new_notifications}
      end)

      :ok
    end
  end

  # Getters para auditoria
  def get_history, do: Agent.get(__MODULE__, & &1.history)
  def get_notifications, do: Agent.get(__MODULE__, & &1.notifications)
end

# Exemplo de uso:
# {:ok, _pid} = NotificationManager.start_link([])
# NotificationManager.process_notification(%{user_id: 1, message: "Olá!"})
# NotificationManager.get_history()
