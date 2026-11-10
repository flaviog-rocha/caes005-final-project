# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de gerenciamento de notificações com histórico de operações.
  Utiliza Agent para simular um banco de dados em memória.
  """

  defstruct id: nil, message: nil, status: :pending, created_at: nil

  @type t :: %__MODULE__{id: integer, message: String.t(), status: atom, created_at: DateTime.t()}

  # --- Inicialização ---

  def start_link do
    Agent.start_link(fn -> %{notifications: %{}, history: [], last_id: 0} end, name: __MODULE__)
  end

  # --- Interface CRUD ---

  def create(message) do
    Agent.get_and_update(__MODULE__, fn state ->
      id = state.last_id + 1
      notification = %__MODULE__{id: id, message: message, created_at: DateTime.utc_now()}
      new_state = %{
        state |
        notifications: Map.put(state.notifications, id, notification),
        history: ["CREATE: Notification #{id}" | state.history],
        last_id: id
      }
      {notification, new_state}
    end)
  end

  def get(id), do: Agent.get(__MODULE__, &Map.get(&1.notifications, id))

  def update(id, new_message) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.fetch(state.notifications, id) do
        {:ok, _} ->
          updated_note = %{state.notifications[id] | message: new_message}
          new_state = %{
            state |
            notifications: Map.put(state.notifications, id, updated_note),
            history: ["UPDATE: Notification #{id}" | state.history]
          }
          {updated_note, new_state}
        :error -> {:error, state}
      end
    end)
  end

  def delete(id) do
    Agent.get_and_update(__MODULE__, fn state ->
      new_state = %{
        state |
        notifications: Map.delete(state.notifications, id),
        history: ["DELETE: Notification #{id}" | state.history]
      }
      {:ok, new_state}
    end)
  end

  # --- Lógica de Domínio (Condicional) ---

  def trigger_if_urgent(id, criteria) do
    notification = get(id)
    if String.contains?(notification.message, criteria) do
      # Simulação de envio de notificação (ex: Webhook ou Email)
      IO.puts(">>> DISPARO URGENTE: #{notification.message}")
      :triggered
    else
      :ignored
    end
  end

  # --- Auditoria ---

  def get_history, do: Agent.get(__MODULE__, & &1.history)
end

# --- Exemplo de Uso ---
# NotificationSystem.start_link()
# note = NotificationSystem.create("Alerta: Servidor sobrecarregado")
# NotificationSystem.trigger_if_urgent(note.id, "Alerta")
# NotificationSystem.update(note.id, "Alerta: Servidor estabilizado")
# NotificationSystem.delete(note.id)
# IO.inspect(NotificationSystem.get_history())
