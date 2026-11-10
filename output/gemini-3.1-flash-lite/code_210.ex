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

  # --- Interface Pública (CRUD) ---

  def create(message) do
    id = generate_id()
    notification = %__MODULE__{id: id, message: message, created_at: DateTime.utc_now()}
    
    update_state(fn state ->
      %{state | 
        notifications: Map.put(state.notifications, id, notification),
        history: [{:created, id, DateTime.utc_now()} | state.history]
      }
    end)
    
    {:ok, notification}
  end

  def get(id), do: Agent.get(__MODULE__, fn state -> Map.get(state.notifications, id) end)

  def update(id, new_message) do
    update_state(fn state ->
      case Map.has_key?(state.notifications, id) do
        true ->
          updated = %{state.notifications[id] | message: new_message}
          %{state | 
            notifications: Map.put(state.notifications, id, updated),
            history: [{:updated, id, DateTime.utc_now()} | state.history]
          }
        false -> state
      end
    end)
  end

  def delete(id) do
    update_state(fn state ->
      %{state | 
        notifications: Map.delete(state.notifications, id),
        history: [{:deleted, id, DateTime.utc_now()} | state.history]
      }
    end)
  end

  def get_history, do: Agent.get(__MODULE__, & &1.history)

  # --- Lógica de Negócio (Condicional) ---

  def trigger_if_urgent(id, keyword) do
    notification = get(id)
    if notification && String.contains?(notification.message, keyword) do
      update_state(fn state ->
        updated = %{notification | status: :urgent_alert}
        %{state | notifications: Map.put(state.notifications, id, updated)}
      end)
      {:triggered, id}
    else
      :no_action
    end
  end

  # --- Privados ---

  defp generate_id do
    Agent.get_and_update(__MODULE__, fn state ->
      new_id = state.last_id + 1
      {new_id, %{state | last_id: new_id}}
    end)
  end

  defp update_state(func), do: Agent.update(__MODULE__, func)
end

# --- Exemplo de Execução ---

NotificationSystem.start_link()

# 1. Cadastro
{:ok, n1} = NotificationSystem.create("System failure imminent")
{:ok, n2} = NotificationSystem.create("User logged in")

# 2. Condicional
NotificationSystem.trigger_if_urgent(n1.id, "failure")

# 3. Atualização
NotificationSystem.update(n2.id, "User updated profile")

# 4. Remoção
NotificationSystem.delete(n2.id)

# 5. Auditoria
IO.inspect(NotificationSystem.get_history(), label: "Histórico de Operações")
