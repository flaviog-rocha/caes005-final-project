# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de gerenciamento de notificações utilizando GenServer para isolamento de estado
  e concorrência segura através de processamento sequencial de mensagens.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{notifications: %{}}, name: __MODULE__)
  end

  @doc "Registra uma notificação de forma síncrona, garantindo consistência."
  def notify(user_id, message) do
    GenServer.call(__MODULE__, {:add, user_id, message})
  end

  @doc "Recupera notificações de um usuário."
  def get_notifications(user_id) do
    GenServer.call(__MODULE__, {:get, user_id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:add, user_id, message}, _from, state) do
    # Simulação de persistência (ex: Ecto.Repo.insert/1)
    IO.puts("[DB] Persistindo notificação para #{user_id}...")

    new_state =
      update_in(state, [:notifications, user_id], fn existing ->
        (existing || []) ++ [message]
      end)

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get, user_id}, _from, state) do
    notifications = get_in(state, [:notifications, user_id]) || []
    {:reply, notifications, state}
  end
end

# --- Exemplo de uso concorrente ---

# Iniciar o sistema
{:ok, _} = NotificationSystem.start_link([])

# Simulação de concorrência: Múltiplos processos enviando notificações
tasks =
  Enum.map(1..5, fn i ->
    Task.async(fn ->
      NotificationSystem.notify("user_1", "Mensagem #{i} vinda do processo #{inspect(self())}")
    end)
  end)

# Aguarda conclusão das operações
Enum.each(tasks, &Task.await/1)

# Consulta o resultado final
IO.inspect(NotificationSystem.get_notifications("user_1"), label: "Notificações do Usuário 1")

# --- Nota sobre Migração (Conceitual com Ecto) ---
# Se estivéssemos usando Ecto, a migração seria:
# def change do
#   create table(:notifications) do
#     add :user_id, :string
#     add :message, :text
#     timestamps()
#   end
# end
