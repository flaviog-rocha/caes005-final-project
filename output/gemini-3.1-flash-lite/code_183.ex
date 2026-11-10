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
  Sistema de gerenciamento de notificações utilizando GenServer para concorrência
  e estado consistente em memória (podendo ser persistido via Ecto futuramente).
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc "Registra uma nova notificação de forma thread-safe."
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
    # O estado é um Map onde a chave é user_id e o valor é uma List de notificações
    {:ok, state}
  end

  @impl true
  def handle_call({:add, user_id, message}, _from, state) do
    # Simulação de escrita em banco de dados
    persist_to_db(user_id, message)

    new_state = Map.update(state, user_id, [message], fn list -> [message | list] end)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get, user_id}, _from, state) do
    {:reply, Map.get(state, user_id, []), state}
  end

  # --- Funções Auxiliares / Domínio ---

  defp persist_to_db(user_id, message) do
    # Representa a camada de integração com banco de dados (ex: Ecto.Repo.insert)
    # Em um cenário real, usaria-se um Repo.insert(%Notification{...})
    IO.puts("[DB Mock] Persistindo para #{user_id}: #{message}")
  end
end

# --- Exemplo de Execução Concorrente ---

# Para testar a concorrência e a consistência:
{:ok, _pid} = NotificationSystem.start_link([])

tasks =
  1..5
  |> Enum.map(fn i ->
    Task.async(fn ->
      NotificationSystem.notify("user_1", "Mensagem #{i}")
    end)
  end)

Enum.each(tasks, &Task.await/1)

IO.inspect(NotificationSystem.get_notifications("user_1"), label: "Notificações do Usuário")
