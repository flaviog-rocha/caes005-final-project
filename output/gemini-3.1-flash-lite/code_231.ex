# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSystem do
  @moduledoc """
  Sistema de gerenciamento de usuários e sessões com validação,
  notificações e expiração automática usando processos (GenServer).
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{users: %{}, sessions: %{}}, name: __MODULE__)

  def create_user(username, email) do
    GenServer.call(__MODULE__, {:create_user, username, email})
  end

  def create_session(user_id, ttl_seconds) do
    GenServer.call(__MODULE__, {:create_session, user_id, ttl_seconds})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:create_user, username, email}, _from, state) do
    cond do
      is_nil(username) or String.trim(username) == "" ->
        {:reply, {:error, :invalid_username}, state}
      not String.contains?(email, "@") ->
        {:reply, {:error, :invalid_email}, state}
      true ->
        user_id = :erlang.unique_integer([:positive])
        new_users = Map.put(state.users, user_id, %{username: username, email: email})
        notify("User created: #{username}")
        {:reply, {:ok, user_id}, %{state | users: new_users}}
    end
  end

  @impl true
  def handle_call({:create_session, user_id, ttl}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      session_id = :erlang.unique_integer([:positive])
      
      # Agendar expiração automática
      Process.send_after(self(), {:expire_session, session_id}, ttl * 1000)
      
      new_sessions = Map.put(state.sessions, session_id, %{user_id: user_id})
      notify("Session #{session_id} created for user #{user_id}")
      
      {:reply, {:ok, session_id}, %{state | sessions: new_sessions}}
    else
      {:reply, {:error, :user_not_found}, state}
    end
  end

  @impl true
  def handle_info({:expire_session, session_id}, state) do
    if Map.has_key?(state.sessions, session_id) do
      new_sessions = Map.delete(state.sessions, session_id)
      notify("Session #{session_id} expired")
      {:noreply, %{state | sessions: new_sessions}}
    else
      {:noreply, state}
    end
  end

  # --- Funções Auxiliares ---

  defp notify(message) do
    IO.puts("[NOTIFICATION] #{message}")
  end
end

# --- Exemplo de Execução ---
# {:ok, _pid} = UserSystem.start_link([])
# {:ok, uid} = UserSystem.create_user("Alice", "alice@example.com")
# {:ok, sid} = UserSystem.create_session(uid, 5) # Expira em 5 segundos
