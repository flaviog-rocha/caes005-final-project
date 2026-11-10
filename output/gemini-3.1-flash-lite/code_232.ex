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
  notificações e expiração automática usando GenServer.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{users: %{}, sessions: %{}}, name: __MODULE__)

  def create_user(username, email) do
    GenServer.call(__MODULE__, {:create_user, %{username: username, email: email}})
  end

  def create_session(user_id, ttl_seconds) do
    GenServer.call(__MODULE__, {:create_session, user_id, ttl_seconds})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:create_user, data}, _from, state) do
    with {:ok, valid_data} <- validate_user(data),
         false <- Map.has_key?(state.users, valid_data.email) do
      new_id = System.unique_integer([:positive])
      new_state = put_in(state, [:users, new_id], Map.put(valid_data, :id, new_id))
      
      notify("Usuário criado: #{valid_data.email}")
      {:reply, {:ok, new_id}, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      true -> {:reply, {:error, :user_exists}, state}
    end
  end

  @impl true
  def handle_call({:create_session, user_id, ttl}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      session_id = :crypto.strong_rand_bytes(16) |> Base.encode16()
      expiry = System.system_time(:second) + ttl
      
      # Agenda expiração
      Process.send_after(self(), {:expire_session, session_id}, ttl * 1000)
      
      new_state = put_in(state, [:sessions, session_id], %{user_id: user_id, expiry: expiry})
      {:reply, {:ok, session_id}, new_state}
    else
      {:reply, {:error, :user_not_found}, state}
    end
  end

  @impl true
  def handle_info({:expire_session, session_id}, state) do
    if Map.has_key?(state.sessions, session_id) do
      notify("Sessão expirada: #{session_id}")
      {:noreply, %{state | sessions: Map.delete(state.sessions, session_id)}}
    else
      {:noreply, state}
    end
  end

  # --- Helpers Internos ---

  defp validate_user(%{username: u, email: e}) when is_binary(u) and is_binary(e) do
    if String.contains?(e, "@"), do: {:ok, %{username: u, email: e}}, else: {:error, :invalid_email}
  end
  defp validate_user(_), do: {:error, :invalid_params}

  defp notify(message) do
    IO.puts("[NOTIFICAÇÃO] #{message}")
  end
end
