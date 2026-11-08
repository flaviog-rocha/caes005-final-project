# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  use GenServer

  @default_session_ttl_ms 3_600_000
  @default_sweep_interval_ms 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def subscribe(server \\ __MODULE__) do
    GenServer.call(server, {:subscribe, self()})
  end

  def create_user(attrs), do: create_user(__MODULE__, attrs)

  def create_user(server, attrs) do
    GenServer.call(server, {:create_user, attrs})
  end

  def get_user(id), do: get_user(__MODULE__, id)

  def get_user(server, id) do
    GenServer.call(server, {:get_user, id})
  end

  def delete_user(id), do: delete_user(__MODULE__, id)

  def delete_user(server, id) do
    GenServer.call(server, {:delete_user, id})
  end

  def create_session(user_id), do: create_session(__MODULE__, user_id, :default)

  def create_session(user_id, ttl_ms) do
    create_session(__MODULE__, user_id, ttl_ms)
  end

  def create_session(server, user_id, ttl_ms) do
    GenServer.call(server, {:create_session, user_id, ttl_ms})
  end

  def get_session(id), do: get_session(__MODULE__, id)

  def get_session(server, id) do
    GenServer.call(server, {:get_session, id})
  end

  def revoke_session(id), do: revoke_session(__MODULE__, id)

  def revoke_session(server, id) do
    GenServer.call(server, {:revoke_session, id})
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       users: %{},
       sessions: %{},
       subscribers: MapSet.new(),
       default_session_ttl_ms:
         positive_option(opts, :default_session_ttl_ms, @default_session_ttl_ms),
       sweep_interval_ms:
         positive_option(opts, :sweep_interval_ms, @default_sweep_interval_ms)
     }, {:continue, :schedule_sweep}}
  end

  @impl true
  def handle_continue(:schedule_sweep, state) do
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    state = expire_records(state)
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:create_user, attrs}, _from, state) do
    state = expire_records(state)

    case validate_user(attrs) do
      {:ok, name, email, ttl_ms} ->
        id = new_id()
        now = System.system_time(:millisecond)

        user = %{
          id: id,
          name: name,
          email: email,
          created_at_ms: now,
          expires_at_ms: expiry_time(ttl_ms)
        }

        state = put_in(state.users[id], user)
        notify(state, %{type: :user_created, user: user})
        {:reply, {:ok, user}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_user, id}, _from, state) do
    state = expire_records(state)

    case valid_id?(id) && Map.fetch(state.users, id) do
      {:ok, user} -> {:reply, {:ok, user}, state}
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_user, id}, _from, state) do
    state = expire_records(state)

    if valid_id?(id) do
      case Map.pop(state.users, id) do
        {nil, _users} ->
          {:reply, {:error, :not_found}, state}

        {user, users} ->
          {removed_sessions, sessions} =
            Enum.split_with(state.sessions, fn {_session_id, session} ->
              session.user_id == id
            end)

          state = %{state | users: users, sessions: Map.new(sessions)}

          Enum.each(removed_sessions, fn {_session_id, session} ->
            notify(state, %{type: :session_revoked, session: session, reason: :user_deleted})
          end)

          notify(state, %{type: :user_deleted, user: user})
          {:reply, :ok, state}
      end
    else
      {:reply, {:error, :invalid_id}, state}
    end
  end

  def handle_call({:create_session, user_id, requested_ttl}, _from, state) do
    state = expire_records(state)
    ttl_ms = if requested_ttl == :default, do: state.default_session_ttl_ms, else: requested_ttl

    cond do
      not valid_id?(user_id) ->
        {:reply, {:error, :invalid_user_id}, state}

      not valid_ttl?(ttl_ms) ->
        {:reply, {:error, :invalid_ttl}, state}

      not Map.has_key?(state.users, user_id) ->
        {:reply, {:error, :user_not_found}, state}

      true ->
        id = new_id()
        now = System.system_time(:millisecond)

        session = %{
          id: id,
          user_id: user_id,
          created_at_ms: now,
          expires_at_ms: expiry_time(ttl_ms)
        }

        state = put_in(state.sessions[id], session)
        notify(state, %{type: :session_created, session: session})
        {:reply, {:ok, session}, state}
    end
  end

  def handle_call({:get_session, id}, _from, state) do
    state = expire_records(state)

    case valid_id?(id) && Map.fetch(state.sessions, id) do
      {:ok, session} -> {:reply, {:ok, session}, state}
      _ -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:revoke_session, id}, _from, state) do
    state = expire_records(state)

    if valid_id?(id) do
      case Map.pop(state.sessions, id) do
        {nil, _sessions} ->
          {:reply, {:error, :not_found}, state}

        {session, sessions} ->
          state = %{state | sessions: sessions}
          notify(state, %{type: :session_revoked, session: session})
          {:reply, {:ok, session}, state}
      end
    else
      {:reply, {:error, :invalid_id}, state}
    end
  end

  @impl true
  def handle_info(:sweep_expired, state) do
    state = expire_records(state)
    schedule_sweep(state.sweep_interval_ms)
    {:noreply, state}
  end

  defp validate_user(attrs) when is_map(attrs) do
    name = Map.get(attrs, :name)
    email = Map.get(attrs, :email)
    ttl_ms = Map.get(attrs, :ttl_ms)

    cond do
      not (is_binary(name) and String.trim(name) != "") ->
        {:error, :invalid_name}

      not valid_email?(email) ->
        {:error, :invalid_email}

      not (is_nil(ttl_ms) or valid_ttl?(ttl_ms)) ->
        {:error, :invalid_ttl}

      true ->
        {:ok, String.trim(name), email, ttl_ms}
    end
  end

  defp validate_user(_), do: {:error, :invalid_user_data}

  defp valid_email?(email) when is_binary(email) do
    Regex.match?(~r/^[^\s@]+@[^\s@]+\.[^\s@]+$/, email)
  end

  defp valid_email?(_), do: false

  defp valid_ttl?(ttl_ms), do: is_integer(ttl_ms) and ttl_ms > 0

  defp valid_id?(id), do: is_integer(id) and id > 0

  defp expiry_time(nil), do: nil

  defp expiry_time(ttl_ms) do
    System.monotonic_time(:millisecond) + ttl_ms
  end

  defp expired?(nil, _now), do: false
  defp expired?(expires_at_ms, now), do: expires_at_ms <= now

  defp expire_records(state) do
    now = System.monotonic_time(:millisecond)

    expired_users =
      state.users
      |> Enum.filter(fn {_id, user} -> expired?(user.expires_at_ms, now) end)

    expired_user_ids = MapSet.new(Enum.map(expired_users, fn {id, _user} -> id end))

    expired_sessions =
      state.sessions
      |> Enum.filter(fn {_id, session} ->
        expired?(session.expires_at_ms, now) or MapSet.member?(expired_user_ids, session.user_id)
      end)

    users =
      Enum.reduce(expired_users, state.users, fn {id, _user}, acc ->
        Map.delete(acc, id)
      end)

    sessions =
      Enum.reduce(expired_sessions, state.sessions, fn {id, _session}, acc ->
        Map.delete(acc, id)
      end)

    state = %{state | users: users, sessions: sessions}

    Enum.each(expired_sessions, fn {_id, session} ->
      reason =
        if MapSet.member?(expired_user_ids, session.user_id),
          do: :user_expired,
          else: :expired

      notify(state, %{type: :session_expired, session: session, reason: reason})
    end)

    Enum.each(expired_users, fn {_id, user} ->
      notify(state, %{type: :user_expired, user: user})
    end)

    state
  end

  defp notify(state, event) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:user_session_notification, event})
    end)
  end

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep_expired, interval_ms)
  end

  defp new_id do
    System.unique_integer([:positive, :monotonic])
  end

  defp positive_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end
