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

  @moduledoc """
  Gerencia usuários e sessões em memória, com validação, expiração automática
  e notificações enviadas aos processos inscritos.
  """

  @type manager :: GenServer.server()

  def start_link(opts \\ [])

  def start_link(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      GenServer.start_link(__MODULE__, opts, start_options(opts))
    else
      {:error, :invalid_options}
    end
  end

  def start_link(_opts), do: {:error, :invalid_options}

  def create_user(manager, attrs), do: GenServer.call(manager, {:create_user, attrs})
  def get_user(manager, user_id), do: GenServer.call(manager, {:get_user, user_id})
  def list_users(manager), do: GenServer.call(manager, :list_users)
  def delete_user(manager, user_id), do: GenServer.call(manager, {:delete_user, user_id})

  def login(manager, user_id), do: GenServer.call(manager, {:login, user_id})
  def validate_session(manager, token), do: GenServer.call(manager, {:validate_session, token})
  def logout(manager, token), do: GenServer.call(manager, {:logout, token})

  def subscribe(manager), do: GenServer.call(manager, {:subscribe, self()})
  def unsubscribe(manager), do: GenServer.call(manager, {:unsubscribe, self()})

  @impl true
  def init(opts) do
    if valid_options?(opts) do
      state = %{
        users: %{},
        sessions: %{},
        email_index: %{},
        username_index: %{},
        subscribers: MapSet.new(),
        session_ttl_ms: Keyword.get(opts, :session_ttl_ms, 3_600_000),
        user_ttl_ms: Keyword.get(opts, :user_ttl_ms, :infinity),
        sweep_interval_ms: Keyword.get(opts, :sweep_interval_ms, 1_000)
      }

      schedule_sweep(state.sweep_interval_ms)
      {:ok, state}
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:create_user, attrs}, _from, state) do
    state = expire_due(state, now())

    case validate_user_attrs(attrs) do
      {:ok, username, email} ->
        email_key = String.downcase(email)
        username_key = String.downcase(username)

        cond do
          Map.has_key?(state.email_index, email_key) ->
            {:reply, {:error, :email_taken}, state}

          Map.has_key?(state.username_index, username_key) ->
            {:reply, {:error, :username_taken}, state}

          true ->
            timestamp = now()
            id = new_id("u")
            expires_at = expiration(timestamp, state.user_ttl_ms)

            user = %{
              id: id,
              username: username,
              email: email,
              inserted_at: timestamp,
              expires_at: expires_at
            }

            state =
              state
              |> put_in([:users, id], user)
              |> put_in([:email_index, email_key], id)
              |> put_in([:username_index, username_key], id)
              |> notify(%{type: :user_created, user_id: id, at: timestamp})

            {:reply, {:ok, user}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_user, user_id}, _from, state) do
    state = expire_due(state, now())

    case validate_id(user_id) do
      :ok ->
        case Map.fetch(state.users, user_id) do
          {:ok, user} -> {:reply, {:ok, user}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:list_users, _from, state) do
    state = expire_due(state, now())
    users = state.users |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, users}, state}
  end

  def handle_call({:delete_user, user_id}, _from, state) do
    state = expire_due(state, now())

    case validate_id(user_id) do
      :ok ->
        case Map.fetch(state.users, user_id) do
          {:ok, user} ->
            state = remove_user(state, user, :deleted, now())
            {:reply, :ok, state}

          :error ->
            {:reply, {:error, :not_found}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:login, user_id}, _from, state) do
    state = expire_due(state, now())

    case validate_id(user_id) do
      :ok ->
        case Map.fetch(state.users, user_id) do
          {:ok, user} ->
            timestamp = now()
            token = new_token()

            session = %{
              token: token,
              user_id: user.id,
              created_at: timestamp,
              expires_at: expiration(timestamp, state.session_ttl_ms)
            }

            state =
              state
              |> put_in([:sessions, token], session)
              |> notify(%{
                type: :session_created,
                user_id: user.id,
                expires_at: session.expires_at,
                at: timestamp
              })

            {:reply, {:ok, session}, state}

          :error ->
            {:reply, {:error, :not_found}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:validate_session, token}, _from, state) do
    state = expire_due(state, now())

    case validate_token(token) do
      :ok ->
        case Map.fetch(state.sessions, token) do
          {:ok, session} -> {:reply, {:ok, session}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:logout, token}, _from, state) do
    state = expire_due(state, now())

    case validate_token(token) do
      :ok ->
        case Map.pop(state.sessions, token) do
          {nil, _sessions} ->
            {:reply, {:error, :not_found}, state}

          {session, sessions} ->
            state =
              %{state | sessions: sessions}
              |> notify(%{
                type: :session_ended,
                user_id: session.user_id,
                reason: :logout,
                at: now()
              })

            {:reply, :ok, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  @impl true
  def handle_info(:sweep, state) do
    state
    |> expire_due(now())
    |> then(fn state ->
      schedule_sweep(state.sweep_interval_ms)
      {:noreply, state}
    end)
  end

  defp validate_user_attrs(attrs) when is_map(attrs) do
    allowed = [:username, :email]

    cond do
      Enum.any?(Map.keys(attrs), &(not (&1 in allowed))) ->
        {:error, {:invalid, :attributes}}

      not is_binary(Map.get(attrs, :username)) ->
        {:error, {:invalid, :username}}

      not is_binary(Map.get(attrs, :email)) ->
        {:error, {:invalid, :email}}

      true ->
        username = String.trim(attrs.username)
        email = String.trim(attrs.email)

        cond do
          String.length(username) < 3 or String.length(username) > 32 ->
            {:error, {:invalid, :username}}

          not Regex.match?(~r/\A[a-zA-Z0-9_-]+\z/, username) ->
            {:error, {:invalid, :username}}

          String.length(email) > 254 or
              not Regex.match?(~r/\A[^\s@]+@[^\s@]+\.[^\s@]+\z/, email) ->
            {:error, {:invalid, :email}}

          true ->
            {:ok, username, email}
        end
    end
  end

  defp validate_user_attrs(_), do: {:error, {:invalid, :attributes}}

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= 128,
    do: :ok

  defp validate_id(_), do: {:error, {:invalid, :user_id}}

  defp validate_token(token) when is_binary(token) and byte_size(token) in 1..256, do: :ok
  defp validate_token(_), do: {:error, {:invalid, :token}}

  defp valid_options?(opts) do
    allowed = [:name, :session_ttl_ms, :user_ttl_ms, :sweep_interval_ms]

    Enum.all?(Keyword.keys(opts), &(&1 in allowed)) and
      valid_ttl?(Keyword.get(opts, :session_ttl_ms, 3_600_000)) and
      valid_ttl?(Keyword.get(opts, :user_ttl_ms, :infinity)) and
      is_integer(Keyword.get(opts, :sweep_interval_ms, 1_000)) and
      Keyword.get(opts, :sweep_interval_ms, 1_000) > 0
  end

  defp valid_ttl?(:infinity), do: true
  defp valid_ttl?(ttl), do: is_integer(ttl) and ttl > 0

  defp start_options(opts) do
    case Keyword.get(opts, :name) do
      nil -> []
      name -> [name: name]
    end
  end

  defp expiration(_timestamp, :infinity), do: nil
  defp expiration(timestamp, ttl), do: timestamp + ttl

  defp now, do: System.system_time(:millisecond)

  defp new_id(prefix) do
    prefix <> "_" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  defp new_token do
    :crypto.strong_rand_bytes(32)
    |> Base.url_encode64(padding: false)
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp expire_due(state, timestamp) do
    expired_users =
      state.users
      |> Map.values()
      |> Enum.filter(&expired?(&1.expires_at, timestamp))

    state =
      Enum.reduce(expired_users, state, fn user, acc ->
        remove_user(acc, user, :expired, timestamp)
      end)

    expired_sessions =
      state.sessions
      |> Enum.filter(fn {_token, session} -> expired?(session.expires_at, timestamp) end)

    Enum.reduce(expired_sessions, state, fn {token, session}, acc ->
      acc
      |> update_in([:sessions], &Map.delete(&1, token))
      |> notify(%{
        type: :session_expired,
        user_id: session.user_id,
        reason: :expired,
        at: timestamp
      })
    end)
  end

  defp expired?(nil, _timestamp), do: false
  defp expired?(expires_at, timestamp), do: expires_at <= timestamp

  defp remove_user(state, user, reason, timestamp) do
    email_key = String.downcase(user.email)
    username_key = String.downcase(user.username)

    sessions =
      Enum.reject(state.sessions, fn {_token, session} -> session.user_id == user.id end)

    state = %{
      state
      | users: Map.delete(state.users, user.id),
        sessions: Map.new(sessions),
        email_index: Map.delete(state.email_index, email_key),
        username_index: Map.delete(state.username_index, username_key)
    }

    state =
      Enum.reduce(sessions_for_user(state, user.id), state, fn session, acc ->
        notify(acc, %{
          type: :session_expired,
          user_id: session.user_id,
          reason: :user_removed,
          at: timestamp
        })
      end)

    notify(state, %{type: :user_expired, user_id: user.id, reason: reason, at: timestamp})
  end

  defp sessions_for_user(_state, _user_id), do: []

  defp notify(state, event) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:user_session_manager, event})
    end)

    state
  end
end
