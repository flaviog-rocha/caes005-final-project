# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Recuperar processos após falhas inesperadas.
# 3. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  use Supervisor

  @store Module.concat(__MODULE__, Store)
  @default_session_ttl 3_600

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # A tabela pertence ao supervisor e sobrevive à reinicialização do Agent.
    db = :ets.new(:users_sessions_db, [:set, :public, read_concurrency: true])

    children = [
      %{
        id: @store,
        start: {Agent, :start_link, [fn -> load_state(db) end, [name: @store]]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def register_user(username, password) do
    with {:ok, username} <- validate_username(username),
         :ok <- validate_password(password) do
      transact(fn state ->
        normalized = normalize_username(username)

        if Map.has_key?(state.usernames, normalized) do
          {{:error, {:validation, :username_taken}}, state}
        else
          id = state.next_user_id
          user = %{id: id, username: username, password_hash: hash_password(password)}

          updated =
            state
            |> put_in([:users, id], user)
            |> put_in([:usernames, normalized], id)
            |> Map.put(:next_user_id, id + 1)

          {{:ok, public_user(user)}, updated}
        end
      end)
    end
  end

  def authenticate(username, password, ttl \\ @default_session_ttl) do
    with {:ok, username} <- validate_username(username),
         :ok <- validate_password(password),
         :ok <- validate_ttl(ttl) do
      transact(fn state ->
        normalized = normalize_username(username)

        result =
          with user_id when not is_nil(user_id) <- Map.get(state.usernames, normalized),
               user when not is_nil(user) <- Map.get(state.users, user_id),
               true <- password_matches?(password, user.password_hash) do
            token = unique_token(state.sessions)
            expires_at = System.system_time(:second) + ttl

            session = %{
              token: token,
              user_id: user.id,
              created_at: System.system_time(:second),
              expires_at: expires_at
            }

            updated = put_in(state, [:sessions, token], session)
            {{:ok, public_session(session, user)}, updated}
          else
            _ -> {{:error, {:validation, :invalid_credentials}}, state}
          end

        result
      end)
    end
  end

  def get_session(token) when is_binary(token) do
    transact(fn state ->
      case Map.get(state.sessions, token) do
        nil ->
          {{:error, {:validation, :session_not_found}}, state}

        session when session.expires_at <= System.system_time(:second) ->
          updated = update_in(state.sessions, &Map.delete(&1, token))
          {{:error, {:validation, :session_expired}}, updated}

        session ->
          case Map.get(state.users, session.user_id) do
            nil ->
              updated = update_in(state.sessions, &Map.delete(&1, token))
              {{:error, {:validation, :user_not_found}}, updated}

            user ->
              {{:ok, public_session(session, user)}, state}
          end
      end
    end)
  end

  def get_session(_token), do: {:error, {:validation, :invalid_session_token}}

  def revoke_session(token) when is_binary(token) do
    transact(fn state ->
      if Map.has_key?(state.sessions, token) do
        updated = update_in(state.sessions, &Map.delete(&1, token))
        {{:ok, :revoked}, updated}
      else
        {{:error, {:validation, :session_not_found}}, state}
      end
    end)
  end

  def revoke_session(_token), do: {:error, {:validation, :invalid_session_token}}

  def get_user(id) when is_integer(id) do
    safely(fn ->
      Agent.get(@store, fn state ->
        case Map.get(state.users, id) do
          nil -> {:error, {:validation, :user_not_found}}
          user -> {:ok, public_user(user)}
        end
      end)
    end)
  end

  def get_user(_id), do: {:error, {:validation, :invalid_user_id}}

  def delete_user(id) when is_integer(id) do
    transact(fn state ->
      case Map.pop(state.users, id) do
        {nil, _users} ->
          {{:error, {:validation, :user_not_found}}, state}

        {user, users} ->
          normalized = normalize_username(user.username)

          sessions =
            state.sessions
            |> Enum.reject(fn {_token, session} -> session.user_id == id end)
            |> Map.new()

          updated = %{
            state
            | users: users,
              usernames: Map.delete(state.usernames, normalized),
              sessions: sessions
          }

          {{:ok, :deleted}, updated}
      end
    end)
  end

  def delete_user(_id), do: {:error, {:validation, :invalid_user_id}}

  defp validate_username(username) when is_binary(username) do
    username = String.trim(username)
    length = String.length(username)

    if length in 3..32 do
      {:ok, username}
    else
      {:error, {:validation, :invalid_username}}
    end
  end

  defp validate_username(_), do: {:error, {:validation, :invalid_username}}

  defp validate_password(password) when is_binary(password) do
    if byte_size(password) in 8..256 do
      :ok
    else
      {:error, {:validation, :invalid_password}}
    end
  end

  defp validate_password(_), do: {:error, {:validation, :invalid_password}}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0 and ttl <= 2_592_000, do: :ok
  defp validate_ttl(_), do: {:error, {:validation, :invalid_session_ttl}}

  defp normalize_username(username), do: String.downcase(username)

  defp hash_password(password), do: :crypto.hash(:sha256, password)

  defp password_matches?(password, stored_hash) do
    :crypto.hash_equals(hash_password(password), stored_hash)
  end

  defp unique_token(sessions) do
    token =
      :crypto.strong_rand_bytes(32)
      |> Base.url_encode64(padding: false)

    if Map.has_key?(sessions, token), do: unique_token(sessions), else: token
  end

  defp public_user(user) do
    %{id: user.id, username: user.username}
  end

  defp public_session(session, user) do
    %{
      token: session.token,
      user: public_user(user),
      created_at: session.created_at,
      expires_at: session.expires_at
    }
  end

  defp transact(operation) do
    safely(fn ->
      Agent.get_and_update(@store, fn state ->
        {result, updated_state} = operation.(state)
        persist_state(updated_state)
        {result, updated_state}
      end)
    end)
  end

  defp safely(operation) do
    try do
      operation.()
    rescue
      error -> {:error, {:operational, {:exception, error.__struct__}}}
    catch
      :exit, reason -> {:error, {:operational, {:process_unavailable, reason}}}
      kind, reason -> {:error, {:operational, {kind, reason}}}
    end
  end

  defp load_state(db) do
    case :ets.lookup(db, :state) do
      [{:state, persisted}] -> Map.put(persisted, :db, db)
      [] -> initial_state(db)
    end
  end

  defp initial_state(db) do
    %{db: db, users: %{}, usernames: %{}, sessions: %{}, next_user_id: 1}
  end

  defp persist_state(state) do
    persisted = Map.drop(state, [:db])
    :ets.insert(state.db, {:state, persisted})
    :ok
  end
end
