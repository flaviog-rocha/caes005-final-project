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

defmodule UserSession.User do
  @moduledoc false

  @enforce_keys [:id, :username, :created_at]
  defstruct [:id, :username, :created_at]
end

defmodule UserSession.Session do
  @moduledoc false

  @enforce_keys [:token, :user_id, :expires_at]
  defstruct [:token, :user_id, :expires_at]
end

defmodule UserSession.Store do
  @moduledoc false
  use Agent

  alias UserSession.{Session, User}

  @state_key {__MODULE__, :state}
  @default_session_ttl 3_600

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Agent.start_link(fn -> load_state() end, name: name)
  end

  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def register(username, password, server \\ __MODULE__) do
    with {:ok, username} <- validate_username(username),
         :ok <- validate_password(password) do
      safe_call(fn ->
        Agent.get_and_update(server, fn state ->
          if Map.has_key?(state.username_index, username) do
            {{:error, {:validation, :username_taken}}, state}
          else
            id = System.unique_integer([:positive, :monotonic])

            user = %User{
              id: id,
              username: username,
              created_at: System.system_time(:second)
            }

            next_state =
              state
              |> put_in([:users, id], user)
              |> put_in([:username_index, username], id)
              |> put_in([:password_hashes, id], password_hash(password))
              |> persist()

            {{:ok, user}, next_state}
          end
        end)
      end)
    end
  end

  def login(username, password, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    ttl = Keyword.get(opts, :ttl, @default_session_ttl)

    with {:ok, username} <- validate_username(username),
         :ok <- validate_password(password),
         :ok <- validate_ttl(ttl) do
      safe_call(fn ->
        Agent.get_and_update(server, fn state ->
          case Map.fetch(state.username_index, username) do
            {:ok, user_id} ->
              if Map.get(state.password_hashes, user_id) == password_hash(password) do
                token = :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)

                session = %Session{
                  token: token,
                  user_id: user_id,
                  expires_at: System.system_time(:second) + ttl
                }

                next_state = put_in(state.sessions[token], session) |> persist()
                {{:ok, session}, next_state}
              else
                {{:error, {:validation, :invalid_credentials}}, state}
              end

            :error ->
              {{:error, {:validation, :invalid_credentials}}, state}
          end
        end)
      end)
    end
  end

  def get_user(user_id, server \\ __MODULE__) do
    if valid_user_id?(user_id) do
      safe_call(fn ->
        Agent.get(server, fn state ->
          case Map.fetch(state.users, user_id) do
            {:ok, user} -> {:ok, user}
            :error -> {:error, {:validation, :user_not_found}}
          end
        end)
      end)
    else
      {:error, {:validation, :invalid_user_id}}
    end
  end

  def session_user(token, server \\ __MODULE__) do
    if valid_token?(token) do
      safe_call(fn ->
        Agent.get_and_update(server, fn state ->
          case Map.fetch(state.sessions, token) do
            {:ok, %Session{expires_at: expires_at}} when expires_at <= System.system_time(:second) ->
              next_state = update_in(state.sessions, &Map.delete(&1, token)) |> persist()
              {{:error, {:validation, :session_expired}}, next_state}

            {:ok, %Session{user_id: user_id}} ->
              case Map.fetch(state.users, user_id) do
                {:ok, user} -> {{:ok, user}, state}
                :error -> {{:error, {:validation, :user_not_found}}, state}
              end

            :error ->
              {{:error, {:validation, :session_not_found}}, state}
          end
        end)
      end)
    else
      {:error, {:validation, :invalid_session_token}}
    end
  end

  def logout(token, server \\ __MODULE__) do
    if valid_token?(token) do
      safe_call(fn ->
        Agent.get_and_update(server, fn state ->
          if Map.has_key?(state.sessions, token) do
            next_state = update_in(state.sessions, &Map.delete(&1, token)) |> persist()
            {:ok, next_state}
          else
            {{:error, {:validation, :session_not_found}}, state}
          end
        end)
      end)
    else
      {:error, {:validation, :invalid_session_token}}
    end
  end

  def delete_user(user_id, server \\ __MODULE__) do
    if valid_user_id?(user_id) do
      safe_call(fn ->
        Agent.get_and_update(server, fn state ->
          case Map.pop(state.users, user_id) do
            {nil, _users} ->
              {{:error, {:validation, :user_not_found}}, state}

            {user, users} ->
              sessions =
                state.sessions
                |> Enum.reject(fn {_token, session} -> session.user_id == user_id end)
                |> Map.new()

              next_state =
                %{state | users: users, sessions: sessions}
                |> update_in([:username_index], &Map.delete(&1, user.username))
                |> update_in([:password_hashes], &Map.delete(&1, user_id))
                |> persist()

              {:ok, next_state}
          end
        end)
      end)
    else
      {:error, {:validation, :invalid_user_id}}
    end
  end

  defp validate_username(username) when is_binary(username) do
    normalized = String.trim(username)

    if byte_size(normalized) in 1..100 do
      {:ok, String.downcase(normalized)}
    else
      {:error, {:validation, :invalid_username}}
    end
  end

  defp validate_username(_), do: {:error, {:validation, :invalid_username}}

  defp validate_password(password) when is_binary(password) do
    if byte_size(password) in 8..1_024 do
      :ok
    else
      {:error, {:validation, :invalid_password}}
    end
  end

  defp validate_password(_), do: {:error, {:validation, :invalid_password}}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0 and ttl <= 31_536_000, do: :ok
  defp validate_ttl(_), do: {:error, {:validation, :invalid_session_ttl}}

  defp valid_user_id?(id), do: is_integer(id) and id > 0
  defp valid_token?(token), do: is_binary(token) and byte_size(token) > 0

  defp password_hash(password), do: :crypto.hash(:sha256, password)

  defp empty_state do
    %{users: %{}, username_index: %{}, password_hashes: %{}, sessions: %{}}
  end

  # Armazenamento fictício no nó atual: permite restaurar o Agent após reinício,
  # mas não substitui um banco de dados durável em produção.
  defp load_state do
    :persistent_term.get(@state_key, empty_state())
  end

  defp persist(state) do
    :persistent_term.put(@state_key, state)
    state
  end

  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error -> {:error, {:operation, {:exception, error}}}
    catch
      :exit, reason -> {:error, {:operation, {:store_unavailable, reason}}}
    end
  end
end

defmodule UserSession.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    Supervisor.init([UserSession.Store], strategy: :one_for_one)
  end
end

defmodule UserSession do
  @moduledoc false

  def start_link(opts \\ []), do: UserSession.Supervisor.start_link(opts)

  def register(username, password), do: UserSession.Store.register(username, password)
  def login(username, password), do: UserSession.Store.login(username, password)
  def get_user(user_id), do: UserSession.Store.get_user(user_id)
  def session_user(token), do: UserSession.Store.session_user(token)
  def logout(token), do: UserSession.Store.logout(token)
  def delete_user(user_id), do: UserSession.Store.delete_user(user_id)
end
