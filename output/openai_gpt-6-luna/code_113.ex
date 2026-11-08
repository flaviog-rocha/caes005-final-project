# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSession.Manager do
  use GenServer

  @cleanup_interval_ms 60_000
  @default_session_ttl_ms 3_600_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def create_user(attrs, server \\ __MODULE__) do
    GenServer.call(server, {:create_user, attrs})
  end

  def get_user(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:get_user, user_id})
  end

  def list_users(server \\ __MODULE__) do
    GenServer.call(server, :list_users)
  end

  def delete_user(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:delete_user, user_id})
  end

  def create_session(user_id, ttl_ms \\ @default_session_ttl_ms, server \\ __MODULE__) do
    GenServer.call(server, {:create_session, user_id, ttl_ms})
  end

  def get_session(session_id, server \\ __MODULE__) do
    GenServer.call(server, {:get_session, session_id})
  end

  def revoke_session(session_id, server \\ __MODULE__) do
    GenServer.call(server, {:revoke_session, session_id})
  end

  @impl true
  def init(_opts) do
    schedule_cleanup()

    {:ok,
     %{
       users: %{},
       sessions: %{},
       next_user_id: 1
     }}
  end

  @impl true
  def handle_call({:create_user, attrs}, _from, state) when is_map(attrs) do
    user_id = state.next_user_id
    user = Map.put(attrs, :id, user_id)

    {:reply, {:ok, user},
     %{
       state
       | users: Map.put(state.users, user_id, user),
         next_user_id: user_id + 1
     }}
  end

  def handle_call({:create_user, _attrs}, _from, state) do
    {:reply, {:error, :invalid_attributes}, state}
  end

  def handle_call({:get_user, user_id}, _from, state) do
    case Map.fetch(state.users, user_id) do
      {:ok, user} -> {:reply, {:ok, user}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_users, _from, state) do
    {:reply, Map.values(state.users), state}
  end

  def handle_call({:delete_user, user_id}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      sessions =
        state.sessions
        |> Enum.reject(fn {_session_id, session} -> session.user_id == user_id end)
        |> Map.new()

      {:reply, :ok, %{state | users: Map.delete(state.users, user_id), sessions: sessions}}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:create_session, user_id, ttl_ms}, _from, state)
      when is_integer(ttl_ms) and ttl_ms > 0 do
    if Map.has_key?(state.users, user_id) do
      session_id = make_ref()
      now = System.system_time(:millisecond)

      session = %{
        id: session_id,
        user_id: user_id,
        created_at_ms: now,
        expires_at_ms: now + ttl_ms
      }

      {:reply, {:ok, session},
       %{state | sessions: Map.put(state.sessions, session_id, session)}}
    else
      {:reply, {:error, :user_not_found}, state}
    end
  end

  def handle_call({:create_session, _user_id, _ttl_ms}, _from, state) do
    {:reply, {:error, :invalid_ttl}, state}
  end

  def handle_call({:get_session, session_id}, _from, state) do
    now = System.system_time(:millisecond)

    case Map.fetch(state.sessions, session_id) do
      {:ok, %{expires_at_ms: expires_at_ms}} when expires_at_ms <= now ->
        {:reply, {:error, :expired}, %{state | sessions: Map.delete(state.sessions, session_id)}}

      {:ok, session} ->
        {:reply, {:ok, session}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:revoke_session, session_id}, _from, state) do
    if Map.has_key?(state.sessions, session_id) do
      {:reply, :ok, %{state | sessions: Map.delete(state.sessions, session_id)}}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info(:cleanup_expired_sessions, state) do
    now = System.system_time(:millisecond)

    sessions =
      state.sessions
      |> Enum.reject(fn {_id, session} -> session.expires_at_ms <= now end)
      |> Map.new()

    schedule_cleanup()
    {:noreply, %{state | sessions: sessions}}
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup_expired_sessions, @cleanup_interval_ms)
  end
end

defmodule UserSession.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    children = [UserSession.Manager]

    # O Supervisor monitora o processo gerente e o reinicia se ele terminar inesperadamente.
    Supervisor.init(children, strategy: :one_for_one)
  end
end
