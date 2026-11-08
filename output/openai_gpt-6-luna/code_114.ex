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

defmodule UserSessions.UserStore do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def create_user(name), do: GenServer.call(__MODULE__, {:create_user, name})
  def get_user(user_id), do: GenServer.call(__MODULE__, {:get_user, user_id})
  def delete_user(user_id), do: GenServer.call(__MODULE__, {:delete_user, user_id})

  @impl true
  def init(users), do: {:ok, users}

  @impl true
  def handle_call({:create_user, name}, _from, users) when is_binary(name) do
    user_id = System.unique_integer([:positive])

    user = %{
      id: user_id,
      name: name,
      created_at: System.system_time(:second)
    }

    {:reply, {:ok, user}, Map.put(users, user_id, user)}
  end

  def handle_call({:create_user, _name}, _from, users) do
    {:reply, {:error, :invalid_name}, users}
  end

  def handle_call({:get_user, user_id}, _from, users) do
    case Map.fetch(users, user_id) do
      {:ok, user} -> {:reply, {:ok, user}, users}
      :error -> {:reply, {:error, :not_found}, users}
    end
  end

  def handle_call({:delete_user, user_id}, _from, users) do
    if Map.has_key?(users, user_id) do
      {:reply, :ok, Map.delete(users, user_id)}
    else
      {:reply, {:error, :not_found}, users}
    end
  end
end

defmodule UserSessions.Session do
  use GenServer

  def start_link({user_id, ttl_ms}) do
    GenServer.start_link(__MODULE__, {user_id, ttl_ms})
  end

  def child_spec({user_id, ttl_ms}) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [{user_id, ttl_ms}]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def touch(session_pid), do: GenServer.call(session_pid, :touch)
  def expired?(session_pid), do: GenServer.call(session_pid, :expired?)

  @impl true
  def init({user_id, ttl_ms}) do
    now = System.monotonic_time(:millisecond)
    {:ok, %{user_id: user_id, ttl_ms: ttl_ms, last_seen: now}}
  end

  @impl true
  def handle_call(:touch, _from, state) do
    now = System.monotonic_time(:millisecond)
    {:reply, :ok, %{state | last_seen: now}}
  end

  def handle_call(:expired?, _from, state) do
    now = System.monotonic_time(:millisecond)
    {:reply, now - state.last_seen >= state.ttl_ms, state}
  end
end

defmodule UserSessions.SessionSupervisor do
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok), do: DynamicSupervisor.init(strategy: :one_for_one)
end

defmodule UserSessions.SessionManager do
  use GenServer

  @maintenance_interval 30_000
  @default_ttl 1_800_000

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def start_session(user_id, ttl_ms \\ @default_ttl) do
    GenServer.call(__MODULE__, {:start_session, user_id, ttl_ms})
  end

  def end_session(session_pid) do
    GenServer.call(__MODULE__, {:end_session, session_pid})
  end

  def end_user_sessions(user_id) do
    GenServer.call(__MODULE__, {:end_user_sessions, user_id})
  end

  def list_sessions do
    GenServer.call(__MODULE__, :list_sessions)
  end

  @impl true
  def init(sessions) do
    schedule_maintenance()
    {:ok, sessions}
  end

  @impl true
  def handle_call({:start_session, user_id, ttl_ms}, _from, sessions)
      when is_integer(ttl_ms) and ttl_ms > 0 do
    with {:ok, _user} <- UserSessions.UserStore.get_user(user_id),
         {:ok, pid} <-
           DynamicSupervisor.start_child(
             UserSessions.SessionSupervisor,
             {UserSessions.Session, {user_id, ttl_ms}}
           ) do
      ref = Process.monitor(pid)
      {:reply, {:ok, pid}, Map.put(sessions, pid, %{ref: ref, user_id: user_id})}
    else
      {:error, :not_found} -> {:reply, {:error, :user_not_found}, sessions}
      {:error, reason} -> {:reply, {:error, reason}, sessions}
    end
  end

  def handle_call({:start_session, _user_id, _ttl_ms}, _from, sessions) do
    {:reply, {:error, :invalid_ttl}, sessions}
  end

  def handle_call({:end_session, pid}, _from, sessions) do
    if Map.has_key?(sessions, pid) do
      reply =
        case DynamicSupervisor.terminate_child(UserSessions.SessionSupervisor, pid) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:reply, reply, sessions}
    else
      {:reply, {:error, :not_found}, sessions}
    end
  end

  def handle_call({:end_user_sessions, user_id}, _from, sessions) do
    matching_pids =
      for {pid, %{user_id: session_user_id}} <- sessions,
          session_user_id == user_id,
          do: pid

    Enum.each(matching_pids, fn pid ->
      DynamicSupervisor.terminate_child(UserSessions.SessionSupervisor, pid)
    end)

    {:reply, :ok, sessions}
  end

  def handle_call(:list_sessions, _from, sessions) do
    result =
      Enum.map(sessions, fn {pid, %{user_id: user_id}} ->
        %{pid: pid, user_id: user_id}
      end)

    {:reply, result, sessions}
  end

  @impl true
  def handle_info(:maintenance, sessions) do
    Enum.each(sessions, fn {pid, _info} ->
      try do
        if UserSessions.Session.expired?(pid) do
          DynamicSupervisor.terminate_child(UserSessions.SessionSupervisor, pid)
        end
      catch
        :exit, _reason -> :ok
      end
    end)

    schedule_maintenance()
    {:noreply, sessions}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, sessions) do
    case Map.get(sessions, pid) do
      %{ref: ^ref} ->
        unless reason in [:normal, :shutdown] or match?({:shutdown, _}, reason) do
          IO.puts(:stderr, "Sessão encerrada inesperadamente (#{inspect(pid)}): #{inspect(reason)}")
        end

        {:noreply, Map.delete(sessions, pid)}

      _ ->
        {:noreply, sessions}
    end
  end

  defp schedule_maintenance do
    Process.send_after(self(), :maintenance, @maintenance_interval)
  end
end

defmodule UserSessions.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      UserSessions.UserStore,
      UserSessions.SessionSupervisor,
      UserSessions.SessionManager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule UserSessions do
  def start_link(opts \\ []), do: UserSessions.Supervisor.start_link(opts)

  def create_user(name), do: UserSessions.UserStore.create_user(name)
  def get_user(user_id), do: UserSessions.UserStore.get_user(user_id)

  def delete_user(user_id) do
    UserSessions.SessionManager.end_user_sessions(user_id)
    UserSessions.UserStore.delete_user(user_id)
  end

  def start_session(user_id, ttl_ms \\ 1_800_000) do
    UserSessions.SessionManager.start_session(user_id, ttl_ms)
  end

  def touch_session(session_pid), do: UserSessions.Session.touch(session_pid)
  def end_session(session_pid), do: UserSessions.SessionManager.end_session(session_pid)
  def list_sessions, do: UserSessions.SessionManager.list_sessions()
end
