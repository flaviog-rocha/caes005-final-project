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

# Arquivo autocontido: gerenciamento de usuários e sessões em Elixir.
# Execute com: elixir sessoes.exs

# ---------------------------------------------------------------------------
# Banco de dados fictício em memória (Agent).
# Simula as tabelas `users` e `sessions`. Se o domínio exigir persistência
# real, esta camada pode ser substituída por um Ecto.Repo (com migrations
# em priv/repo/migrations), mantendo a mesma interface pública.
# ---------------------------------------------------------------------------
defmodule Store do
  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{users: %{}, sessions: %{}} end, name: __MODULE__)
  end

  def put_user(%{id: id} = user) do
    Agent.update(__MODULE__, fn db -> %{db | users: Map.put(db.users, id, user)} end)
  end

  def get_user(id), do: Agent.get(__MODULE__, &Map.get(&1.users, id))

  def put_session(%{token: token} = session) do
    Agent.update(__MODULE__, fn db -> %{db | sessions: Map.put(db.sessions, token, session)} end)
  end

  def get_session(token), do: Agent.get(__MODULE__, &Map.get(&1.sessions, token))

  def delete_session(token) do
    Agent.update(__MODULE__, fn db -> %{db | sessions: Map.delete(db.sessions, token)} end)
  end

  def list_sessions, do: Agent.get(__MODULE__, &Map.values(&1.sessions))

  # Remove sessões expiradas e devolve a lista das que foram removidas.
  def delete_expired(ts) do
    Agent.get_and_update(__MODULE__, fn db ->
      {expired, kept} = Enum.split_with(db.sessions, fn {_token, s} -> s.expires_at <= ts end)
      {Enum.map(expired, &elem(&1, 1)), %{db | sessions: Map.new(kept)}}
    end)
  end
end

# ---------------------------------------------------------------------------
# Gerenciador de sessões.
# - Monitora o processo "dono" de cada sessão.
# - Encerramento inesperado do dono => sessão revogada e registrado em log.
# - Ao ser reiniciado pelo Supervisor, reancora os monitores às sessões
#   que ainda estão válidas no banco.
# ---------------------------------------------------------------------------
defmodule SessionManager do
  use GenServer
  require Logger

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def create_session(user_id, owner_pid \\ self(), ttl_ms \\ 60_000) do
    GenServer.call(__MODULE__, {:create, user_id, owner_pid, ttl_ms})
  end

  def valid?(token), do: GenServer.call(__MODULE__, {:valid?, token})

  def revoke(token), do: GenServer.call(__MODULE__, {:revoke, token})

  @impl true
  def init(:ok) do
    ts = now()

    # Recuperação após restart: reancora monitores em sessões ainda válidas.
    monitors =
      Enum.reduce(Store.list_sessions(), %{}, fn session, acc ->
        if session.expires_at > ts and Process.alive?(session.owner) do
          ref = Process.monitor(session.owner)
          Map.put(acc, ref, session.token)
        else
          Store.delete_session(session.token)
          acc
        end
      end)

    {:ok, %{monitors: monitors}}
  end

  @impl true
  def handle_call({:create, user_id, owner, ttl}, _from, state) do
    case Store.get_user(user_id) do
      nil ->
        {:reply, {:error, :user_not_found}, state}

      _user ->
        token = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

        Store.put_session(%{
          token: token,
          user_id: user_id,
          owner: owner,
          expires_at: now() + ttl
        })

        ref = Process.monitor(owner)
        {:reply, {:ok, token}, %{state | monitors: Map.put(state.monitors, ref, token)}}
    end
  end

  def handle_call({:valid?, token}, _from, state) do
    valid =
      case Store.get_session(token) do
        nil -> false
        session -> session.expires_at > now()
      end

    {:reply, valid, state}
  end

  def handle_call({:revoke, token}, _from, state) do
    Store.delete_session(token)
    {:reply, :ok, state}
  end

  # Detecção de encerramento de processos monitorados.
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {token, monitors} ->
        if reason == :normal do
          Logger.info("Dono #{inspect(pid)} encerrou normalmente; sessão revogada")
        else
          Logger.warning("Encerramento inesperado de #{inspect(pid)} (#{inspect(reason)}); sessão revogada")
        end

        Store.delete_session(token)
        {:noreply, %{state | monitors: monitors}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp now, do: System.system_time(:millisecond)
end

# ---------------------------------------------------------------------------
# Job periódico genérico.
# - Executa `fun` a cada `interval` ms usando Process.send_after/3.
# - Se `fun` falhar, o processo termina com erro e o Supervisor o reinicia.
#   O terminate/2 registra a falha antes do encerramento.
# ---------------------------------------------------------------------------
defmodule PeriodicJob do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init(opts) do
    interval = Keyword.fetch!(opts, :interval)
    fun = Keyword.fetch!(opts, :fun)
    schedule(interval)
    {:ok, %{interval: interval, fun: fun, runs: 0}}
  end

  @impl true
  def handle_info(:tick, state) do
    state.fun.()
    schedule(state.interval)
    {:noreply, %{state | runs: state.runs + 1}}
  end

  @impl true
  def terminate(:normal, _state), do: :ok
  def terminate(:shutdown, _state), do: :ok
  def terminate({:shutdown, _}, _state), do: :ok

  def terminate(reason, state) do
    Logger.error("Job periódico encerrou inesperadamente após #{state.runs} execuções: #{inspect(reason)}")
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end

# ---------------------------------------------------------------------------
# Supervisor raiz da aplicação (estratégia :one_for_one).
# Se um filho falhar, apenas ele é reiniciado.
# ---------------------------------------------------------------------------
defmodule Sessions.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Store,
      SessionManager,
      {PeriodicJob,
       name: :session_cleanup,
       interval: 1_000,
       fun: &Sessions.Supervisor.cleanup_expired/0}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end

  @doc false
  def cleanup_expired do
    expired = Store.delete_expired(System.system_time(:millisecond))

    if expired != [] do
      Logger.info("Sessões expiradas removidas: #{length(expired)}")
    end

    :ok
  end
end

# ---------------------------------------------------------------------------
# Demonstração (executada ao rodar o arquivo com `elixir`).
# ---------------------------------------------------------------------------
require Logger

{:ok, _sup} = Sessions.Supervisor.start_link([])

:ok = Store.put_user(%{id: 1, name: "Ana"})

# Processo "dono" da sessão, simulando, por exemplo, uma conexão de cliente.
owner = spawn(fn -> receive do: (:never -> :ok) end)

{:ok, token} = SessionManager.create_session(1, owner, 10_000)
IO.puts("Sessão válida após criação: #{SessionManager.valid?(token)}")

# Encerramento inesperado do dono: detectado via monitor e a sessão é revogada.
Process.exit(owner, :boom)
Process.sleep(100)
IO.puts("Sessão válida após crash do dono: #{SessionManager.valid?(token)}")

# Crash do próprio SessionManager: o Supervisor o reinicia.
old_pid = Process.whereis(SessionManager)
Process.exit(old_pid, :kill)
Process.sleep(100)
new_pid = Process.whereis(SessionManager)
IO.puts("SessionManager reiniciado pelo Supervisor: #{new_pid != nil and new_pid != old_pid}")

# Aguarda algumas execuções do job periódico de limpeza.
Process.sleep(2_500)
