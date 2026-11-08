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

# Arquivo único: gerenciamento de usuários e sessões com Supervisor,
# tarefas periódicas, detecção de encerramentos inesperados e banco fictício.
# Execute com: elixir sessoes.exs

defmodule UserSessionSystem do
  @moduledoc """
  Árvore de supervisão do sistema.

  Estratégia `:rest_for_one`: se o banco fictício cair, todos os processos
  dependentes (gerenciador de sessões e agendador) são reiniciados junto,
  mantendo a consistência do estado. Se apenas um processo posterior cair,
  somente ele e os que vêm depois são reiniciados.
  """
  use Supervisor

  @task_sup UserSessionSystem.TaskSup

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    jobs = Keyword.get(opts, :jobs, default_jobs())

    children = [
      # Banco de dados fictício (Agent em memória)
      UserSessionSystem.FakeDB,
      # Supervisor de tarefas assíncronas usadas pelo agendador
      {Task.Supervisor, name: @task_sup},
      # Regras de negócio de usuários e sessões
      UserSessionSystem.SessionManager,
      # Execução periódica de operações
      {UserSessionSystem.Scheduler, jobs: jobs, task_sup: @task_sup}
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 5, max_seconds: 10)
  end

  defp default_jobs do
    [
      # {nome, intervalo_ms, {módulo, função, argumentos}}
      {:purge_expired_sessions, 1_000, {UserSessionSystem.SessionManager, :purge_expired, []}}
    ]
  end
end

defmodule UserSessionSystem.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.
  Não depende de Ecto; serve apenas para simular persistência.
  """
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{users: %{}, sessions: %{}, next_id: 1} end, name: __MODULE__)
  end

  # ---------- Usuários ----------

  def insert_user(email, salt, hash) do
    Agent.get_and_update(__MODULE__, fn db ->
      if Enum.any?(db.users, fn {_id, u} -> u.email == email end) do
        {{:error, :email_taken}, db}
      else
        id = db.next_id
        user = %{id: id, email: email, salt: salt, hash: hash}
        {{:ok, user}, %{db | users: Map.put(db.users, id, user), next_id: id + 1}}
      end
    end)
  end

  def find_user_by_email(email) do
    Agent.get(__MODULE__, fn db ->
      Enum.find_value(db.users, {:error, :not_found}, fn {_id, u} ->
        if u.email == email, do: {:ok, u}
      end)
    end)
  end

  # ---------- Sessões ----------

  def put_session(session) do
    Agent.update(__MODULE__, fn db ->
      %{db | sessions: Map.put(db.sessions, session.token, session)}
    end)
  end

  def get_session(token) do
    Agent.get(__MODULE__, fn db -> Map.fetch(db.sessions, token) end)
  end

  def delete_session(token) do
    Agent.update(__MODULE__, fn db ->
      %{db | sessions: Map.delete(db.sessions, token)}
    end)
  end

  def delete_sessions_where(predicate) do
    Agent.get_and_update(__MODULE__, fn db ->
      {removed, kept} = Enum.split_with(db.sessions, fn {_token, s} -> predicate.(s) end)
      {removed, %{db | sessions: Map.new(kept)}}
    end)
  end

  def list_sessions do
    Agent.get(__MODULE__, fn db -> Map.values(db.sessions) end)
  end
end

defmodule UserSessionSystem.SessionManager do
  @moduledoc """
  Gerencia usuários e sessões.

  - Sessões são associadas ao processo que fez o login (owner).
  - Um monitor é criado sobre o owner. Se ele terminar de forma inesperada,
    a sessão é revogada e um alerta é registrado.
  - Na reinicialização (feita pelo Supervisor), a reconciliação recria os
    monitores e remove sessões órfãs.
  """
  use GenServer
  require Logger

  alias UserSessionSystem.FakeDB

  # ---------- API pública ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register_user(email, password) do
    salt = :crypto.strong_rand_bytes(16)

    case FakeDB.insert_user(email, salt, hash(salt, password)) do
      {:ok, user} -> {:ok, user.id}
      {:error, _reason} = error -> error
    end
  end

  def login(email, password, ttl_ms \\ 3_600_000) do
    GenServer.call(__MODULE__, {:login, email, password, ttl_ms})
  end

  def logout(token), do: GenServer.call(__MODULE__, {:logout, token})

  def validate(token), do: GenServer.call(__MODULE__, {:validate, token})

  def purge_expired, do: GenServer.call(__MODULE__, :purge_expired)

  # ---------- Callbacks ----------

  @impl true
  def init(_opts) do
    {:ok, reconcile(%{monitors: %{}})}
  end

  @impl true
  def handle_call({:login, email, password, ttl_ms}, {owner, _tag}, state) do
    with {:ok, user} <- FakeDB.find_user_by_email(email),
         :ok <- check_password(user, password) do
      token = new_token()

      session = %{
        token: token,
        user_id: user.id,
        owner: owner,
        expires_at: now() + ttl_ms
      }

      FakeDB.put_session(session)
      ref = Process.monitor(owner)

      {:reply, {:ok, token}, %{state | monitors: Map.put(state.monitors, ref, token)}}
    else
      _ -> {:reply, {:error, :invalid_credentials}, state}
    end
  end

  def handle_call({:logout, token}, _from, state) do
    FakeDB.delete_session(token)
    {:reply, :ok, drop_monitor(state, token)}
  end

  def handle_call({:validate, token}, _from, state) do
    reply =
      case FakeDB.get_session(token) do
        {:ok, session} ->
          if session.expires_at > now(), do: {:ok, session.user_id}, else: {:error, :expired}

        :error ->
          {:error, :invalid}
      end

    {:reply, reply, state}
  end

  def handle_call(:purge_expired, _from, state) do
    current = now()
    removed = FakeDB.delete_sessions_where(fn s -> s.expires_at <= current end)

    state =
      Enum.reduce(removed, state, fn {token, _session}, acc -> drop_monitor(acc, token) end)

    {:reply, length(removed), state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {token, monitors} ->
        state = %{state | monitors: monitors}

        if normal_exit?(reason) do
          # Encerramento esperado: a sessão continua válida até expirar
          {:noreply, state}
        else
          Logger.warning(
            "Processo #{inspect(pid)} encerrou inesperadamente (#{inspect(reason)}). " <>
              "Revogando sessão #{token}."
          )

          FakeDB.delete_session(token)
          {:noreply, state}
        end
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Auxiliares privados ----------

  # Reconstrói monitores após reinício e remove sessões cujo dono já morreu
  defp reconcile(state) do
    Enum.reduce(FakeDB.list_sessions(), state, fn session, acc ->
      if Process.alive?(session.owner) do
        ref = Process.monitor(session.owner)
        %{acc | monitors: Map.put(acc.monitors, ref, session.token)}
      else
        FakeDB.delete_session(session.token)
        Logger.warning("Sessão órfã removida na reconciliação: #{session.token}")
        acc
      end
    end)
  end

  defp drop_monitor(state, token) do
    case Enum.find(state.monitors, fn {_ref, t} -> t == token end) do
      {ref, _token} ->
        Process.demonitor(ref, [:flush])
        %{state | monitors: Map.delete(state.monitors, ref)}

      nil ->
        state
    end
  end

  defp normal_exit?(:normal), do: true
  defp normal_exit?(:shutdown), do: true
  defp normal_exit?({:shutdown, _}), do: true
  defp normal_exit?(_), do: false

  defp check_password(user, password) do
    if user.hash == hash(user.salt, password), do: :ok, else: :error
  end

  defp hash(salt, password), do: :crypto.hash(:sha256, salt <> password)

  defp new_token, do: Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

  defp now, do: System.system_time(:millisecond)
end

defmodule UserSessionSystem.Scheduler do
  @moduledoc """
  Executa operações periodicamente.

  Cada job roda em uma Task supervisionada (async_nolink). Falhas nas tarefas
  são detectadas via mensagens :DOWN e registradas, sem derrubar o agendador.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    task_sup = Keyword.fetch!(opts, :task_sup)

    jobs =
      opts
      |> Keyword.fetch!(:jobs)
      |> Map.new(fn {name, interval, mfa} -> {name, %{interval: interval, mfa: mfa}} end)

    Enum.each(jobs, fn {name, job} -> schedule(name, job.interval) end)

    {:ok, %{task_sup: task_sup, jobs: jobs, tasks: %{}}}
  end

  @impl true
  def handle_info({:run, name}, state) do
    case Map.fetch(state.jobs, name) do
      {:ok, %{interval: interval, mfa: {m, f, a}}} ->
        # Reagenda antes de executar, garantindo periodicidade
        schedule(name, interval)
        task = Task.Supervisor.async_nolink(state.task_sup, m, f, a)
        {:noreply, %{state | tasks: Map.put(state.tasks, task.ref, name)}}

      :error ->
        {:noreply, state}
    end
  end

  # Resultado bem-sucedido de uma tarefa
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {name, tasks} ->
        Process.demonitor(ref, [:flush])
        Logger.debug("Job #{inspect(name)} concluído: #{inspect(result)}")
        {:noreply, %{state | tasks: tasks}}
    end
  end

  # Encerramento inesperado de uma tarefa
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {name, tasks} ->
        if reason != :normal do
          Logger.error("Job #{inspect(name)} falhou: #{inspect(reason)}")
        end

        {:noreply, %{state | tasks: tasks}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule(name, interval) do
    Process.send_after(self(), {:run, name}, interval)
  end
end

defmodule UserSessionSystem.Demo do
  @moduledoc false
  alias UserSessionSystem.SessionManager, as: SM

  def run do
    {:ok, _id} = SM.register_user("ana@exemplo.com", "senha123")

    parent = self()

    # Cliente que faz login e termina de forma inesperada
    spawn(fn ->
      {:ok, token} = SM.login("ana@exemplo.com", "senha123", 60_000)
      send(parent, {:token, token})
      Process.sleep(100)
      exit(:falha_simulada)
    end)

    token =
      receive do
        {:token, t} -> t
      after
        2_000 -> raise "timeout aguardando token"
      end

    Process.sleep(300)
    IO.inspect(SM.validate(token), label: "sessão após crash do cliente")

    # Sessão de curta duração, mantida pelo processo atual
    {:ok, short_token} = SM.login("ana@exemplo.com", "senha123", 500)
    IO.inspect(SM.validate(short_token), label: "sessão recém-criada")

    Process.sleep(1_500)
    IO.inspect(SM.validate(short_token), label: "após expiração e limpeza periódica")
  end
end

{:ok, _sup} = UserSessionSystem.start_link()
UserSessionSystem.Demo.run()
