# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de usuários e sessões em Elixir
#
# Requisitos atendidos:
#   1. Limite de operações simultâneas: Sessions.Limiter (semáforo contador via GenServer).
#   2. Detecção de encerramentos inesperados: monitores em Sessions.Limiter e Sessions.Manager.
#   3. Uso de macros: Sessions.Macros.defoperation/2.
#   4. Logger: registros em todos os módulos.

# ---------------------------------------------------------------------------
# Macros: envolvem funções públicas com limite de concorrência e logging
# ---------------------------------------------------------------------------
defmodule Sessions.Macros do
  @moduledoc """
  Macros que definem operações protegidas por limite de concorrência.

  Uso:

      defoperation login(user_id) do
        Sessions.Manager.login(user_id)
      end
  """

  require Logger

  @doc """
  Define uma função cujo corpo é executado dentro do limitador de concorrência,
  com registro de início, fim, falhas e rejeições por limite atingido.
  """
  defmacro defoperation({name, _meta, _args} = head, do: body) do
    op_name = Atom.to_string(name)

    quote do
      def unquote(head) do
        Sessions.Macros.run_operation(unquote(op_name), fn -> unquote(body) end)
      end
    end
  end

  @doc false
  def run_operation(name, fun) do
    case Sessions.Limiter.run(fn -> execute(name, fun) end) do
      {:ok, result} ->
        result

      {:error, :limit_reached} = error ->
        Logger.warning("Operação #{name} rejeitada: limite de concorrência atingido")
        error
    end
  end

  defp execute(name, fun) do
    Logger.debug("Operação #{name} iniciada")

    try do
      result = fun.()
      Logger.debug("Operação #{name} concluída")
      result
    rescue
      exception ->
        Logger.error("Operação #{name} falhou: #{Exception.message(exception)}")
        reraise exception, __STACKTRACE__
    end
  end
end

# ---------------------------------------------------------------------------
# Limitador de operações simultâneas (semáforo contador)
# ---------------------------------------------------------------------------
defmodule Sessions.Limiter do
  @moduledoc """
  Limita o número de operações executadas simultaneamente.

  Cada operação adquirida é monitorada: se o processo que a executa morrer
  de forma inesperada, a vaga é liberada automaticamente e o encerramento
  é registrado.
  """

  use GenServer
  require Logger

  @default_max 10

  @doc "Inicia o limitador. Opções: `:max` (padrão #{@default_max}) e `:name`."
  def start_link(opts \\ []) do
    max = Keyword.get(opts, :max, @default_max)
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, max, name: name)
  end

  @doc """
  Executa `fun` dentro de uma vaga do limitador.

  Retorna `{:ok, resultado}` ou `{:error, :limit_reached}`.
  """
  def run(fun, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)

    case GenServer.call(server, :acquire) do
      {:ok, ref} ->
        try do
          {:ok, fun.()}
        after
          GenServer.cast(server, {:release, ref})
        end

      {:error, :limit_reached} = error ->
        error
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag}, %{max: max, active: active} = state) do
    if map_size(active) < max do
      ref = Process.monitor(pid)
      {:reply, {:ok, ref}, %{state | active: Map.put(active, ref, pid)}}
    else
      {:reply, {:error, :limit_reached}, state}
    end
  end

  @impl true
  def handle_cast({:release, ref}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | active: Map.delete(state.active, ref)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _} ->
        {:noreply, state}

      {_pid, active} ->
        if reason != :normal do
          Logger.error(
            "Processo #{inspect(pid)} encerrou inesperadamente durante operação: #{inspect(reason)}. Vaga liberada."
          )
        end

        {:noreply, %{state | active: active}}
    end
  end
end

# ---------------------------------------------------------------------------
# Gerenciador de usuários e sessões
# ---------------------------------------------------------------------------
defmodule Sessions.Manager do
  @moduledoc """
  Mantém usuários e sessões. Cada sessão é um processo monitorado;
  encerramentos anormais são detectados e registrados, e a sessão é removida.
  """

  use GenServer
  require Logger

  # Tempo de inatividade (ms) até a sessão expirar.
  @session_ttl 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc false
  def create_user(name), do: GenServer.call(__MODULE__, {:create_user, name})

  @doc false
  def login(user_id), do: GenServer.call(__MODULE__, {:login, user_id})

  @doc false
  def logout(token), do: GenServer.call(__MODULE__, {:logout, token})

  @doc false
  def touch(token), do: GenServer.call(__MODULE__, {:touch, token})

  @doc false
  def crash_session(token, reason), do: GenServer.call(__MODULE__, {:crash, token, reason})

  @doc false
  def list_sessions, do: GenServer.call(__MODULE__, :list_sessions)

  @impl true
  def init(_opts) do
    {:ok, %{users: %{}, next_id: 1, sessions: %{}, refs: %{}}}
  end

  @impl true
  def handle_call({:create_user, name}, _from, state) do
    id = state.next_id
    users = Map.put(state.users, id, %{name: name})
    Logger.info("Usuário #{id} (#{name}) criado")
    {:reply, {:ok, id}, %{state | users: users, next_id: id + 1}}
  end

  def handle_call({:login, user_id}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      token = new_token()
      ttl = @session_ttl
      pid = spawn(fn -> session_loop(ttl) end)
      ref = Process.monitor(pid)

      session = %{user_id: user_id, pid: pid, ref: ref}

      state = %{
        state
        | sessions: Map.put(state.sessions, token, session),
          refs: Map.put(state.refs, ref, token)
      }

      Logger.info("Sessão #{token} iniciada para usuário #{user_id}")
      {:reply, {:ok, token}, state}
    else
      Logger.warning("Tentativa de login para usuário inexistente: #{user_id}")
      {:reply, {:error, :user_not_found}, state}
    end
  end

  def handle_call({:logout, token}, _from, state) do
    case Map.fetch(state.sessions, token) do
      {:ok, %{pid: pid, ref: ref}} ->
        send(pid, :logout)
        Process.demonitor(ref, [:flush])
        Logger.info("Sessão #{token} encerrada pelo usuário")
        {:reply, :ok, drop_session(state, token, ref)}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:touch, token}, _from, state) do
    case Map.fetch(state.sessions, token) do
      {:ok, %{pid: pid}} ->
        send(pid, :touch)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # Usado para simular falhas em sessões.
  def handle_call({:crash, token, reason}, _from, state) do
    case Map.fetch(state.sessions, token) do
      {:ok, %{pid: pid}} ->
        send(pid, {:fail, reason})
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_sessions, _from, state) do
    list =
      Enum.map(state.sessions, fn {token, %{user_id: uid}} -> {token, uid} end)

    {:reply, list, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _} ->
        {:noreply, state}

      {token, refs} ->
        if reason in [:normal, :shutdown] do
          Logger.info("Sessão #{token} finalizada (#{inspect(reason)})")
        else
          Logger.error(
            "Encerramento inesperado da sessão #{token} (processo #{inspect(pid)}): #{inspect(reason)}"
          )
        end

        sessions = Map.delete(state.sessions, token)
        {:noreply, %{state | sessions: sessions, refs: refs}}
    end
  end

  defp drop_session(state, token, ref) do
    %{
      state
      | sessions: Map.delete(state.sessions, token),
        refs: Map.delete(state.refs, ref)
    }
  end

  # Processo de sessão: expira por inatividade e reage a comandos.
  defp session_loop(ttl) do
    receive do
      :touch -> session_loop(ttl)
      :logout -> :ok
      {:fail, reason} -> exit(reason)
    after
      ttl -> :ok
    end
  end

  defp new_token do
    :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  end
end

# ---------------------------------------------------------------------------
# API pública: operações protegidas pelo limitador via macro
# ---------------------------------------------------------------------------
defmodule Sessions do
  @moduledoc """
  API pública do sistema de usuários e sessões.
  Todas as operações passam pelo limitador de concorrência.
  """

  import Sessions.Macros

  defoperation create_user(name) do
    Sessions.Manager.create_user(name)
  end

  defoperation login(user_id) do
    Sessions.Manager.login(user_id)
  end

  defoperation logout(token) do
    Sessions.Manager.logout(token)
  end

  defoperation touch(token) do
    Sessions.Manager.touch(token)
  end

  defoperation crash_session(token, reason) do
    Sessions.Manager.crash_session(token, reason)
  end

  defoperation list_sessions() do
    Sessions.Manager.list_sessions()
  end
end

# ---------------------------------------------------------------------------
# Supervisor da aplicação
# ---------------------------------------------------------------------------
defmodule Sessions.Supervisor do
  @moduledoc """
  Supervisor raiz. Opções: `:max_operations` (limite de operações simultâneas).

  Exemplo:

      {:ok, _} = Sessions.Supervisor.start_link(max_operations: 5)
  """

  use Supervisor
  require Logger

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_operations, 10)
    Logger.info("Sistema de sessões iniciado (máximo de #{max} operações simultâneas)")

    children = [
      {Sessions.Limiter, max: max},
      Sessions.Manager
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
