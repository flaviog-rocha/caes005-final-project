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

# Gerenciamento de usuários e sessões com limite de operações simultâneas,
# detecção de encerramentos inesperados e uso de macros.
# Dependências: apenas a biblioteca padrão do Elixir (GenServer, Supervisor, Logger).

require Logger

defmodule SessionMgr.Limiter do
  @moduledoc """
  Semáforo que limita o número de operações simultâneas.

  Cada chamador que adquire um slot é monitorado. Se o processo morrer
  enquanto segura o slot (ou estiver aguardando na fila), o slot é liberado
  automaticamente e o encerramento inesperado é registrado no Logger.
  """
  use GenServer
  require Logger

  # Permite iniciar com `name:` opcional e `max:` como limite de concorrência.
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    max = Keyword.get(opts, :max, 5)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, max, gen_opts)
  end

  @doc "Solicita um slot. Bloqueia até que haja disponibilidade."
  def acquire(server, timeout \\ 30_000) do
    GenServer.call(server, :acquire, timeout)
  end

  @doc "Libera o slot que o processo chamador detém."
  def release(server) do
    GenServer.call(server, :release)
  end

  @impl true
  def init(max) do
    Logger.info("Limiter iniciado com limite de #{max} operações simultâneas")
    {:ok, %{max: max, active: %{}, queue: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    # Monitora o chamador para detectar encerramento, mesmo se estiver na fila.
    ref = Process.monitor(pid)

    if map_size(state.active) < state.max do
      {:reply, :ok, %{state | active: Map.put(state.active, ref, pid)}}
    else
      Logger.debug("Limite atingido; #{inspect(pid)} aguardando slot")
      {:noreply, %{state | queue: :queue.in({from, pid, ref}, state.queue)}}
    end
  end

  def handle_call(:release, {pid, _tag}, state) do
    case Enum.find(state.active, fn {_ref, p} -> p == pid end) do
      {ref, _pid} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, state |> drop_active(ref) |> dispatch()}

      nil ->
        {:reply, {:error, :not_acquired}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    cond do
      Map.has_key?(state.active, ref) ->
        if reason != :normal do
          Logger.warning(
            "Processo #{inspect(pid)} encerrou inesperadamente segurando um slot " <>
              "(motivo: #{inspect(reason)}); slot liberado"
          )
        end

        {:noreply, state |> drop_active(ref) |> dispatch()}

      true ->
        # Chamador na fila morreu: remove da fila sem consumir slot.
        queue = :queue.filter(fn {_from, _pid, r} -> r != ref end, state.queue)
        {:noreply, %{state | queue: queue}}
    end
  end

  defp drop_active(state, ref) do
    %{state | active: Map.delete(state.active, ref)}
  end

  # Promove chamadores da fila enquanto houver slots livres.
  defp dispatch(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.queue) do
        {{:value, {from, pid, ref}}, rest} ->
          GenServer.reply(from, :ok)
          dispatch(%{state | active: Map.put(state.active, ref, pid), queue: rest})

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule SessionMgr.Macros do
  @moduledoc "Macros utilitárias: controle de slot e rastreamento de operações."

  @doc """
  Executa o bloco adquirindo um slot do limiter e garantindo a liberação
  ao final, mesmo em caso de exceção.
  """
  defmacro with_slot(limiter, do: block) do
    quote do
      :ok = SessionMgr.Limiter.acquire(unquote(limiter))

      try do
        unquote(block)
      after
        SessionMgr.Limiter.release(unquote(limiter))
      end
    end
  end

  @doc """
  Registra no Logger o tempo de execução do bloco, e loga erros
  antes de propagá-los.
  """
  defmacro traced(op_name, do: block) do
    quote do
      require Logger
      started_at = System.monotonic_time(:millisecond)

      try do
        result = unquote(block)
        elapsed = System.monotonic_time(:millisecond) - started_at
        Logger.debug("Operação #{unquote(op_name)} concluída em #{elapsed}ms")
        result
      rescue
        exception ->
          Logger.error(
            "Operação #{unquote(op_name)} falhou: " <> Exception.message(exception)
          )

          reraise exception, __STACKTRACE__
      end
    end
  end
end

defmodule SessionMgr.Store do
  @moduledoc """
  Mantém usuários e sessões. Cada sessão é um processo separado monitorado;
  encerramentos inesperados são detectados via `:DOWN` e registrados.
  """
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: name)
  end

  def register_user(name), do: GenServer.call(__MODULE__, {:register_user, name})
  def start_session(name), do: GenServer.call(__MODULE__, {:start_session, name})
  def end_session(id), do: GenServer.call(__MODULE__, {:end_session, id})
  def crash_session(id), do: GenServer.call(__MODULE__, {:crash_session, id})
  def sessions, do: GenServer.call(__MODULE__, :sessions)

  @impl true
  def init(:ok) do
    {:ok, %{users: MapSet.new(), sessions: %{}}}
  end

  @impl true
  def handle_call({:register_user, name}, _from, state) do
    if MapSet.member?(state.users, name) do
      {:reply, {:error, :already_exists}, state}
    else
      Logger.info("Usuário registrado: #{name}")
      {:reply, :ok, %{state | users: MapSet.put(state.users, name)}}
    end
  end

  def handle_call({:start_session, name}, _from, state) do
    if MapSet.member?(state.users, name) do
      id = "sess-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
      pid = spawn(fn -> session_loop(name) end)
      ref = Process.monitor(pid)

      Logger.info("Sessão #{id} iniciada para #{name} (#{inspect(pid)})")

      session = %{user: name, pid: pid, ref: ref}
      {:reply, {:ok, id}, %{state | sessions: Map.put(state.sessions, id, session)}}
    else
      {:reply, {:error, :unknown_user}, state}
    end
  end

  def handle_call({:end_session, id}, _from, state) do
    case Map.pop(state.sessions, id) do
      {nil, _} ->
        {:reply, {:error, :not_found}, state}

      {%{pid: pid, ref: ref, user: user}, rest} ->
        # Desmonitora antes de encerrar, pois o término é esperado.
        Process.demonitor(ref, [:flush])
        send(pid, :logout)
        Logger.info("Sessão #{id} de #{user} encerrada normalmente")
        {:reply, :ok, %{state | sessions: rest}}
    end
  end

  # Usado para simular falhas e testar a detecção de encerramento inesperado.
  def handle_call({:crash_session, id}, _from, state) do
    case Map.fetch(state.sessions, id) do
      {:ok, %{pid: pid}} ->
        send(pid, :crash)
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:sessions, _from, state) do
    list = Enum.map(state.sessions, fn {id, s} -> {id, s.user} end)
    {:reply, list, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Enum.find(state.sessions, fn {_id, s} -> s.ref == ref end) do
      {id, %{user: user}} ->
        if reason == :normal do
          Logger.info("Sessão #{id} de #{user} finalizou normalmente")
        else
          Logger.error(
            "Sessão #{id} de #{user} (#{inspect(pid)}) encerrou inesperadamente: " <>
              inspect(reason)
          )
        end

        {:noreply, %{state | sessions: Map.delete(state.sessions, id)}}

      nil ->
        {:noreply, state}
    end
  end

  # Processo que representa uma sessão ativa.
  defp session_loop(user) do
    receive do
      :logout -> :ok
      :crash -> exit({:simulated_failure, user})
      _other -> session_loop(user)
    end
  end
end

defmodule SessionMgr do
  @moduledoc """
  API pública: limita operações simultâneas via `SessionMgr.Limiter`,
  registra operações via macros e delega o estado a `SessionMgr.Store`.
  """
  require Logger
  require SessionMgr.Macros
  import SessionMgr.Macros

  alias SessionMgr.{Limiter, Store}

  @doc "Inicia o supervisor com o limite de operações simultâneas (`:max_concurrent`)."
  def start_link(opts \\ []) do
    max = Keyword.get(opts, :max_concurrent, 5)

    children = [
      {Limiter, name: Limiter, max: max},
      {Store, name: Store}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__.Supervisor)
  end

  @doc "Executa `fun` dentro do limite de concorrência, com rastreamento."
  def exec(op_name, fun) when is_function(fun, 0) do
    traced op_name do
      with_slot Limiter do
        fun.()
      end
    end
  end

  def register_user(name), do: exec("register_user", fn -> Store.register_user(name) end)

  def start_session(name), do: exec("start_session", fn -> Store.start_session(name) end)

  def end_session(id), do: exec("end_session", fn -> Store.end_session(id) end)

  def crash_session(id), do: exec("crash_session", fn -> Store.crash_session(id) end)

  def sessions, do: exec("sessions", fn -> Store.sessions() end)
end

# ---------------------------------------------------------------------------
# Demonstração de uso
# ---------------------------------------------------------------------------

{:ok, _sup} = SessionMgr.start_link(max_concurrent: 2)

:ok = SessionMgr.register_user("ana")
{:ok, s1} = SessionMgr.start_session("ana")
{:ok, s2} = SessionMgr.start_session("ana")
IO.inspect(SessionMgr.sessions(), label: "Sessões ativas")

# Falha simulada: a detecção via monitor deve registrar erro no Logger.
:ok = SessionMgr.crash_session(s1)
Process.sleep(100)

# Limite de concorrência: com max_concurrent: 2, as operações excedentes aguardam.
1..6
|> Enum.map(fn i ->
  Task.async(fn ->
    SessionMgr.exec("slow_op_#{i}", fn -> Process.sleep(200) end)
  end)
end)
|> Enum.each(&Task.await(&1, 10_000))

# Processo que morre segurando um slot: o slot deve ser liberado automaticamente.
spawn(fn ->
  SessionMgr.Limiter.acquire(SessionMgr.Limiter)
  exit(:boom)
end)

Process.sleep(100)

:ok = SessionMgr.end_session(s2)
IO.inspect(SessionMgr.sessions(), label: "Sessões após encerramento")
