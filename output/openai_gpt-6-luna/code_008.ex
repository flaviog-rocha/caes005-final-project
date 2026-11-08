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

defmodule UserSessionManager.Log do
  defmacro event(level, message) do
    quote do
      require Logger
      Logger.log(unquote(level), unquote(message))
    end
  end
end

defmodule UserSessionManager do
  use GenServer
  require UserSessionManager.Log

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    server_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  def create_user(attrs, server \\ __MODULE__)
      when is_map(attrs) do
    GenServer.call(server, {:create_user, attrs})
  end

  def get_user(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:get_user, user_id})
  end

  def list_users(server \\ __MODULE__) do
    GenServer.call(server, :list_users)
  end

  def start_session(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:start_session, user_id})
  end

  def end_session(session_id, server \\ __MODULE__) do
    GenServer.call(server, {:end_session, session_id})
  end

  def list_sessions(server \\ __MODULE__) do
    GenServer.call(server, :list_sessions)
  end

  def run_operation(fun, server \\ __MODULE__) when is_function(fun, 0) do
    GenServer.call(server, {:run_operation, fun}, :infinity)
  end

  def run_operation(_fun, _server), do: {:error, :invalid_operation}

  @impl true
  def init(opts) do
    max_concurrent = Keyword.get(opts, :max_concurrent, 4)

    if is_integer(max_concurrent) and max_concurrent > 0 do
      {:ok,
       %{
         max_concurrent: max_concurrent,
         users: %{},
         sessions: %{},
         operations: %{},
         operation_pids: %{},
         monitors: %{}
       }}
    else
      {:stop, {:invalid_max_concurrent, max_concurrent}}
    end
  end

  @impl true
  def handle_call({:create_user, attrs}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])
    user = %{id: id, attributes: attrs, created_at: System.system_time(:millisecond)}

    UserSessionManager.Log.event(:info, "Created user #{inspect(id)}")

    {:reply, {:ok, user}, put_in(state.users[id], user)}
  end

  def handle_call({:get_user, user_id}, _from, state) do
    reply =
      case Map.fetch(state.users, user_id) do
        {:ok, user} -> {:ok, user}
        :error -> {:error, :user_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_users, _from, state) do
    {:reply, Map.values(state.users), state}
  end

  def handle_call({:start_session, user_id}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      session_id = System.unique_integer([:positive, :monotonic])
      {pid, monitor_ref} = spawn_monitor(fn -> session_wait() end)

      session = %{
        id: session_id,
        user_id: user_id,
        started_at: System.system_time(:millisecond)
      }

      state =
        state
        |> put_in([:sessions, session_id], Map.merge(session, %{pid: pid, monitor_ref: monitor_ref}))
        |> put_in([:monitors, monitor_ref], {:session, session_id})

      UserSessionManager.Log.event(
        :info,
        "Started session #{inspect(session_id)} for user #{inspect(user_id)}"
      )

      {:reply, {:ok, session_id}, state}
    else
      {:reply, {:error, :user_not_found}, state}
    end
  end

  def handle_call({:end_session, session_id}, _from, state) do
    case Map.pop(state.sessions, session_id) do
      {nil, _sessions} ->
        {:reply, {:error, :session_not_found}, state}

      {session, sessions} ->
        Process.demonitor(session.monitor_ref, [:flush])
        Process.exit(session.pid, :shutdown)

        state = %{
          state
          | sessions: sessions,
            monitors: Map.delete(state.monitors, session.monitor_ref)
        }

        UserSessionManager.Log.event(:info, "Ended session #{inspect(session_id)}")
        {:reply, :ok, state}
    end
  end

  def handle_call(:list_sessions, _from, state) do
    sessions =
      state.sessions
      |> Map.values()
      |> Enum.map(&Map.drop(&1, [:pid, :monitor_ref]))

    {:reply, sessions, state}
  end

  def handle_call({:run_operation, _fun}, _from, state)
      when map_size(state.operations) >= state.max_concurrent do
    {:reply, {:error, :too_many_operations}, state}
  end

  def handle_call({:run_operation, fun}, from, state) do
    manager = self()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result =
          try do
            {:ok, fun.()}
          rescue
            error -> {:error, {:exception, error, __STACKTRACE__}}
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(manager, {:operation_result, self(), result})
      end)

    operation = %{from: from, pid: pid}

    state =
      state
      |> put_in([:operations, monitor_ref], operation)
      |> put_in([:operation_pids, pid], monitor_ref)
      |> put_in([:monitors, monitor_ref], :operation)

    UserSessionManager.Log.event(:debug, "Started operation in process #{inspect(pid)}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:operation_result, pid, result}, state) do
    case Map.pop(state.operation_pids, pid) do
      {nil, _operation_pids} ->
        {:noreply, state}

      {monitor_ref, operation_pids} ->
        case Map.pop(state.operations, monitor_ref) do
          {nil, _operations} ->
            {:noreply, %{state | operation_pids: operation_pids}}

          {operation, operations} ->
            Process.demonitor(monitor_ref, [:flush])
            GenServer.reply(operation.from, result)

            state = %{
              state
              | operations: operations,
                operation_pids: operation_pids,
                monitors: Map.delete(state.monitors, monitor_ref)
            }

            {:noreply, state}
        end
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {{:session, session_id}, monitors} ->
        sessions = Map.delete(state.sessions, session_id)

        Logger.warning(
          "Session process #{inspect(pid)} for session #{inspect(session_id)} terminated unexpectedly: #{inspect(reason)}"
        )

        {:noreply, %{state | sessions: sessions, monitors: monitors}}

      {:operation, monitors} ->
        case Map.pop(state.operations, monitor_ref) do
          {nil, operations} ->
            {:noreply, %{state | operations: operations, monitors: monitors}}

          {operation, operations} ->
            Logger.warning(
              "Operation process #{inspect(pid)} terminated before returning a result: #{inspect(reason)}"
            )

            GenServer.reply(operation.from, {:error, {:operation_terminated, reason}})

            state = %{
              state
              | operations: operations,
                operation_pids: Map.delete(state.operation_pids, pid),
                monitors: monitors
            }

            {:noreply, state}
        end

      {nil, _monitors} ->
        {:noreply, state}
    end
  end

  defp session_wait do
    receive do
      :stop -> :ok
      _message -> session_wait()
    end
  end
end
