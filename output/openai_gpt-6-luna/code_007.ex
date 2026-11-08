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

defmodule UserSessionManager do
  use GenServer
  require Logger

  defmacro with_operation(manager, do: body) do
    manager_var = Macro.unique_var(:manager, __CALLER__.module)
    permit_var = Macro.unique_var(:permit, __CALLER__.module)

    quote do
      unquote(manager_var) = unquote(manager)

      case UserSessionManager.acquire_operation(unquote(manager_var)) do
        {:ok, unquote(permit_var)} ->
          try do
            unquote(body)
          after
            UserSessionManager.release_operation(
              unquote(manager_var),
              unquote(permit_var)
            )
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def start_link(opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      opts,
      Keyword.take(opts, [:name])
    )
  end

  def create_user(manager, user_id, attributes \\ %{}) do
    with_operation manager do
      GenServer.call(manager, {:create_user, user_id, attributes}, :infinity)
    end
  end

  def get_user(manager, user_id) do
    with_operation manager do
      GenServer.call(manager, {:get_user, user_id}, :infinity)
    end
  end

  def delete_user(manager, user_id) do
    with_operation manager do
      GenServer.call(manager, {:delete_user, user_id}, :infinity)
    end
  end

  def start_session(manager, user_id) do
    with_operation manager do
      GenServer.call(manager, {:start_session, user_id}, :infinity)
    end
  end

  def stop_session(manager, session_id) do
    with_operation manager do
      GenServer.call(manager, {:stop_session, session_id}, :infinity)
    end
  end

  def crash_session(manager, session_id, reason \\ :simulated_failure) do
    with_operation manager do
      GenServer.call(manager, {:crash_session, session_id, reason}, :infinity)
    end
  end

  def acquire_operation(manager) do
    limiter = GenServer.call(manager, :limiter, :infinity)
    UserSessionManager.Limiter.acquire(limiter)
  end

  def release_operation(_manager, {limiter, token}) do
    UserSessionManager.Limiter.release(limiter, token)
  end

  @impl true
  def init(opts) do
    limit = Keyword.get(opts, :max_concurrent_operations, 4)

    if is_integer(limit) and limit > 0 do
      case UserSessionManager.Limiter.start_link(limit) do
        {:ok, limiter} ->
          {:ok,
           %{
             limiter: limiter,
             users: %{},
             sessions: %{},
             monitor_refs: %{}
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, {:invalid_max_concurrent_operations, limit}}
    end
  end

  @impl true
  def handle_call(:limiter, _from, state) do
    {:reply, state.limiter, state}
  end

  def handle_call({:create_user, user_id, attributes}, _from, state)
      when is_map(attributes) do
    if Map.has_key?(state.users, user_id) do
      {:reply, {:error, :already_exists}, state}
    else
      user = %{id: user_id, attributes: attributes}
      {:reply, {:ok, user}, put_in(state.users[user_id], user)}
    end
  end

  def handle_call({:create_user, _user_id, _attributes}, _from, state) do
    {:reply, {:error, :invalid_attributes}, state}
  end

  def handle_call({:get_user, user_id}, _from, state) do
    case Map.fetch(state.users, user_id) do
      {:ok, user} -> {:reply, {:ok, user}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_user, user_id}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      has_sessions =
        Enum.any?(state.sessions, fn {_id, session} ->
          session.user_id == user_id
        end)

      if has_sessions do
        {:reply, {:error, :active_sessions}, state}
      else
        {:reply, :ok, %{state | users: Map.delete(state.users, user_id)}}
      end
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:start_session, user_id}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      session_id = make_ref()
      {pid, monitor_ref} = spawn_monitor(fn -> session_loop() end)

      session = %{
        pid: pid,
        monitor_ref: monitor_ref,
        user_id: user_id,
        expected_stop: false
      }

      state = %{
        state
        | sessions: Map.put(state.sessions, session_id, session),
          monitor_refs: Map.put(state.monitor_refs, monitor_ref, session_id)
      }

      {:reply, {:ok, session_id}, state}
    else
      {:reply, {:error, :user_not_found}, state}
    end
  end

  def handle_call({:stop_session, session_id}, _from, state) do
    case Map.fetch(state.sessions, session_id) do
      {:ok, session} ->
        if Process.alive?(session.pid) do
          send(session.pid, :stop)
          session = %{session | expected_stop: true}
          {:reply, :ok, put_in(state.sessions[session_id], session)}
        else
          {:reply, :ok, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:crash_session, session_id, reason}, _from, state) do
    case Map.fetch(state.sessions, session_id) do
      {:ok, session} ->
        send(session.pid, {:crash, reason})
        {:reply, :ok, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitor_refs, monitor_ref) do
      {nil, _refs} ->
        {:noreply, state}

      {session_id, refs} ->
        case Map.pop(state.sessions, session_id) do
          {nil, sessions} ->
            {:noreply, %{state | sessions: sessions, monitor_refs: refs}}

          {session, sessions} ->
            unless session.expected_stop do
              Logger.warning(
                "Session #{inspect(session_id)} for user " <>
                  "#{inspect(session.user_id)} terminated unexpectedly: " <>
                  "#{inspect(reason)}"
              )
            end

            {:noreply, %{state | sessions: sessions, monitor_refs: refs}}
        end
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp session_loop do
    receive do
      :stop ->
        :ok

      {:crash, reason} ->
        exit(reason)

      _message ->
        session_loop()
    end
  end
end

defmodule UserSessionManager.Limiter do
  use GenServer

  def start_link(limit) do
    GenServer.start_link(__MODULE__, limit)
  end

  def acquire(limiter) do
    GenServer.call(limiter, :acquire, :infinity)
  end

  def release(limiter, token) do
    GenServer.call(limiter, {:release, token}, :infinity)
  end

  @impl true
  def init(limit) do
    {:ok,
     %{
       limit: limit,
       active: %{},
       pending: %{},
       refs: %{},
       queue: :queue.new()
     }}
  end

  @impl true
  def handle_call(:acquire, from, state) do
    pid = elem(from, 0)
    token = make_ref()
    monitor_ref = Process.monitor(pid)

    if map_size(state.active) < state.limit do
      state = %{
        state
        | active: Map.put(state.active, token, {pid, monitor_ref}),
          refs: Map.put(state.refs, monitor_ref, {:active, token})
      }

      {:reply, {:ok, {self(), token}}, state}
    else
      pending = Map.put(state.pending, token, {from, pid, monitor_ref})
      refs = Map.put(state.refs, monitor_ref, {:pending, token})
      queue = :queue.in(token, state.queue)

      {:noreply, %{state | pending: pending, refs: refs, queue: queue}}
    end
  end

  def handle_call({:release, token}, from, state) do
    caller_pid = elem(from, 0)

    case Map.fetch(state.active, token) do
      {:ok, {^caller_pid, monitor_ref}} ->
        Process.demonitor(monitor_ref, [:flush])

        state = %{
          state
          | active: Map.delete(state.active, token),
            refs: Map.delete(state.refs, monitor_ref)
        }

        {:reply, :ok, grant_waiters(state)}

      {:ok, _owner} ->
        {:reply, {:error, :not_owner}, state}

      :error ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.pop(state.refs, monitor_ref) do
      {nil, _refs} ->
        {:noreply, state}

      {{:active, token}, refs} ->
        state = %{
          state
          | active: Map.delete(state.active, token),
            refs: refs
        }

        {:noreply, grant_waiters(state)}

      {{:pending, token}, refs} ->
        state = %{
          state
          | pending: Map.delete(state.pending, token),
            refs: refs
        }

        {:noreply, grant_waiters(state)}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp grant_waiters(state) do
    if map_size(state.active) >= state.limit do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, token}, queue} ->
          state = %{state | queue: queue}

          case Map.pop(state.pending, token) do
            {nil, pending} ->
              grant_waiters(%{state | pending: pending})

            {{from, pid, monitor_ref}, pending} ->
              if Process.alive?(pid) do
                refs = Map.put(state.refs, monitor_ref, {:active, token})
                active = Map.put(state.active, token, {pid, monitor_ref})
                GenServer.reply(from, {:ok, {self(), token}})

                grant_waiters(%{
                  state
                  | active: active,
                    pending: pending,
                    refs: refs
                })
              else
                Process.demonitor(monitor_ref, [:flush])

                grant_waiters(%{
                  state
                  | pending: pending,
                    refs: Map.delete(state.refs, monitor_ref)
                })
              end
          end
      end
    end
  end
end
