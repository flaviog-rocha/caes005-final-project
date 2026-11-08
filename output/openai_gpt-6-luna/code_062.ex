# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir a execução de operações simultâneas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Manter um histórico das operações realizadas.
# 5. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageSystem do
  @moduledoc """
  Sistema de troca de mensagens entre usuários executados em processos separados.
  """

  def start_link(opts \\ []) do
    Supervisor.start_link(
      [
        {Registry, keys: :unique, name: MessageSystem.Registry},
        {DynamicSupervisor, strategy: :one_for_one, name: MessageSystem.UserSupervisor},
        {MessageSystem.Hub, opts}
      ],
      strategy: :one_for_one,
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def register_user(user), do: MessageSystem.Hub.register_user(user)
  def send_message(from, to, body), do: MessageSystem.Hub.send_message(from, to, body)
  def inbox(user), do: MessageSystem.Hub.inbox(user)
  def history, do: MessageSystem.Hub.history()

  def user_pid(user) do
    case Registry.lookup(MessageSystem.Registry, user) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end
end

defmodule MessageSystem.User do
  use GenServer

  def start_link(user) do
    GenServer.start_link(__MODULE__, user,
      name: {:via, Registry, {MessageSystem.Registry, user}}
    )
  end

  def child_spec(user) do
    %{
      id: {__MODULE__, user},
      start: {__MODULE__, :start_link, [user]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init(user), do: {:ok, %{user: user, inbox: []}}

  @impl true
  def handle_cast({:send, recipient_pid, message}, state) do
    GenServer.cast(recipient_pid, {:deliver, message})
    {:noreply, state}
  end

  def handle_cast({:deliver, message}, state) do
    {:noreply, %{state | inbox: [message | state.inbox]}}
  end
end

defmodule MessageSystem.Hub do
  use GenServer

  @history_key {__MODULE__, :history}
  @default_interval 60_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def register_user(user), do: GenServer.call(__MODULE__, {:register_user, user})
  def send_message(from, to, body), do: GenServer.call(__MODULE__, {:send_message, from, to, body})
  def inbox(user), do: GenServer.call(__MODULE__, {:inbox, user})
  def history, do: GenServer.call(__MODULE__, :history)

  @impl true
  def init(opts) do
    history_rev = :persistent_term.get(@history_key, [])
    interval = Keyword.get(opts, :interval, @default_interval)

    send(self(), :restore_users)
    Process.send_after(self(), :maintenance, interval)

    {:ok, %{history_rev: history_rev, seq: length(history_rev), interval: interval}}
  end

  @impl true
  def handle_call({:register_user, user}, _from, state) do
    already_registered? = user_pid(user) != nil

    case ensure_user(user) do
      {:ok, _pid} ->
        state =
          if already_registered? do
            state
          else
            record(state, %{operation: :user_registered, user: user})
          end

        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:send_message, from, to, body}, _caller, state) do
    with {:ok, sender_pid} <- ensure_user(from),
         {:ok, recipient_pid} <- ensure_user(to) do
      message = %{from: from, to: to, body: body, sent_at: now()}
      GenServer.cast(sender_pid, {:send, recipient_pid, message})

      state =
        record(state, %{
          operation: :message_sent,
          from: from,
          to: to,
          body: body,
          sent_at: message.sent_at
        })

      {:reply, :ok, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:inbox, user}, _caller, state) do
    messages =
      state.history_rev
      |> Enum.reverse()
      |> Enum.filter(&(&1.operation == :message_sent and &1.to == user))
      |> Enum.map(fn event ->
        %{from: event.from, to: event.to, body: event.body, sent_at: event.sent_at}
      end)

    {:reply, messages, state}
  end

  def handle_call(:history, _caller, state) do
    {:reply, Enum.reverse(state.history_rev), state}
  end

  @impl true
  def handle_info(:restore_users, state) do
    state.history_rev
    |> known_users()
    |> Enum.each(&ensure_user/1)

    {:noreply, state}
  end

  def handle_info(:maintenance, state) do
    users = known_users(state.history_rev)
    Enum.each(users, &ensure_user/1)

    state = record(state, %{operation: :maintenance, users_checked: users})
    Process.send_after(self(), :maintenance, state.interval)

    {:noreply, state}
  end

  defp ensure_user(user) do
    case user_pid(user) do
      nil ->
        case DynamicSupervisor.start_child(
               MessageSystem.UserSupervisor,
               {MessageSystem.User, user}
             ) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end

      pid ->
        {:ok, pid}
    end
  end

  defp user_pid(user) do
    case Registry.lookup(MessageSystem.Registry, user) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp known_users(history_rev) do
    history_rev
    |> Enum.flat_map(fn
      %{operation: :user_registered, user: user} -> [user]
      %{operation: :message_sent, from: from, to: to} -> [from, to]
      _ -> []
    end)
    |> Enum.uniq()
  end

  defp record(state, event) do
    event =
      event
      |> Map.put(:seq, state.seq + 1)
      |> Map.put(:at, now())

    history_rev = [event | state.history_rev]
    :persistent_term.put(@history_key, history_rev)

    %{state | history_rev: history_rev, seq: state.seq + 1}
  end

  defp now, do: System.system_time(:millisecond)
end
