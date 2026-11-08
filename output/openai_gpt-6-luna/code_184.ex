# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  use GenServer

  defmodule Notification do
    @enforce_keys [:id, :user_id, :content, :created_at]
    defstruct [:id, :user_id, :content, :created_at, :read_at]
  end

  defstruct next_id: 1,
            notifications: %{},
            subscribers: %{},
            monitors: %{}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  def create(user_id, content, server \\ __MODULE__) do
    GenServer.call(server, {:create, user_id, content})
  end

  def list(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:list, user_id})
  end

  def mark_read(user_id, notification_id, server \\ __MODULE__) do
    GenServer.call(server, {:mark_read, user_id, notification_id})
  end

  def subscribe(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:subscribe, user_id, self()})
  end

  def unsubscribe(user_id, server \\ __MODULE__) do
    GenServer.call(server, {:unsubscribe, user_id, self()})
  end

  @impl true
  def init(:ok), do: {:ok, %__MODULE__{}}

  @impl true
  def handle_call({:create, user_id, content}, _from, state) do
    notification = %Notification{
      id: state.next_id,
      user_id: user_id,
      content: content,
      created_at: System.system_time(:millisecond)
    }

    notifications = Map.put(state.notifications, notification.id, notification)

    state = %{
      state
      | next_id: state.next_id + 1,
        notifications: notifications
    }

    state.subscribers
    |> Map.get(user_id, %{})
    |> Map.keys()
    |> Enum.each(&send(&1, {:notification, notification}))

    {:reply, {:ok, notification}, state}
  end

  def handle_call({:list, user_id}, _from, state) do
    notifications =
      state.notifications
      |> Map.values()
      |> Enum.filter(&(&1.user_id == user_id))
      |> Enum.sort_by(& &1.id)

    {:reply, notifications, state}
  end

  def handle_call({:mark_read, user_id, notification_id}, _from, state) do
    case Map.get(state.notifications, notification_id) do
      %Notification{user_id: ^user_id} = notification ->
        updated =
          if notification.read_at do
            notification
          else
            %{notification | read_at: System.system_time(:millisecond)}
          end

        state = put_in(state.notifications[notification_id], updated)
        {:reply, {:ok, updated}, state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:subscribe, user_id, pid}, _from, state) do
    user_subscribers = Map.get(state.subscribers, user_id, %{})

    if Map.has_key?(user_subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)

      subscribers = Map.put(state.subscribers, user_id, Map.put(user_subscribers, pid, ref))
      monitors = Map.put(state.monitors, ref, {user_id, pid})

      {:reply, :ok, %{state | subscribers: subscribers, monitors: monitors}}
    end
  end

  def handle_call({:unsubscribe, user_id, pid}, _from, state) do
    case get_in(state.subscribers, [user_id, pid]) do
      nil ->
        {:reply, :ok, state}

      ref ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, remove_subscriber(state, user_id, pid, ref)}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.monitors, ref) do
      {user_id, ^pid} ->
        {:noreply, remove_subscriber(state, user_id, pid, ref)}

      _ ->
        {:noreply, state}
    end
  end

  defp remove_subscriber(state, user_id, pid, ref) do
    user_subscribers =
      state.subscribers
      |> Map.get(user_id, %{})
      |> Map.delete(pid)

    subscribers =
      if map_size(user_subscribers) == 0 do
        Map.delete(state.subscribers, user_id)
      else
        Map.put(state.subscribers, user_id, user_subscribers)
      end

    %{state | subscribers: subscribers, monitors: Map.delete(state.monitors, ref)}
  end
end
