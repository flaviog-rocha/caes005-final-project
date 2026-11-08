# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  use GenServer

  defstruct [:id, :recipient, :title, :body, :inserted_at, :read_at]

  @type notification :: %__MODULE__{
          id: pos_integer(),
          recipient: term(),
          title: String.t(),
          body: String.t(),
          inserted_at: DateTime.t(),
          read_at: DateTime.t() | nil
        }

  # Todas as operações sobre notificações são serializadas pelo GenServer.
  # Eventos enviados aos assinantes têm o formato {:notification, evento, notificacao}.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def create(attrs), do: create(__MODULE__, attrs)

  def create(server, attrs) do
    GenServer.call(server, {:create, attrs})
  end

  def get(id), do: get(__MODULE__, id)

  def get(server, id) do
    GenServer.call(server, {:get, id})
  end

  def list(), do: list(__MODULE__, :all)

  def list(recipient), do: list(__MODULE__, recipient)

  def list(server, recipient) do
    GenServer.call(server, {:list, recipient})
  end

  def mark_read(id), do: mark_read(__MODULE__, id)

  def mark_read(server, id) do
    GenServer.call(server, {:mark_read, id})
  end

  def delete(id), do: delete(__MODULE__, id)

  def delete(server, id) do
    GenServer.call(server, {:delete, id})
  end

  def subscribe(), do: subscribe(__MODULE__, :all)

  def subscribe(recipient), do: subscribe(__MODULE__, recipient)

  def subscribe(server, recipient) do
    GenServer.call(server, {:subscribe, recipient})
  end

  def unsubscribe(), do: unsubscribe(__MODULE__)

  def unsubscribe(server) do
    GenServer.call(server, :unsubscribe)
  end

  @impl true
  def init(:ok) do
    {:ok, %{next_id: 1, notifications: %{}, subscribers: %{}, refs: %{}}}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    case validate_attrs(attrs) do
      {:ok, fields} ->
        notification = %__MODULE__{
          id: state.next_id,
          recipient: fields.recipient,
          title: fields.title,
          body: fields.body,
          inserted_at: DateTime.utc_now(),
          read_at: nil
        }

        state =
          state
          |> put_in([:notifications, notification.id], notification)
          |> Map.put(:next_id, state.next_id + 1)
          |> broadcast(:created, notification)

        {:reply, {:ok, notification}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.notifications, id) do
      {:ok, notification} -> {:reply, {:ok, notification}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:list, recipient}, _from, state) do
    notifications =
      state.notifications
      |> Map.values()
      |> Enum.filter(fn notification ->
        recipient == :all or notification.recipient == recipient
      end)
      |> Enum.sort_by(& &1.id)

    {:reply, notifications, state}
  end

  def handle_call({:mark_read, id}, _from, state) do
    case Map.fetch(state.notifications, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{read_at: nil} = notification} ->
        updated = %{notification | read_at: DateTime.utc_now()}
        state = put_in(state, [:notifications, id], updated)
        state = broadcast(state, :read, updated)
        {:reply, {:ok, updated}, state}

      {:ok, notification} ->
        {:reply, {:ok, notification}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.pop(state.notifications, id) do
      {nil, _notifications} ->
        {:reply, {:error, :not_found}, state}

      {notification, notifications} ->
        state = %{state | notifications: notifications}
        state = broadcast(state, :deleted, notification)
        {:reply, {:ok, notification}, state}
    end
  end

  def handle_call({:subscribe, recipient}, {pid, _tag}, state) do
    case Map.fetch(state.subscribers, pid) do
      {:ok, subscription} ->
        subscribers = Map.put(state.subscribers, pid, %{subscription | recipient: recipient})
        {:reply, :ok, %{state | subscribers: subscribers}}

      :error ->
        ref = Process.monitor(pid)
        subscription = %{ref: ref, recipient: recipient}

        state = %{
          state
          | subscribers: Map.put(state.subscribers, pid, subscription),
            refs: Map.put(state.refs, ref, pid)
        }

        {:reply, :ok, state}
    end
  end

  def handle_call(:unsubscribe, {pid, _tag}, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {%{ref: ref}, subscribers} ->
        Process.demonitor(ref, [:flush])

        state = %{
          state
          | subscribers: subscribers,
            refs: Map.delete(state.refs, ref)
        }

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.refs, ref) do
      ^pid ->
        {:noreply,
         %{
           state
           | subscribers: Map.delete(state.subscribers, pid),
             refs: Map.delete(state.refs, ref)
         }}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp validate_attrs(attrs) when is_map(attrs) do
    recipient = Map.get(attrs, :recipient)
    title = Map.get(attrs, :title)
    body = Map.get(attrs, :body)

    if not is_nil(recipient) and valid_text?(title) and valid_text?(body) do
      {:ok, %{recipient: recipient, title: title, body: body}}
    else
      {:error, :invalid_notification}
    end
  end

  defp validate_attrs(_), do: {:error, :invalid_notification}

  defp valid_text?(value), do: is_binary(value) and byte_size(value) > 0

  defp broadcast(state, event, notification) do
    Enum.each(state.subscribers, fn {pid, %{recipient: recipient}} ->
      if recipient == :all or recipient == notification.recipient do
        send(pid, {:notification, event, notification})
      end
    end)

    state
  end
end
