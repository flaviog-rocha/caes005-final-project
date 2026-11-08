# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem.Validation do
  defmacro valid(condition, reason) do
    quote do
      if unquote(condition),
        do: :ok,
        else: {:error, {:validation, unquote(reason)}}
    end
  end
end

defmodule MessagingSystem.User do
  use GenServer

  def start do
    GenServer.start(__MODULE__, nil)
  end

  def init(_initial_state), do: {:ok, %{messages: [], count: 0}}

  def handle_call({:deliver, message, threshold}, _from, state) do
    new_count = state.count + 1
    crossed_threshold = state.count < threshold and new_count >= threshold

    new_state = %{
      messages: [message | state.messages],
      count: new_count
    }

    {:reply, {new_count, crossed_threshold}, new_state}
  end

  def handle_call(:inbox, _from, state) do
    {:reply, Enum.reverse(state.messages), state}
  end
end

defmodule MessagingSystem do
  use GenServer
  require MessagingSystem.Validation

  alias MessagingSystem.User

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def register_user(server, username) do
    with :ok <- MessagingSystem.Validation.valid(is_pid(server), :invalid_server),
         :ok <-
           MessagingSystem.Validation.valid(
             is_binary(username) and String.trim(username) != "",
             :invalid_username
           ) do
      safe_call(server, {:register_user, username})
    end
  end

  def send_message(server, from, to, body) do
    with :ok <- MessagingSystem.Validation.valid(is_pid(server), :invalid_server),
         :ok <-
           MessagingSystem.Validation.valid(
             is_binary(from) and String.trim(from) != "",
             :invalid_sender
           ),
         :ok <-
           MessagingSystem.Validation.valid(
             is_binary(to) and String.trim(to) != "",
             :invalid_recipient
           ),
         :ok <-
           MessagingSystem.Validation.valid(
             is_binary(body) and String.trim(body) != "",
             :invalid_message
           ) do
      safe_call(server, {:send_message, from, to, body})
    end
  end

  def inbox(server, username) do
    with :ok <- MessagingSystem.Validation.valid(is_pid(server), :invalid_server),
         :ok <-
           MessagingSystem.Validation.valid(
             is_binary(username) and String.trim(username) != "",
             :invalid_username
           ) do
      safe_call(server, {:inbox, username})
    end
  end

  def subscribe(server, subscriber \\ self()) do
    with :ok <- MessagingSystem.Validation.valid(is_pid(server), :invalid_server),
         :ok <- MessagingSystem.Validation.valid(is_pid(subscriber), :invalid_subscriber) do
      safe_call(server, {:subscribe, subscriber})
    end
  end

  def init(opts) do
    threshold = Keyword.get(opts, :notification_threshold, 10)
    threshold = if is_integer(threshold) and threshold > 0, do: threshold, else: 10

    {:ok,
     %{
       users: %{},
       monitors: %{},
       subscribers: MapSet.new(),
       notification_threshold: threshold
     }}
  end

  def handle_call({:register_user, username}, _from, state) do
    if Map.has_key?(state.users, username) do
      {:reply, {:error, {:validation, :username_taken}}, state}
    else
      case User.start() do
        {:ok, pid} ->
          ref = Process.monitor(pid)

          new_state = %{
            state
            | users: Map.put(state.users, username, pid),
              monitors: Map.put(state.monitors, ref, {username, pid})
          }

          {:reply, {:ok, pid}, new_state}

        {:error, reason} ->
          {:reply, {:error, {:operational, reason}}, state}
      end
    end
  end

  def handle_call({:send_message, from, to, body}, _from_call, state) do
    with {:ok, _sender_pid} <- find_user(state, from),
         {:ok, recipient_pid} <- find_user(state, to) do
      message = %{
        from: from,
        to: to,
        body: body,
        sent_at: System.system_time(:millisecond)
      }

      case deliver(recipient_pid, message, state.notification_threshold) do
        {:ok, {_count, crossed_threshold}} ->
          notify(state, {:message_delivered, message})

          if crossed_threshold do
            notify(state, {:inbox_threshold_reached, to, state.notification_threshold})
          end

          {:reply, {:ok, message}, state}

        {:error, reason} ->
          {:reply, {:error, {:operational, {:recipient_unavailable, reason}}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call({:inbox, username}, _from, state) do
    case find_user(state, username) do
      {:ok, pid} ->
        case user_call(pid, :inbox) do
          {:ok, messages} -> {:reply, {:ok, messages}, state}
          {:error, reason} -> {:reply, {:error, {:operational, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, {:validation, reason}}, state}
    end
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    subscribers = MapSet.put(state.subscribers, subscriber)
    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {{username, pid}, monitors} ->
        users =
          case Map.get(state.users, username) do
            ^pid -> Map.delete(state.users, username)
            _other -> state.users
          end

        new_state = %{state | users: users, monitors: monitors}

        if unexpected_exit?(reason) do
          notify(new_state, {:user_process_down, username, reason})
        end

        {:noreply, new_state}
    end
  end

  def terminate(_reason, state) do
    Enum.each(state.users, fn {_username, pid} ->
      if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    end)

    :ok
  end

  defp find_user(state, username) do
    case Map.fetch(state.users, username) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :user_not_found}
    end
  end

  defp deliver(pid, message, threshold) do
    case user_call(pid, {:deliver, message, threshold}) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp user_call(pid, request) do
    try do
      {:ok, GenServer.call(pid, request)}
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp notify(state, event) do
    Enum.each(state.subscribers, fn subscriber ->
      send(subscriber, {:messaging_notification, event})
    end)
  end

  defp unexpected_exit?(reason) when reason in [:normal, :shutdown], do: false
  defp unexpected_exit?({:shutdown, _reason}), do: false
  defp unexpected_exit?(_reason), do: true
end
