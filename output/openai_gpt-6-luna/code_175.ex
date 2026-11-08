# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Detectar encerramentos inesperados de processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Messaging.Macros do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import Messaging.Macros, only: [notification_rule: 2]
    end
  end

  defmacro notification_rule(event, do: condition) do
    quote do
      def notify?(unquote(event), payload), do: unquote(condition)
    end
  end
end

defmodule Messaging.NotificationRules do
  @moduledoc false
  use Messaging.Macros

  notification_rule :message_delivered do
    is_map(var!(payload)) and Map.has_key?(var!(payload), :to)
  end

  notification_rule :offline_queue_threshold do
    is_map(var!(payload)) and
      Map.get(var!(payload), :queued, 0) >= Map.get(var!(payload), :threshold, 1)
  end

  notification_rule :user_process_down do
    is_map(var!(payload)) and Map.get(var!(payload), :unexpected, false)
  end

  def notify?(_event, _payload), do: false
end

defmodule Messaging do
  @moduledoc """
  Sistema básico de troca de mensagens entre usuários.

  Erros de entrada ou de regras de negócio são retornados como
  `{:error, {:validation, reason}}`. Falhas de comunicação com o servidor
  são retornadas como `{:error, {:operational, reason}}`.
  """

  use GenServer

  @default_queue_threshold 3
  @max_message_length 10_000

  def start_link(opts \\ []) do
    threshold = Keyword.get(opts, :queue_threshold, @default_queue_threshold)

    if is_integer(threshold) and threshold > 0 do
      GenServer.start_link(__MODULE__, threshold, Keyword.take(opts, [:name]))
    else
      {:error, {:validation, :invalid_queue_threshold}}
    end
  end

  def register_user(server, user_id, pid \\ self()) do
    cond do
      not valid_user_id?(user_id) ->
        validation_error(:invalid_user_id)

      not is_pid(pid) ->
        validation_error(:invalid_process)

      true ->
        safe_call(server, {:register_user, user_id, pid})
    end
  end

  def subscribe(server, pid \\ self()) do
    if is_pid(pid) do
      safe_call(server, {:subscribe, pid})
    else
      validation_error(:invalid_process)
    end
  end

  def send_message(server, from, to, body) do
    cond do
      not valid_user_id?(from) ->
        validation_error(:invalid_sender)

      not valid_user_id?(to) ->
        validation_error(:invalid_recipient)

      not is_binary(body) or byte_size(body) == 0 ->
        validation_error(:invalid_message)

      byte_size(body) > @max_message_length ->
        validation_error(:message_too_long)

      true ->
        safe_call(server, {:send_message, from, to, body})
    end
  end

  @impl true
  def init(queue_threshold) do
    {:ok,
     %{
       users: %{},
       subscribers: %{},
       monitors: %{},
       queues: %{},
       queue_threshold: queue_threshold
     }}
  end

  @impl true
  def handle_call({:register_user, user_id, pid}, _from, state) do
    if Map.has_key?(state.users, user_id) do
      {:reply, validation_error(:user_already_registered), state}
    else
      monitor = Process.monitor(pid)
      user = %{pid: pid, monitor: monitor}

      state = %{
        state
        | users: Map.put(state.users, user_id, user),
          monitors: Map.put(state.monitors, monitor, {:user, user_id})
      }

      queued_messages = Map.get(state.queues, user_id, [])
      state = %{state | queues: Map.delete(state.queues, user_id)}

      state =
        Enum.reduce(queued_messages, state, fn message, acc ->
          deliver(pid, message)
          notify(acc, :message_delivered, delivery_payload(message))
        end)

      {:reply, {:ok, :registered}, state}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, validation_error(:already_subscribed), state}
    else
      monitor = Process.monitor(pid)

      state = %{
        state
        | subscribers: Map.put(state.subscribers, pid, monitor),
          monitors: Map.put(state.monitors, monitor, {:subscriber, pid})
      }

      {:reply, {:ok, :subscribed}, state}
    end
  end

  def handle_call({:send_message, from, to, body}, _caller, state) do
    cond do
      not Map.has_key?(state.users, from) ->
        {:reply, validation_error(:unknown_sender), state}

      Map.has_key?(state.users, to) ->
        message = new_message(from, to, body)
        %{pid: recipient_pid} = Map.fetch!(state.users, to)
        deliver(recipient_pid, message)

        state =
          notify(state, :message_delivered, delivery_payload(message))

        {:reply, {:ok, :delivered}, state}

      true ->
        message = new_message(from, to, body)
        queue = Map.get(state.queues, to, [])
        updated_queue = queue ++ [message]
        state = %{state | queues: Map.put(state.queues, to, updated_queue)}

        state =
          if length(queue) < state.queue_threshold and
               length(updated_queue) >= state.queue_threshold do
            notify(state, :offline_queue_threshold, %{
              to: to,
              queued: length(updated_queue),
              threshold: state.queue_threshold
            })
          else
            state
          end

        {:reply, {:ok, :queued}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.get(state.monitors, monitor) do
      {:user, user_id} ->
        state = %{
          state
          | users: Map.delete(state.users, user_id),
            monitors: Map.delete(state.monitors, monitor)
        }

        payload = %{
          user: user_id,
          reason: reason,
          unexpected: unexpected_exit?(reason)
        }

        {:noreply, notify(state, :user_process_down, payload)}

      {:subscriber, pid} ->
        state = %{
          state
          | subscribers: Map.delete(state.subscribers, pid),
            monitors: Map.delete(state.monitors, monitor)
        }

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp deliver(pid, %{from: from, body: body, timestamp: timestamp}) do
    send(pid, {:message, from, body, timestamp})
  end

  defp new_message(from, to, body) do
    %{
      from: from,
      to: to,
      body: body,
      timestamp: System.system_time(:millisecond)
    }
  end

  defp delivery_payload(message) do
    %{from: message.from, to: message.to, timestamp: message.timestamp}
  end

  defp notify(state, event, payload) do
    if Messaging.NotificationRules.notify?(event, payload) do
      Enum.each(state.subscribers, fn {pid, _monitor} ->
        send(pid, {:notification, event, payload})
      end)
    end

    state
  end

  defp unexpected_exit?(:normal), do: false
  defp unexpected_exit?(:shutdown), do: false
  defp unexpected_exit?({:shutdown, _reason}), do: false
  defp unexpected_exit?(_reason), do: true

  defp valid_user_id?(user_id) do
    is_binary(user_id) and String.trim(user_id) != ""
  end

  defp validation_error(reason), do: {:error, {:validation, reason}}

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end
end
