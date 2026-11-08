# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Validar os dados recebidos antes de realizar operações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Process
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageSystem do
  @max_id_length 64
  @max_message_length 4_000

  def start do
    spawn(fn -> loop(initial_state()) end)
  end

  def register(server, user_id, user_pid \\ self()) do
    request(server, {:register, user_id, user_pid})
  end

  def unregister(server, user_id) do
    request(server, {:unregister, user_id})
  end

  def send_message(server, from, to, body) do
    request(server, {:send_message, from, to, body})
  end

  def inbox(server, user_id) do
    request(server, {:inbox, user_id})
  end

  def stop(server) do
    request(server, :stop)
  end

  defp initial_state do
    %{
      users: %{},
      refs: %{},
      inboxes: %{},
      next_message_id: 1
    }
  end

  defp request(server, operation) when is_pid(server) do
    ref = Process.monitor(server)
    request_ref = make_ref()
    send(server, {:request, self(), request_ref, operation})

    receive do
      {:response, ^request_ref, response} ->
        Process.demonitor(ref, [:flush])
        response

      {:DOWN, ^ref, :process, ^server, _reason} ->
        {:error, :process_terminated}
    end
  end

  defp request(_server, _operation), do: {:error, :invalid_server}

  defp loop(state) do
    receive do
      {:request, caller, request_ref, operation} ->
        case handle_request(operation, state) do
          {:stop, reply} ->
            send(caller, {:response, request_ref, reply})

          {reply, new_state} ->
            send(caller, {:response, request_ref, reply})
            loop(new_state)
        end

      {:DOWN, ref, :process, _pid, _reason} ->
        loop(handle_down(ref, state))

      _unexpected_message ->
        loop(state)
    end
  end

  defp handle_request({:register, user_id, user_pid}, state) do
    with :ok <- validate_user_id(user_id),
         :ok <- validate_user_pid(user_pid),
         false <- Map.has_key?(state.users, user_id) do
      ref = Process.monitor(user_pid)
      user = %{pid: user_pid, ref: ref}

      new_state = %{
        state
        | users: Map.put(state.users, user_id, user),
          refs: Map.put(state.refs, ref, user_id),
          inboxes: Map.put_new(state.inboxes, user_id, [])
      }

      {{:ok, user_id}, new_state}
    else
      true -> {{:error, :already_registered}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp handle_request({:unregister, user_id}, state) do
    with :ok <- validate_user_id(user_id),
         {:ok, user} <- Map.fetch(state.users, user_id) do
      Process.demonitor(user.ref, [:flush])

      new_state = %{
        state
        | users: Map.delete(state.users, user_id),
          refs: Map.delete(state.refs, user.ref)
      }

      {{:ok, user_id}, new_state}
    else
      :error -> {{:error, :user_not_registered}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp handle_request({:send_message, from, to, body}, state) do
    with :ok <- validate_user_id(from),
         :ok <- validate_user_id(to),
         :ok <- validate_body(body),
         {:ok, sender} <- Map.fetch(state.users, from),
         {:ok, recipient} <- Map.fetch(state.users, to) do
      message = %{
        id: state.next_message_id,
        from: from,
        to: to,
        body: body,
        sent_at: System.system_time(:millisecond)
      }

      new_state = %{
        state
        | inboxes: Map.update!(state.inboxes, to, &(&1 ++ [message])),
          next_message_id: state.next_message_id + 1
      }

      send(recipient.pid, {:new_message, message})
      {{:ok, message}, new_state}
    else
      :error -> {{:error, :user_not_registered}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp handle_request({:inbox, user_id}, state) do
    with :ok <- validate_user_id(user_id),
         {:ok, messages} <- Map.fetch(state.inboxes, user_id) do
      {{:ok, messages}, state}
    else
      :error -> {{:error, :unknown_user}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  defp handle_request(:stop, _state), do: {:stop, :ok}

  defp handle_request(_operation, state), do: {{:error, :invalid_request}, state}

  defp handle_down(ref, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        state

      {user_id, refs} ->
        %{
          state
          | users: Map.delete(state.users, user_id),
            refs: refs
        }
    end
  end

  defp validate_user_id(user_id)
       when is_binary(user_id) and byte_size(user_id) > 0 and
              byte_size(user_id) <= @max_id_length do
    if String.valid?(user_id) and String.trim(user_id) != "" do
      :ok
    else
      {:error, :invalid_user_id}
    end
  end

  defp validate_user_id(_user_id), do: {:error, :invalid_user_id}

  defp validate_user_pid(pid) when is_pid(pid) do
    if Process.alive?(pid), do: :ok, else: {:error, :process_not_alive}
  end

  defp validate_user_pid(_pid), do: {:error, :invalid_process}

  defp validate_body(body)
       when is_binary(body) and byte_size(body) > 0 and
              byte_size(body) <= @max_message_length do
    if String.valid?(body) and String.trim(body) != "" do
      :ok
    else
      {:error, :invalid_message}
    end
  end

  defp validate_body(_body), do: {:error, :invalid_message}
end
