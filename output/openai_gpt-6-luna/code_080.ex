# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Fazer uso de macros.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageExchange do
  use Agent

  # Guardas definidas com macros para validar IDs e strings não vazias.
  defguardp valid_id(id) when is_integer(id) and id > 0
  defguardp nonempty_string(value) when is_binary(value) and byte_size(value) > 0

  def start_link(opts \\ []) do
    agent_opts =
      if Keyword.has_key?(opts, :name) do
        [name: Keyword.fetch!(opts, :name)]
      else
        []
      end

    Agent.start_link(
      fn ->
        %{
          users: %{},
          messages: %{},
          next_user_id: 1,
          next_message_id: 1
        }
      end,
      agent_opts
    )
  end

  def create_user(server, attrs) do
    with {:ok, name} <- validate_user_attrs(attrs) do
      Agent.get_and_update(server, fn state ->
        id = state.next_user_id
        user = %{id: id, name: name}

        {{:ok, user},
         %{
           state
           | users: Map.put(state.users, id, user),
             next_user_id: id + 1
         }}
      end)
    end
  end

  def get_user(server, id) when valid_id(id) do
    Agent.get(server, fn state ->
      case Map.fetch(state.users, id) do
        {:ok, user} -> {:ok, user}
        :error -> {:error, :not_found}
      end
    end)
  end

  def get_user(_server, _id), do: {:error, :invalid_id}

  def list_users(server) do
    Agent.get(server, fn state ->
      state.users
      |> Map.values()
      |> Enum.sort_by(& &1.id)
    end)
  end

  def update_user(server, id, attrs) when valid_id(id) do
    with {:ok, name} <- validate_user_attrs(attrs) do
      Agent.get_and_update(server, fn state ->
        case Map.fetch(state.users, id) do
          {:ok, user} ->
            updated_user = %{user | name: name}
            {{:ok, updated_user}, put_in(state.users[id], updated_user)}

          :error ->
            {{:error, :not_found}, state}
        end
      end)
    end
  end

  def update_user(_server, _id, _attrs), do: {:error, :invalid_id}

  def delete_user(server, id) when valid_id(id) do
    Agent.get_and_update(server, fn state ->
      case Map.pop(state.users, id) do
        {nil, _users} ->
          {{:error, :not_found}, state}

        {user, users} ->
          messages =
            state.messages
            |> Enum.reject(fn {_message_id, message} ->
              message.sender_id == id or message.recipient_id == id
            end)
            |> Map.new()

          {{:ok, user}, %{state | users: users, messages: messages}}
      end
    end)
  end

  def delete_user(_server, _id), do: {:error, :invalid_id}

  def create_message(server, attrs) do
    with {:ok, message_attrs} <- validate_message_attrs(attrs) do
      Agent.get_and_update(server, fn state ->
        if users_exist?(state, message_attrs.sender_id, message_attrs.recipient_id) do
          id = state.next_message_id
          message = Map.put(message_attrs, :id, id)

          {{:ok, message},
           %{
             state
             | messages: Map.put(state.messages, id, message),
               next_message_id: id + 1
           }}
        else
          {{:error, :user_not_found}, state}
        end
      end)
    end
  end

  def get_message(server, id) when valid_id(id) do
    Agent.get(server, fn state ->
      case Map.fetch(state.messages, id) do
        {:ok, message} -> {:ok, message}
        :error -> {:error, :not_found}
      end
    end)
  end

  def get_message(_server, _id), do: {:error, :invalid_id}

  def list_messages(server) do
    Agent.get(server, fn state ->
      state.messages
      |> Map.values()
      |> Enum.sort_by(& &1.id)
    end)
  end

  def update_message(server, id, attrs) when valid_id(id) do
    with :ok <- validate_message_update(attrs) do
      Agent.get_and_update(server, fn state ->
        case Map.fetch(state.messages, id) do
          {:ok, message} ->
            updated_message = Map.merge(message, attrs)

            if users_exist?(
                 state,
                 updated_message.sender_id,
                 updated_message.recipient_id
               ) do
              {{:ok, updated_message},
               put_in(state.messages[id], updated_message)}
            else
              {{:error, :user_not_found}, state}
            end

          :error ->
            {{:error, :not_found}, state}
        end
      end)
    end
  end

  def update_message(_server, _id, _attrs), do: {:error, :invalid_id}

  def delete_message(server, id) when valid_id(id) do
    Agent.get_and_update(server, fn state ->
      case Map.pop(state.messages, id) do
        {nil, _messages} ->
          {{:error, :not_found}, state}

        {message, messages} ->
          {{:ok, message}, %{state | messages: messages}}
      end
    end)
  end

  def delete_message(_server, _id), do: {:error, :invalid_id}

  # Executa envios em tarefas simultâneas, limitadas por max_concurrency.
  def send_messages_concurrently(server, requests, max_concurrency)
      when is_list(requests) and is_integer(max_concurrency) and max_concurrency > 0 do
    requests
    |> Task.async_stream(
      fn attrs -> create_message(server, attrs) end,
      max_concurrency: max_concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
  end

  def send_messages_concurrently(_server, _requests, _max_concurrency),
    do: {:error, :invalid_arguments}

  defp validate_user_attrs(%{name: name}) when nonempty_string(name),
    do: {:ok, name}

  defp validate_user_attrs(_attrs), do: {:error, :invalid_user_data}

  defp validate_message_attrs(%{
         sender_id: sender_id,
         recipient_id: recipient_id,
         body: body
       })
       when valid_id(sender_id) and valid_id(recipient_id) and nonempty_string(body) do
    {:ok, %{sender_id: sender_id, recipient_id: recipient_id, body: body}}
  end

  defp validate_message_attrs(_attrs), do: {:error, :invalid_message_data}

  defp validate_message_update(attrs) when is_map(attrs) and map_size(attrs) > 0 do
    valid_fields = [:sender_id, :recipient_id, :body]

    if Enum.all?(attrs, fn {key, value} ->
         key in valid_fields and valid_message_field?(key, value)
       end) do
      :ok
    else
      {:error, :invalid_message_data}
    end
  end

  defp validate_message_update(_attrs), do: {:error, :invalid_message_data}

  defp valid_message_field?(:sender_id, id) when valid_id(id), do: true
  defp valid_message_field?(:recipient_id, id) when valid_id(id), do: true
  defp valid_message_field?(:body, body) when nonempty_string(body), do: true
  defp valid_message_field?(_key, _value), do: false

  defp users_exist?(state, sender_id, recipient_id) do
    Map.has_key?(state.users, sender_id) and Map.has_key?(state.users, recipient_id)
  end
end
