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

defmodule MessageSystem do
  defguardp valid_id?(id) when is_integer(id) and id > 0

  @max_name_length 100
  @max_message_length 5_000

  def start_link(opts \\ []) do
    Agent.start_link(
      fn ->
        %{
          users: %{},
          messages: %{},
          next_user_id: 1,
          next_message_id: 1
        }
      end,
      opts
    )
  end

  def run_concurrently(operations, max_concurrency \\ System.schedulers_online())

  def run_concurrently(operations, max_concurrency)
      when is_list(operations) and is_integer(max_concurrency) and max_concurrency > 0 do
    if Enum.all?(operations, &is_function(&1, 0)) do
      results =
        operations
        |> Task.async_stream(
          fn operation -> operation.() end,
          max_concurrency: max_concurrency,
          ordered: true,
          timeout: :infinity
        )
        |> Enum.to_list()

      {:ok, results}
    else
      {:error, :invalid_operations}
    end
  end

  def run_concurrently(_, _), do: {:error, :invalid_concurrency_options}

  def create_user(agent, name) do
    with :ok <- validate_name(name) do
      Agent.get_and_update(agent, fn state ->
        id = state.next_user_id
        user = %{id: id, name: String.trim(name)}

        {{:ok, user},
         %{state | users: Map.put(state.users, id, user), next_user_id: id + 1}}
      end)
    end
  end

  def get_user(agent, id) when valid_id?(id) do
    case Agent.get(agent, &Map.get(&1.users, id)) do
      nil -> {:error, :not_found}
      user -> {:ok, user}
    end
  end

  def get_user(_agent, _id), do: {:error, :invalid_id}

  def update_user(agent, id, attrs) when valid_id?(id) and is_map(attrs) do
    with {:ok, name} <- validate_user_attrs(attrs) do
      Agent.get_and_update(agent, fn state ->
        case Map.fetch(state.users, id) do
          {:ok, user} ->
            updated_user = %{user | name: String.trim(name)}
            {{:ok, updated_user}, %{state | users: Map.put(state.users, id, updated_user)}}

          :error ->
            {{:error, :not_found}, state}
        end
      end)
    end
  end

  def update_user(_agent, id, _attrs) when not is_integer(id) or id < 1,
    do: {:error, :invalid_id}

  def update_user(_agent, _id, _attrs), do: {:error, :invalid_attributes}

  def delete_user(agent, id) when valid_id?(id) do
    Agent.get_and_update(agent, fn state ->
      if Map.has_key?(state.users, id) do
        users = Map.delete(state.users, id)

        messages =
          state.messages
          |> Enum.reject(fn {_message_id, message} ->
            message.sender_id == id or message.recipient_id == id
          end)
          |> Map.new()

        {{:ok, :deleted}, %{state | users: users, messages: messages}}
      else
        {{:error, :not_found}, state}
      end
    end)
  end

  def delete_user(_agent, _id), do: {:error, :invalid_id}

  def send_message(agent, sender_id, recipient_id, body) do
    with :ok <- validate_id(sender_id),
         :ok <- validate_id(recipient_id),
         :ok <- validate_body(body) do
      Agent.get_and_update(agent, fn state ->
        if Map.has_key?(state.users, sender_id) and Map.has_key?(state.users, recipient_id) do
          id = state.next_message_id

          message = %{
            id: id,
            sender_id: sender_id,
            recipient_id: recipient_id,
            body: String.trim(body),
            inserted_at: DateTime.utc_now()
          }

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

  def get_message(agent, id) when valid_id?(id) do
    case Agent.get(agent, &Map.get(&1.messages, id)) do
      nil -> {:error, :not_found}
      message -> {:ok, message}
    end
  end

  def get_message(_agent, _id), do: {:error, :invalid_id}

  def update_message(agent, id, attrs) when valid_id?(id) and is_map(attrs) do
    with {:ok, body} <- validate_message_attrs(attrs) do
      Agent.get_and_update(agent, fn state ->
        case Map.fetch(state.messages, id) do
          {:ok, message} ->
            updated_message = %{message | body: String.trim(body)}
            {{:ok, updated_message}, %{state | messages: Map.put(state.messages, id, updated_message)}}

          :error ->
            {{:error, :not_found}, state}
        end
      end)
    end
  end

  def update_message(_agent, id, _attrs) when not is_integer(id) or id < 1,
    do: {:error, :invalid_id}

  def update_message(_agent, _id, _attrs), do: {:error, :invalid_attributes}

  def delete_message(agent, id) when valid_id?(id) do
    Agent.get_and_update(agent, fn state ->
      if Map.has_key?(state.messages, id) do
        {{:ok, :deleted}, %{state | messages: Map.delete(state.messages, id)}}
      else
        {{:error, :not_found}, state}
      end
    end)
  end

  def delete_message(_agent, _id), do: {:error, :invalid_id}

  def list_messages(agent, user_id) when valid_id?(user_id) do
    Agent.get(agent, fn state ->
      state.messages
      |> Map.values()
      |> Enum.filter(fn message ->
        message.sender_id == user_id or message.recipient_id == user_id
      end)
      |> Enum.sort_by(& &1.id)
    end)
  end

  def list_messages(_agent, _user_id), do: {:error, :invalid_id}

  defp validate_id(id) when valid_id?(id), do: :ok
  defp validate_id(_id), do: {:error, :invalid_id}

  defp validate_name(name)
       when is_binary(name) and byte_size(name) > 0 and byte_size(name) <= @max_name_length do
    if String.valid?(name) and String.trim(name) != "" do
      :ok
    else
      {:error, :invalid_name}
    end
  end

  defp validate_name(_name), do: {:error, :invalid_name}

  defp validate_body(body)
       when is_binary(body) and byte_size(body) > 0 and byte_size(body) <= @max_message_length do
    if String.valid?(body) and String.trim(body) != "" do
      :ok
    else
      {:error, :invalid_body}
    end
  end

  defp validate_body(_body), do: {:error, :invalid_body}

  defp validate_user_attrs(%{name: name} = attrs) when map_size(attrs) == 1 do
    with :ok <- validate_name(name), do: {:ok, name}
  end

  defp validate_user_attrs(_attrs), do: {:error, :invalid_attributes}

  defp validate_message_attrs(%{body: body} = attrs) when map_size(attrs) == 1 do
    with :ok <- validate_body(body), do: {:ok, body}
  end

  defp validate_message_attrs(_attrs), do: {:error, :invalid_attributes}
end
