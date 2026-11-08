# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Fazer uso de macros.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageExchange.Atomic do
  @moduledoc false

  defmacro atomic(do: block) do
    quote do
      try do
        case unquote(block) do
          {:ok, _data, _result} = success -> success
          {:error, _reason} = error -> error
          other -> {:error, {:invalid_transaction_result, other}}
        end
      rescue
        exception -> {:error, {:exception, Exception.message(exception)}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end
    end
  end
end

defmodule MessageExchange do
  use GenServer
  require MessageExchange.Atomic

  @moduledoc """
  Sistema de troca de mensagens com operações idempotentes e atualizações atômicas.

  Inicie `MessageExchange.Supervisor` antes de enviar operações. Cada operação
  recebe uma chave de idempotência e pode ser consultada pelo seu identificador.
  """

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def submit(key, command), do: submit(__MODULE__, key, command)

  def submit(server, key, command) do
    GenServer.call(server, {:submit, key, command})
  end

  def operation(id), do: operation(__MODULE__, id)

  def operation(server, id) do
    GenServer.call(server, {:operation, id})
  end

  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  def get_user(id), do: get_user(__MODULE__, id)

  def get_user(server, id) do
    GenServer.call(server, {:get_user, id})
  end

  def list_users(server \\ __MODULE__) do
    GenServer.call(server, :list_users)
  end

  def get_message(id), do: get_message(__MODULE__, id)

  def get_message(server, id) do
    GenServer.call(server, {:get_message, id})
  end

  def list_messages(server \\ __MODULE__) do
    GenServer.call(server, :list_messages)
  end

  def create_user(key, attrs), do: create_user(__MODULE__, key, attrs)

  def create_user(server, key, attrs) do
    submit(server, key, {:create_user, attrs})
  end

  def update_user(key, id, attrs), do: update_user(__MODULE__, key, id, attrs)

  def update_user(server, key, id, attrs) do
    submit(server, key, {:update_user, id, attrs})
  end

  def delete_user(key, id), do: delete_user(__MODULE__, key, id)

  def delete_user(server, key, id) do
    submit(server, key, {:delete_user, id})
  end

  def create_message(key, attrs), do: create_message(__MODULE__, key, attrs)

  def create_message(server, key, attrs) do
    submit(server, key, {:create_message, attrs})
  end

  def update_message(key, id, attrs), do: update_message(__MODULE__, key, id, attrs)

  def update_message(server, key, id, attrs) do
    submit(server, key, {:update_message, id, attrs})
  end

  def delete_message(key, id), do: delete_message(__MODULE__, key, id)

  def delete_message(server, key, id) do
    submit(server, key, {:delete_message, id})
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       task_supervisor: Keyword.get(opts, :task_supervisor, MessageExchange.TaskSupervisor),
       data: %{
         users: %{},
         messages: %{},
         next_user_id: 1,
         next_message_id: 1
       },
       operations: %{},
       idempotency_keys: %{},
       next_operation_id: 1
     }}
  end

  @impl true
  def handle_call({:submit, nil, _command}, _from, state) do
    {:reply, {:error, :idempotency_key_required}, state}
  end

  def handle_call({:submit, key, _command}, _from, state)
      when is_map_key(state.idempotency_keys, key) do
    id = Map.fetch!(state.idempotency_keys, key)
    operation = Map.fetch!(state.operations, id)
    {:reply, {:ok, id, operation.status}, state}
  end

  def handle_call({:submit, key, command}, _from, state) do
    id = state.next_operation_id
    operation = %{id: id, key: key, status: :running, result: nil, error: nil}
    server_pid = self()

    task =
      Task.Supervisor.start_child(state.task_supervisor, fn ->
        GenServer.cast(server_pid, {:execute, id, command})
      end)

    case task do
      {:ok, _pid} ->
        next_state = %{
          state
          | operations: Map.put(state.operations, id, operation),
            idempotency_keys: Map.put(state.idempotency_keys, key, id),
            next_operation_id: id + 1
        }

        {:reply, {:ok, id, :running}, next_state}

      {:error, reason} ->
        {:reply, {:error, {:cannot_start_operation, reason}}, state}
    end
  end

  def handle_call({:operation, id}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:snapshot, _from, state) do
    {:reply, state.data, state}
  end

  def handle_call({:get_user, id}, _from, state) do
    {:reply, fetch_record(state.data.users, id), state}
  end

  def handle_call(:list_users, _from, state) do
    users = state.data.users |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, users, state}
  end

  def handle_call({:get_message, id}, _from, state) do
    {:reply, fetch_record(state.data.messages, id), state}
  end

  def handle_call(:list_messages, _from, state) do
    messages = state.data.messages |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, messages, state}
  end

  @impl true
  def handle_cast({:execute, id, command}, state) do
    case transact(state.data, command) do
      {:ok, data, result} ->
        operation = Map.fetch!(state.operations, id)
        completed = %{operation | status: :completed, result: result}
        next_state = %{state | data: data, operations: Map.put(state.operations, id, completed)}
        {:noreply, next_state}

      {:error, reason} ->
        operation = Map.fetch!(state.operations, id)
        failed = %{operation | status: :failed, error: reason}
        next_state = %{state | operations: Map.put(state.operations, id, failed)}
        {:noreply, next_state}
    end
  end

  defp fetch_record(records, id) do
    case Map.fetch(records, id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  defp transact(data, command) do
    MessageExchange.Atomic.atomic do
      dispatch(data, command)
    end
  end

  defp dispatch(data, {:create_user, attrs}) when is_map(attrs) do
    id = data.next_user_id
    user = attrs |> Map.delete(:id) |> Map.put(:id, id)

    next_data = %{
      data
      | users: Map.put(data.users, id, user),
        next_user_id: id + 1
    }

    {:ok, next_data, user}
  end

  defp dispatch(_data, {:create_user, _attrs}), do: {:error, :invalid_attributes}

  defp dispatch(data, {:update_user, id, attrs}) when is_map(attrs) do
    case Map.fetch(data.users, id) do
      {:ok, user} ->
        updated = user |> Map.merge(Map.delete(attrs, :id)) |> Map.put(:id, id)
        {:ok, %{data | users: Map.put(data.users, id, updated)}, updated}

      :error ->
        {:error, :user_not_found}
    end
  end

  defp dispatch(_data, {:update_user, _id, _attrs}), do: {:error, :invalid_attributes}

  defp dispatch(data, {:delete_user, id}) do
    cond do
      not Map.has_key?(data.users, id) ->
        {:error, :user_not_found}

      Enum.any?(data.messages, fn {_message_id, message} ->
        message.sender_id == id or message.recipient_id == id
      end) ->
        {:error, :user_has_messages}

      true ->
        {:ok, %{data | users: Map.delete(data.users, id)}, :deleted}
    end
  end

  defp dispatch(data, {:create_message, attrs}) when is_map(attrs) do
    with true <- Map.has_key?(attrs, :sender_id),
         true <- Map.has_key?(attrs, :recipient_id),
         true <- Map.has_key?(attrs, :body),
         true <- Map.has_key?(data.users, attrs.sender_id),
         true <- Map.has_key?(data.users, attrs.recipient_id) do
      id = data.next_message_id

      message =
        attrs
        |> Map.delete(:id)
        |> Map.put(:id, id)

      next_data = %{
        data
        | messages: Map.put(data.messages, id, message),
          next_message_id: id + 1
      }

      {:ok, next_data, message}
    else
      false -> {:error, :invalid_message_or_user}
    end
  end

  defp dispatch(_data, {:create_message, _attrs}), do: {:error, :invalid_attributes}

  defp dispatch(data, {:update_message, id, attrs}) when is_map(attrs) do
    case Map.fetch(data.messages, id) do
      {:ok, message} ->
        updated = message |> Map.merge(Map.delete(attrs, :id)) |> Map.put(:id, id)

        if valid_message?(data.users, updated) do
          {:ok, %{data | messages: Map.put(data.messages, id, updated)}, updated}
        else
          {:error, :invalid_message_or_user}
        end

      :error ->
        {:error, :message_not_found}
    end
  end

  defp dispatch(_data, {:update_message, _id, _attrs}), do: {:error, :invalid_attributes}

  defp dispatch(data, {:delete_message, id}) do
    if Map.has_key?(data.messages, id) do
      {:ok, %{data | messages: Map.delete(data.messages, id)}, :deleted}
    else
      {:error, :message_not_found}
    end
  end

  defp dispatch(_data, _command), do: {:error, :unknown_command}

  defp valid_message?(users, message) do
    Map.has_key?(message, :sender_id) and
      Map.has_key?(message, :recipient_id) and
      Map.has_key?(message, :body) and
      Map.has_key?(users, message.sender_id) and
      Map.has_key?(users, message.recipient_id)
  end
end

defmodule MessageExchange.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    task_supervisor = Keyword.get(opts, :task_supervisor_name, MessageExchange.TaskSupervisor)
    server_name = Keyword.get(opts, :server_name, MessageExchange)

    children = [
      {Task.Supervisor, name: task_supervisor},
      {MessageExchange, name: server_name, task_supervisor: task_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
