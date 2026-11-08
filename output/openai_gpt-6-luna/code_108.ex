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

defmodule Messaging.CRUD do
  defmacro __using__(_opts) do
    quote do
      def create_user(id, attrs), do: Messaging.Store.create(:users, id, attrs)
      def get_user(id), do: Messaging.Store.read(:users, id)
      def update_user(id, attrs), do: Messaging.Store.update(:users, id, attrs)
      def delete_user(id), do: Messaging.Store.delete(:users, id)

      def create_message(id, attrs), do: Messaging.Store.create(:messages, id, attrs)
      def get_message(id), do: Messaging.Store.read(:messages, id)
      def update_message(id, attrs), do: Messaging.Store.update(:messages, id, attrs)
      def delete_message(id), do: Messaging.Store.delete(:messages, id)
    end
  end
end

defmodule Messaging.Store do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: Keyword.get(opts, :name, __MODULE__))
  end

  def create(table, id, attrs), do: GenServer.call(__MODULE__, {:create, table, id, attrs})
  def read(table, id), do: GenServer.call(__MODULE__, {:read, table, id})
  def update(table, id, attrs), do: GenServer.call(__MODULE__, {:update, table, id, attrs})
  def delete(table, id), do: GenServer.call(__MODULE__, {:delete, table, id})

  def send_message(id, from, to, body) do
    GenServer.call(__MODULE__, {:send_message, id, from, to, body})
  end

  @impl true
  def init(_initial_state) do
    {:ok, %{users: %{}, messages: %{}}}
  end

  @impl true
  def handle_call({:create, table, id, attrs}, _from, state) do
    reply_transaction(create_record(table, id, attrs, state), state)
  end

  def handle_call({:read, table, id}, _from, state) do
    case table_map(state, table) do
      {:ok, records} ->
        case Map.fetch(records, id) do
          {:ok, record} -> {:reply, {:ok, record}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end

      :error ->
        {:reply, {:error, :invalid_table}, state}
    end
  end

  def handle_call({:update, table, id, attrs}, _from, state) do
    reply_transaction(update_record(table, id, attrs, state), state)
  end

  def handle_call({:delete, table, id}, _from, state) do
    reply_transaction(delete_record(table, id, state), state)
  end

  def handle_call({:send_message, id, from, to, body}, _from, state) do
    reply_transaction(
      create_record(:messages, id, %{from: from, to: to, body: body}, state),
      state
    )
  end

  defp create_record(table, id, attrs, state) when table in [:users, :messages] do
    with true <- not is_nil(id) || {:error, :invalid_id},
         true <- is_map(attrs) || {:error, :invalid_attributes},
         {:ok, records} <- table_map(state, table),
         false <- Map.has_key?(records, id) || {:error, :already_exists},
         record = Map.put(attrs, :id, id),
         :ok <- validate_record(table, record, state) do
      next_state = put_in(state, [Access.key!(table), id], record)
      {:ok, record, next_state}
    else
      {:error, _reason} = error -> error
      true -> {:error, :already_exists}
      false -> {:error, :invalid_record}
    end
  end

  defp create_record(_table, _id, _attrs, _state), do: {:error, :invalid_table}

  defp update_record(table, id, attrs, state) when table in [:users, :messages] do
    with true <- is_map(attrs) || {:error, :invalid_attributes},
         {:ok, records} <- table_map(state, table),
         {:ok, current} <- Map.fetch(records, id),
         record = Map.merge(current, attrs) |> Map.put(:id, id),
         :ok <- validate_record(table, record, state) do
      next_state = put_in(state, [Access.key!(table), id], record)
      {:ok, record, next_state}
    else
      {:error, _reason} = error -> error
      :error -> {:error, :not_found}
      false -> {:error, :invalid_attributes}
    end
  end

  defp update_record(_table, _id, _attrs, _state), do: {:error, :invalid_table}

  defp delete_record(table, id, state) when table in [:users, :messages] do
    with {:ok, records} <- table_map(state, table),
         {:ok, record} <- Map.fetch(records, id),
         :ok <- ensure_deletable(table, id, state) do
      next_state = put_in(state, [Access.key!(table)], Map.delete(records, id))
      {:ok, record, next_state}
    else
      {:error, _reason} = error -> error
      :error -> {:error, :not_found}
    end
  end

  defp delete_record(_table, _id, _state), do: {:error, :invalid_table}

  defp validate_record(:users, _record, _state), do: :ok

  defp validate_record(:messages, record, state) do
    with true <- Map.has_key?(record, :from) || {:error, :missing_sender},
         true <- Map.has_key?(record, :to) || {:error, :missing_recipient},
         true <- Map.has_key?(record, :body) || {:error, :missing_body},
         true <- Map.has_key?(state.users, record.from) || {:error, :sender_not_found},
         true <- Map.has_key?(state.users, record.to) || {:error, :recipient_not_found} do
      :ok
    else
      {:error, _reason} = error -> error
      false -> {:error, :invalid_message}
    end
  end

  defp ensure_deletable(:users, id, state) do
    referenced? =
      Enum.any?(state.messages, fn {_message_id, message} ->
        message.from == id or message.to == id
      end)

    if referenced?, do: {:error, :user_has_messages}, else: :ok
  end

  defp ensure_deletable(_table, _id, _state), do: :ok

  defp table_map(state, table) when table in [:users, :messages],
    do: {:ok, Map.fetch!(state, table)}

  defp table_map(_state, _table), do: :error

  defp reply_transaction({:ok, value, next_state}, _state),
    do: {:reply, {:ok, value}, next_state}

  defp reply_transaction({:error, reason}, state),
    do: {:reply, {:error, reason}, state}
end

defmodule Messaging.Operations do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{by_key: %{}, operations: %{}},
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def submit_message(key, from, to, body) do
    GenServer.call(__MODULE__, {:submit_message, key, from, to, body})
  end

  def status(operation_id) do
    GenServer.call(__MODULE__, {:status, operation_id})
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:submit_message, nil, _from, _to, _body}, _from, state) do
    {:reply, {:error, :invalid_idempotency_key}, state}
  end

  def handle_call({:submit_message, key, from, to, body}, _from, state) do
    case Map.fetch(state.by_key, key) do
      {:ok, operation_id} ->
        {:reply, {:ok, operation_id}, state}

      :error ->
        operation_id = {:message_operation, System.unique_integer([:positive, :monotonic])}

        operation = %{
          id: operation_id,
          key: key,
          status: :running,
          result: nil,
          started_at: System.system_time(:millisecond),
          finished_at: nil
        }

        next_state = %{
          state
          | by_key: Map.put(state.by_key, key, operation_id),
            operations: Map.put(state.operations, operation_id, operation)
        }

        task = fn ->
          result =
            try do
              Messaging.Store.send_message(operation_id, from, to, body)
            rescue
              error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
            catch
              kind, reason -> {:error, {kind, reason}}
            end

          GenServer.cast(__MODULE__, {:complete, operation_id, result})
        end

        case Task.Supervisor.start_child(Messaging.TaskSupervisor, task) do
          {:ok, _pid} ->
            {:reply, {:ok, operation_id}, next_state}

          {:error, reason} ->
            failed = %{
              operation
              | status: :failed,
                result: {:error, reason},
                finished_at: System.system_time(:millisecond)
            }

            failed_state = put_in(next_state, [:operations, operation_id], failed)
            {:reply, {:ok, operation_id}, failed_state}
        end
    end
  end

  def handle_call({:status, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_cast({:complete, operation_id, result}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: :running} = operation} ->
        status = if match?({:ok, _}, result), do: :succeeded, else: :failed

        updated = %{
          operation
          | status: status,
            result: result,
            finished_at: System.system_time(:millisecond)
        }

        {:noreply, put_in(state, [:operations, operation_id], updated)}

      _ ->
        {:noreply, state}
    end
  end
end

defmodule Messaging do
  use Messaging.CRUD

  def start_link(opts \\ []) do
    Messaging.Supervisor.start_link(opts)
  end

  def send_message(idempotency_key, from, to, body) do
    Messaging.Operations.submit_message(idempotency_key, from, to, body)
  end

  def operation_status(operation_id) do
    Messaging.Operations.status(operation_id)
  end
end

defmodule Messaging.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    children = [
      {Task.Supervisor, name: Messaging.TaskSupervisor},
      {Messaging.Store, name: Messaging.Store},
      {Messaging.Operations, name: Messaging.Operations}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
