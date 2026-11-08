# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Fazer uso de macros.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageSystem.CRUD do
  @moduledoc false

  defmacro crud_api(collection, singular) do
    create_function = String.to_atom("create_#{singular}")
    get_function = String.to_atom("get_#{singular}")
    list_function = String.to_atom("list_#{collection}")
    update_function = String.to_atom("update_#{singular}")
    delete_function = String.to_atom("delete_#{singular}")

    quote do
      def unquote(create_function)(attrs) do
        GenServer.call(__MODULE__, {:create, unquote(collection), attrs})
      end

      def unquote(get_function)(id) do
        GenServer.call(__MODULE__, {:get, unquote(collection), id})
      end

      def unquote(list_function)() do
        GenServer.call(__MODULE__, {:list, unquote(collection)})
      end

      def unquote(update_function)(id, attrs) do
        GenServer.call(__MODULE__, {:update, unquote(collection), id, attrs})
      end

      def unquote(delete_function)(id) do
        GenServer.call(__MODULE__, {:delete, unquote(collection), id})
      end
    end
  end
end

defmodule MessageSystem do
  use GenServer
  require MessageSystem.CRUD

  @state_key {__MODULE__, :state}

  MessageSystem.CRUD.crud_api(:users, :user)
  MessageSystem.CRUD.crud_api(:messages, :message)

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Mantém o estado disponível caso o processo seja reiniciado pelo supervisor.
    {:ok, :persistent_term.get(@state_key, empty_state())}
  end

  @impl true
  def handle_call({:create, resource, attrs}, _from, state)
      when resource in [:users, :messages] do
    id = state.next_id[resource]

    with :ok <- validate_attributes(resource, attrs),
         record = Map.put(attrs, :id, id),
         :ok <- validate_record(resource, record, state) do
      new_state =
        state
        |> put_in([:records, resource, id], record)
        |> update_in([:next_id, resource], &(&1 + 1))

      commit({:ok, record}, new_state)
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, resource, id}, _from, state)
      when resource in [:users, :messages] do
    case Map.fetch(state.records[resource], id) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:list, resource}, _from, state)
      when resource in [:users, :messages] do
    records =
      state.records[resource]
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, records, state}
  end

  def handle_call({:update, resource, id, attrs}, _from, state)
      when resource in [:users, :messages] do
    with {:ok, existing} <- fetch_record(state, resource, id),
         :ok <- validate_attributes(resource, attrs),
         updated = Map.merge(existing, attrs),
         :ok <- validate_record(resource, updated, state) do
      new_state = put_in(state, [:records, resource, id], updated)
      commit({:ok, updated}, new_state)
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, resource, id}, _from, state)
      when resource in [:users, :messages] do
    case fetch_record(state, resource, id) do
      {:ok, record} ->
        new_state = update_in(state, [:records, resource], &Map.delete(&1, id))

        # Evita referências inválidas quando um usuário é removido.
        new_state =
          if resource == :users do
            update_in(new_state, [:records, :messages], fn messages ->
              messages
              |> Enum.reject(fn {_message_id, message} ->
                message.sender_id == id or message.recipient_id == id
              end)
              |> Map.new()
            end)
          else
            new_state
          end

        commit({:ok, record}, new_state)

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(_request, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  defp commit(reply, new_state) do
    :persistent_term.put(@state_key, new_state)
    {:reply, reply, new_state}
  end

  defp empty_state do
    %{
      records: %{users: %{}, messages: %{}},
      next_id: %{users: 1, messages: 1}
    }
  end

  defp fetch_record(state, resource, id) do
    case Map.fetch(state.records[resource], id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  defp validate_attributes(resource, attrs) when is_map(attrs) do
    allowed_fields =
      case resource do
        :users -> [:name, :email]
        :messages -> [:sender_id, :recipient_id, :body]
      end

    cond do
      Map.has_key?(attrs, :id) ->
        {:error, :id_is_read_only}

      Enum.any?(Map.keys(attrs), &(&1 not in allowed_fields)) ->
        {:error, :invalid_attributes}

      true ->
        :ok
    end
  end

  defp validate_attributes(_resource, _attrs), do: {:error, :attributes_must_be_a_map}

  defp validate_record(:users, %{name: name} = user, _state) do
    cond do
      not valid_text?(name) ->
        {:error, :invalid_name}

      Map.has_key?(user, :email) and not valid_text?(user.email) ->
        {:error, :invalid_email}

      true ->
        :ok
    end
  end

  defp validate_record(:users, _user, _state), do: {:error, :name_required}

  defp validate_record(
         :messages,
         %{sender_id: sender_id, recipient_id: recipient_id, body: body},
         state
       ) do
    cond do
      not is_integer(sender_id) or not Map.has_key?(state.records.users, sender_id) ->
        {:error, :sender_not_found}

      not is_integer(recipient_id) or not Map.has_key?(state.records.users, recipient_id) ->
        {:error, :recipient_not_found}

      not valid_text?(body) ->
        {:error, :invalid_body}

      true ->
        :ok
    end
  end

  defp validate_record(:messages, _message, _state), do: {:error, :required_fields_missing}

  defp valid_text?(value), do: is_binary(value) and String.trim(value) != ""
end

defmodule MessageSystem.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {MessageSystem, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
