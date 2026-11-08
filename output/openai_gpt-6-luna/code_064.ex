# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionCRUD do
  defmacro __using__(opts) do
    entities = Keyword.fetch!(opts, :entities)

    definitions =
      Enum.map(entities, fn entity ->
        entity_name = Atom.to_string(entity)
        plural = if entity == :user, do: :users, else: :sessions

        create_name = String.to_atom("create_#{entity_name}")
        get_name = String.to_atom("get_#{entity_name}")
        list_name = String.to_atom("list_#{plural}")
        update_name = String.to_atom("update_#{entity_name}")
        delete_name = String.to_atom("delete_#{entity_name}")

        quote do
          def unquote(create_name)(attrs) do
            UserSessionManager.request({:crud, :create, unquote(entity), attrs})
          end

          def unquote(get_name)(id) do
            UserSessionManager.request({:crud, :get, unquote(entity), id})
          end

          def unquote(list_name)() do
            UserSessionManager.request({:crud, :list, unquote(entity), nil})
          end

          def unquote(update_name)(id, attrs) do
            UserSessionManager.request({:crud, :update, unquote(entity), {id, attrs}})
          end

          def unquote(delete_name)(id) do
            UserSessionManager.request({:crud, :delete, unquote(entity), id})
          end
        end
      end)

    quote do
      unquote_splicing(definitions)
    end
  end
end

defmodule UserSessionManager do
  use GenServer

  @checkpoint {__MODULE__, :checkpoint}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def request(message) do
    GenServer.call(__MODULE__, message)
  end

  def subscribe(pid \\ self()) when is_pid(pid) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  def history do
    GenServer.call(__MODULE__, :history)
  end

  @impl true
  def init(opts) do
    threshold =
      case Keyword.get(opts, :threshold, 10) do
        value when is_integer(value) and value > 0 -> value
        _ -> 10
      end

    defaults = %{users: %{}, sessions: %{}, history: [], next_id: 1}

    persisted =
      case :persistent_term.get(@checkpoint, nil) do
        value when is_map(value) -> Map.merge(defaults, value)
        _ -> defaults
      end

    {:ok,
     Map.merge(persisted, %{
       threshold: threshold,
       subscribers: MapSet.new(),
       monitors: %{}
     })}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if MapSet.member?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      monitor = Process.monitor(pid)

      updated_state = %{
        state
        | subscribers: MapSet.put(state.subscribers, pid),
          monitors: Map.put(state.monitors, monitor, pid)
      }

      {:reply, :ok, updated_state}
    end
  end

  def handle_call({:crud, operation, entity, args}, _from, state) do
    previous_session_count = map_size(state.sessions)
    {reply, updated_state, id} = perform(operation, entity, args, state)

    entry = %{
      operation: operation,
      entity: entity,
      id: id,
      outcome: outcome(reply),
      timestamp: System.system_time(:millisecond)
    }

    updated_state = %{updated_state | history: [entry | updated_state.history]}
    notify_if_threshold_crossed(state, updated_state, previous_session_count)
    persist(updated_state)

    {:reply, reply, updated_state}
  end

  def handle_call(_message, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    updated_state = %{
      state
      | subscribers: MapSet.delete(state.subscribers, pid),
        monitors: Map.delete(state.monitors, monitor)
    }

    {:noreply, updated_state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp perform(:create, entity, attrs, state) when entity in [:user, :session] do
    with {:ok, attrs} <- normalize_attrs(attrs) do
      collection = collection_key(entity)
      id = state.next_id
      record = Map.put(attrs, :id, id)

      updated_state =
        state
        |> Map.update!(collection, &Map.put(&1, id, record))
        |> Map.put(:next_id, id + 1)

      {{:ok, record}, updated_state, id}
    else
      {:error, reason} -> {{:error, reason}, state, nil}
    end
  end

  defp perform(:get, entity, id, state) when entity in [:user, :session] do
    case Map.fetch(Map.fetch!(state, collection_key(entity)), id) do
      {:ok, record} -> {{:ok, record}, state, id}
      :error -> {{:error, :not_found}, state, id}
    end
  end

  defp perform(:list, entity, _args, state) when entity in [:user, :session] do
    records =
      state
      |> Map.fetch!(collection_key(entity))
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {{:ok, records}, state, nil}
  end

  defp perform(:update, entity, {id, attrs}, state) when entity in [:user, :session] do
    with {:ok, attrs} <- normalize_attrs(attrs) do
      collection = collection_key(entity)
      records = Map.fetch!(state, collection)

      case Map.fetch(records, id) do
        {:ok, record} ->
          updated_record = record |> Map.merge(attrs) |> Map.put(:id, id)
          updated_records = Map.put(records, id, updated_record)
          {{:ok, updated_record}, Map.put(state, collection, updated_records), id}

        :error ->
          {{:error, :not_found}, state, id}
      end
    else
      {:error, reason} -> {{:error, reason}, state, id}
    end
  end

  defp perform(:delete, entity, id, state) when entity in [:user, :session] do
    collection = collection_key(entity)
    records = Map.fetch!(state, collection)

    case Map.pop(records, id) do
      {nil, _records} ->
        {{:error, :not_found}, state, id}

      {record, updated_records} ->
        {{:ok, record}, Map.put(state, collection, updated_records), id}
    end
  end

  defp perform(_operation, _entity, _args, state) do
    {{:error, :invalid_request}, state, nil}
  end

  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}

  defp normalize_attrs(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      {:ok, Map.new(attrs)}
    else
      {:error, :invalid_attributes}
    end
  end

  defp normalize_attrs(_attrs), do: {:error, :invalid_attributes}

  defp collection_key(:user), do: :users
  defp collection_key(:session), do: :sessions

  defp outcome({:error, _reason}), do: :error
  defp outcome(_reply), do: :ok

  defp notify_if_threshold_crossed(old_state, new_state, old_count) do
    new_count = map_size(new_state.sessions)

    if old_count < new_state.threshold and new_count >= new_state.threshold do
      notification =
        {:notification, :session_threshold_reached, new_count, new_state.threshold}

      Enum.each(new_state.subscribers, &send(&1, notification))
    end

    :ok
  end

  defp persist(state) do
    checkpoint = Map.take(state, [:users, :sessions, :history, :next_id])
    :persistent_term.put(@checkpoint, checkpoint)
  end
end

defmodule UserSession do
  use UserSessionCRUD, entities: [:user, :session]

  def subscribe(pid \\ self()), do: UserSessionManager.subscribe(pid)
  def history, do: UserSessionManager.history()
end

defmodule UserSessionSupervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {UserSessionManager, threshold: Keyword.get(opts, :threshold, 10)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
