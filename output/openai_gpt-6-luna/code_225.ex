# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Gerar notificações quando determinadas condições forem atingidas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  use Supervisor

  @registry __MODULE__.Registry
  @dynamic_supervisor __MODULE__.DynamicSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(kind, id, attrs) when is_map(attrs) do
    with :ok <- validate_kind(kind) do
      case DynamicSupervisor.start_child(@dynamic_supervisor, {Entity, {kind, id, attrs}}) do
        {:ok, pid} ->
          {:ok, Entity.record(pid)}

        {:error, {:already_started, _pid}} ->
          {:error, :already_exists}

        {:error, {:already_registered, _pid}} ->
          {:error, :already_exists}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  def create(_kind, _id, _attrs), do: {:error, :invalid_attributes}

  def get(kind, id) do
    with :ok <- validate_kind(kind),
         {:ok, pid} <- find_entity(kind, id) do
      safe_call(pid, :get)
    end
  end

  def update(kind, id, attrs) when is_map(attrs) do
    with :ok <- validate_kind(kind),
         {:ok, pid} <- find_entity(kind, id) do
      safe_call(pid, {:update, attrs})
    end
  end

  def update(_kind, _id, _attrs), do: {:error, :invalid_attributes}

  def delete(kind, id) do
    with :ok <- validate_kind(kind),
         {:ok, pid} <- find_entity(kind, id) do
      safe_call(pid, :delete)
    end
  end

  def list(kind) do
    with :ok <- validate_kind(kind) do
      records =
        @registry
        |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [:"$1"]}])
        |> Enum.filter(fn
          {^kind, _id} -> true
          _ -> false
        end)
        |> Enum.flat_map(fn {^kind, id} ->
          case get(kind, id) do
            {:ok, record} -> [record]
            _ -> []
          end
        end)

      {:ok, records}
    end
  end

  def subscribe(kind, id, pid \\ self(), condition \\ :always)
      when is_pid(pid) do
    with :ok <- validate_kind(kind),
         :ok <- validate_condition(condition),
         {:ok, entity_pid} <- find_entity(kind, id) do
      safe_call(entity_pid, {:subscribe, pid, condition})
    end
  end

  def create_user(id, attrs), do: create(:user, id, attrs)
  def get_user(id), do: get(:user, id)
  def update_user(id, attrs), do: update(:user, id, attrs)
  def delete_user(id), do: delete(:user, id)
  def list_users, do: list(:user)
  def subscribe_user(id, pid \\ self(), condition \\ :always),
    do: subscribe(:user, id, pid, condition)

  def create_session(id, attrs), do: create(:session, id, attrs)
  def get_session(id), do: get(:session, id)
  def update_session(id, attrs), do: update(:session, id, attrs)
  def delete_session(id), do: delete(:session, id)
  def list_sessions, do: list(:session)
  def subscribe_session(id, pid \\ self(), condition \\ :always),
    do: subscribe(:session, id, pid, condition)

  defp validate_kind(kind) when kind in [:user, :session], do: :ok
  defp validate_kind(_kind), do: {:error, :invalid_kind}

  defp validate_condition(condition) do
    if Entity.valid_condition?(condition),
      do: :ok,
      else: {:error, :invalid_condition}
  end

  defp find_entity(kind, id) do
    case Registry.lookup(@registry, {kind, id}) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  defp safe_call(pid, message) do
    try do
      GenServer.call(pid, message)
    catch
      :exit, _reason -> {:error, :not_found}
    end
  end
end

defmodule UserSessionManager.Entity do
  use GenServer

  @registry UserSessionManager.Registry

  def start_link({kind, id, attrs}) do
    GenServer.start_link(
      __MODULE__,
      {kind, id, attrs},
      name: {:via, Registry, {@registry, {kind, id}}}
    )
  end

  def child_spec({kind, id, attrs}) do
    %{
      id: make_ref(),
      start: {__MODULE__, :start_link, [{kind, id, attrs}]},
      restart: :temporary
    }
  end

  def record(pid), do: GenServer.call(pid, :get)

  def valid_condition?(:always), do: true
  def valid_condition?(:created), do: true
  def valid_condition?(:updated), do: true
  def valid_condition?(:deleted), do: true
  def valid_condition?({:field_equals, _field, _value}), do: true
  def valid_condition?({:field_changed, _field}), do: true
  def valid_condition?(_condition), do: false

  @impl true
  def init({kind, id, attrs}) do
    {:ok, %{kind: kind, id: id, attrs: attrs, subscribers: %{}}}
  end

  @impl true
  def handle_call(:get, _from, state) do
    {:reply, {:ok, to_record(state)}, state}
  end

  def handle_call({:update, patch}, _from, state) do
    updated_attrs = Map.merge(state.attrs, patch)
    notify(state, :updated, state.attrs, updated_attrs)

    updated_state = %{state | attrs: updated_attrs}
    {:reply, {:ok, to_record(updated_state)}, updated_state}
  end

  def handle_call({:subscribe, pid, condition}, _from, state) do
    subscribers =
      case Map.get(state.subscribers, pid) do
        {_old_condition, old_ref} ->
          Process.demonitor(old_ref, [:flush])
          state.subscribers

        nil ->
          state.subscribers
      end

    ref = Process.monitor(pid)
    updated = Map.put(subscribers, pid, {condition, ref})
    {:reply, :ok, %{state | subscribers: updated}}
  end

  def handle_call(:delete, _from, state) do
    notify(state, :deleted, state.attrs, state.attrs)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    subscribers =
      case Map.get(state.subscribers, pid) do
        {_condition, ^ref} -> Map.delete(state.subscribers, pid)
        _ -> state.subscribers
      end

    {:noreply, %{state | subscribers: subscribers}}
  end

  defp to_record(state) do
    %{type: state.kind, id: state.id, attrs: state.attrs}
  end

  defp notify(state, event, old_attrs, new_attrs) do
    Enum.each(state.subscribers, fn {pid, {condition, _ref}} ->
      if matches?(condition, event, old_attrs, new_attrs) do
        send(pid, {
          :user_session_notification,
          state.kind,
          state.id,
          event,
          new_attrs
        })
      end
    end)
  end

  defp matches?(:always, _event, _old_attrs, _new_attrs), do: true
  defp matches?(event, event, _old_attrs, _new_attrs)
       when event in [:created, :updated, :deleted],
       do: true

  defp matches?({:field_equals, field, value}, _event, _old_attrs, new_attrs) do
    Map.fetch(new_attrs, field) == {:ok, value}
  end

  defp matches?({:field_changed, field}, :updated, old_attrs, new_attrs) do
    Map.fetch(old_attrs, field) != Map.fetch(new_attrs, field)
  end

  defp matches?(_condition, _event, _old_attrs, _new_attrs), do: false
end
