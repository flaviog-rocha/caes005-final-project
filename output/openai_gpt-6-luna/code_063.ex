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
  @moduledoc false

  defmacro crud_for(resource) do
    resource = Macro.expand(resource, __CALLER__)

    create_function = String.to_atom("create_#{resource}")
    get_function = String.to_atom("get_#{resource}")
    update_function = String.to_atom("update_#{resource}")
    delete_function = String.to_atom("delete_#{resource}")
    list_function = String.to_atom("list_#{resource}")

    quote do
      def unquote(create_function)(attrs),
        do: GenServer.call(__MODULE__, {:create, unquote(resource), attrs})

      def unquote(get_function)(id),
        do: GenServer.call(__MODULE__, {:get, unquote(resource), id})

      def unquote(update_function)(id, attrs),
        do: GenServer.call(__MODULE__, {:update, unquote(resource), id, attrs})

      def unquote(delete_function)(id),
        do: GenServer.call(__MODULE__, {:delete, unquote(resource), id})

      def unquote(list_function)(),
        do: GenServer.call(__MODULE__, {:list, unquote(resource)})
    end
  end
end

defmodule UserSessionStore do
  use GenServer
  require UserSessionCRUD

  @persist_key {__MODULE__, :data}

  UserSessionCRUD.crud_for(:user)
  UserSessionCRUD.crud_for(:session)

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def subscribe do
    GenServer.call(__MODULE__, {:subscribe, self()})
  end

  def unsubscribe do
    GenServer.call(__MODULE__, {:unsubscribe, self()})
  end

  # Retorna o histórico mais recente primeiro.
  def history(limit \\ :all) do
    GenServer.call(__MODULE__, {:history, limit})
  end

  @impl true
  def init(opts) do
    data = :persistent_term.get(@persist_key, initial_data())

    {:ok,
     %{
       data: data,
       subscribers: %{},
       session_threshold: valid_threshold(Keyword.get(opts, :session_threshold, 10))
     }}
  end

  @impl true
  def handle_call({:create, resource, attrs}, _from, state)
      when resource in [:user, :session] do
    case normalize_attrs(attrs) do
      {:ok, attrs} ->
        data = state.data
        id = Map.fetch!(data.next_ids, resource)
        now = now()

        record =
          attrs
          |> Map.put(:id, id)
          |> Map.put(:inserted_at, now)
          |> Map.put(:updated_at, now)

        previous_session_count = map_size(data.sessions)
        records = Map.put(Map.fetch!(data, resource), id, record)

        data =
          data
          |> Map.put(resource, records)
          |> put_in([:next_ids, resource], id + 1)
          |> add_history(:create, resource, id, now)

        state = %{state | data: data}
        persist(data)
        notify_threshold(state, previous_session_count)

        {:reply, {:ok, record}, state}

      :error ->
        {:reply, {:error, :invalid_attributes}, state}
    end
  end

  def handle_call({:get, resource, id}, _from, state)
      when resource in [:user, :session] do
    reply =
      case Map.fetch(Map.fetch!(state.data, resource), id) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, resource, id, attrs}, _from, state)
      when resource in [:user, :session] do
    records = Map.fetch!(state.data, resource)

    case {Map.fetch(records, id), normalize_attrs(attrs)} do
      {{:ok, record}, {:ok, attrs}} ->
        now = now()

        updated_record =
          record
          |> Map.merge(attrs)
          |> Map.put(:id, id)
          |> Map.put(:inserted_at, record.inserted_at)
          |> Map.put(:updated_at, now)

        data =
          state.data
          |> Map.put(resource, Map.put(records, id, updated_record))
          |> add_history(:update, resource, id, now)

        persist(data)
        {:reply, {:ok, updated_record}, %{state | data: data}}

      {:error, _} ->
        {:reply, {:error, :not_found}, state}

      {_, :error} ->
        {:reply, {:error, :invalid_attributes}, state}
    end
  end

  def handle_call({:delete, resource, id}, _from, state)
      when resource in [:user, :session] do
    records = Map.fetch!(state.data, resource)

    case Map.fetch(records, id) do
      {:ok, record} ->
        now = now()
        data =
          state.data
          |> Map.put(resource, Map.delete(records, id))
          |> add_history(:delete, resource, id, now)

        persist(data)
        {:reply, {:ok, record}, %{state | data: data}}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:list, resource}, _from, state)
      when resource in [:user, :session] do
    records =
      state.data
      |> Map.fetch!(resource)
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, records, state}
  end

  def handle_call({:history, limit}, _from, state) do
    entries =
      case limit do
        :all -> state.data.history
        n when is_integer(n) and n > 0 -> Enum.take(state.data.history, n)
        _ -> []
      end

    {:reply, entries, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    case Map.fetch(state.subscribers, pid) do
      {:ok, _monitor} ->
        {:reply, :ok, state}

      :error ->
        monitor = Process.monitor(pid)
        {:reply, :ok, put_in(state.subscribers[pid], monitor)}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {monitor, subscribers} ->
        Process.demonitor(monitor, [:flush])
        {:reply, :ok, %{state | subscribers: subscribers}}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    subscribers =
      case Map.get(state.subscribers, pid) do
        ^monitor -> Map.delete(state.subscribers, pid)
        _ -> state.subscribers
      end

    {:noreply, %{state | subscribers: subscribers}}
  end

  defp initial_data do
    %{
      users: %{},
      sessions: %{},
      next_ids: %{user: 1, session: 1},
      history: []
    }
  end

  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}

  defp normalize_attrs(attrs) when is_list(attrs) do
    try do
      {:ok, Map.new(attrs)}
    rescue
      _ -> :error
    end
  end

  defp normalize_attrs(_attrs), do: :error

  defp add_history(data, action, resource, id, timestamp) do
    entry = %{action: action, resource: resource, id: id, timestamp: timestamp}
    %{data | history: [entry | data.history]}
  end

  defp notify_threshold(state, previous_count) do
    current_count = map_size(state.data.sessions)
    threshold = state.session_threshold

    if previous_count < threshold and current_count >= threshold do
      notification =
        {:notification, :session_threshold_reached,
         %{count: current_count, threshold: threshold}}

      Enum.each(state.subscribers, fn {pid, _monitor} ->
        send(pid, notification)
      end)
    end

    :ok
  end

  defp persist(data), do: :persistent_term.put(@persist_key, data)
  defp now, do: System.system_time(:millisecond)

  defp valid_threshold(value) when is_integer(value) and value > 0, do: value
  defp valid_threshold(_value), do: 10
end

defmodule UserSessionSupervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Supervisor.init([{UserSessionStore, opts}], strategy: :one_for_one)
  end
end
