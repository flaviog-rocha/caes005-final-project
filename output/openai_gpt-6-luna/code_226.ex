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

defmodule UserSessionStore do
  @moduledoc """
  Armazenamento concorrente de usuários e sessões.

  Cada registro é processado por um processo independente. As atualizações são
  aplicadas atomicamente por registro. Inscreva-se com `subscribe/1` para receber
  notificações de operações que satisfaçam o predicado informado.
  """

  use Supervisor

  @registry UserSessionStore.Registry
  @worker_supervisor UserSessionStore.WorkerSupervisor
  @valid_types [:user, :session]

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {Registry, keys: :duplicate, name: UserSessionStore.Notifications},
      {DynamicSupervisor, strategy: :one_for_one, name: @worker_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Cria um usuário ou uma sessão."
  def create(type, id, attributes) when is_map(attributes) do
    with :ok <- validate_type(type),
         {:ok, pid} <- worker(type, id) do
      GenServer.call(pid, {:create, attributes})
    end
  end

  @doc "Consulta um usuário ou uma sessão."
  def read(type, id) do
    with :ok <- validate_type(type),
         {:ok, pid} <- worker(type, id) do
      GenServer.call(pid, :read)
    end
  end

  @doc """
  Atualiza um registro atomicamente.

  A função recebe o registro atual e deve retornar um mapa ou `{:ok, mapa}`.
  Qualquer outro retorno ou exceção cancela a atualização.
  """
  def update(type, id, updater) when is_function(updater, 1) do
    with :ok <- validate_type(type),
         {:ok, pid} <- worker(type, id) do
      GenServer.call(pid, {:update, updater})
    end
  end

  @doc "Remove um usuário ou uma sessão."
  def delete(type, id) do
    with :ok <- validate_type(type),
         {:ok, pid} <- worker(type, id) do
      GenServer.call(pid, :delete)
    end
  end

  @doc """
  Inscreve o processo chamador em notificações.

  O predicado recebe um evento e deve retornar `true` para receber a notificação.
  O evento tem o formato `%{type: ..., id: ..., action: ..., record: ...}`.
  Sem predicado, o processo recebe todos os eventos.
  """
  def subscribe(predicate \\ fn _event -> true end) when is_function(predicate, 1) do
    Registry.register(UserSessionStore.Notifications, :events, predicate)
  end

  @doc "Cancela a inscrição do processo chamador."
  def unsubscribe do
    Registry.unregister(UserSessionStore.Notifications, :events)
  end

  defp validate_type(type) when type in @valid_types, do: :ok
  defp validate_type(_type), do: {:error, :invalid_type}

  defp worker(type, id) do
    key = {type, id}

    case Registry.lookup(@registry, key) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               @worker_supervisor,
               {UserSessionStore.Worker, key}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, reason} ->
            case Registry.lookup(@registry, key) do
              [{pid, _}] -> {:ok, pid}
              [] -> {:error, reason}
            end
        end
    end
  end
end

defmodule UserSessionStore.Worker do
  use GenServer

  @registry UserSessionStore.Registry
  @notifications UserSessionStore.Notifications

  def start_link({type, id} = key) do
    GenServer.start_link(__MODULE__, key, name: {:via, Registry, {@registry, {type, id}}})
  end

  @impl true
  def init({type, id}) do
    {:ok, %{type: type, id: id, record: nil}}
  end

  @impl true
  def handle_call({:create, attributes}, _from, %{record: nil} = state) do
    record =
      attributes
      |> Map.put(:id, state.id)
      |> Map.put(:type, state.type)

    notify(state, :created, record)
    {:reply, {:ok, record}, %{state | record: record}}
  end

  def handle_call({:create, _attributes}, _from, state) do
    {:reply, {:error, :already_exists}, state}
  end

  def handle_call(:read, _from, %{record: nil} = state) do
    {:reply, {:error, :not_found}, state}
  end

  def handle_call(:read, _from, state) do
    {:reply, {:ok, state.record}, state}
  end

  def handle_call({:update, _updater}, _from, %{record: nil} = state) do
    {:reply, {:error, :not_found}, state}
  end

  def handle_call({:update, updater}, _from, state) do
    result =
      try do
        {:ok, updater.(state.record)}
      rescue
        error -> {:error, {:update_failed, error}}
      catch
        kind, reason -> {:error, {:update_failed, {kind, reason}}}
      end

    case result do
      {:ok, {:ok, record}} when is_map(record) ->
        commit_update(state, record)

      {:ok, record} when is_map(record) ->
        commit_update(state, record)

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {:ok, _invalid_result} ->
        {:reply, {:error, :invalid_update_result}, state}
    end
  end

  def handle_call(:delete, _from, %{record: nil} = state) do
    {:reply, {:error, :not_found}, state}
  end

  def handle_call(:delete, _from, state) do
    notify(state, :deleted, state.record)
    {:reply, {:ok, state.record}, %{state | record: nil}}
  end

  defp commit_update(state, record) do
    record =
      record
      |> Map.put(:id, state.id)
      |> Map.put(:type, state.type)

    notify(state, :updated, record)
    {:reply, {:ok, record}, %{state | record: record}}
  end

  defp notify(state, action, record) do
    event = %{
      type: state.type,
      id: state.id,
      action: action,
      record: record
    }

    Registry.dispatch(@notifications, :events, fn entries ->
      Enum.each(entries, fn {pid, predicate} ->
        try do
          if predicate.(event), do: send(pid, {:user_session_event, event})
        rescue
          _error -> :ok
        catch
          _kind, _reason -> :ok
        end
      end)
    end)
  end
end
