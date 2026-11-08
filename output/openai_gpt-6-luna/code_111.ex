# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  @moduledoc """
  Gerenciador de notificações com operações limitadas, serializadas e persistidas em DETS.
  """

  @supervisor_name NotificationManager.Supervisor
  @dynamic_supervisor_name NotificationManager.OperationSupervisor

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: @supervisor_name)
  end

  @impl Supervisor
  def init(opts) do
    children = [
      %{
        id: NotificationManager.OperationSupervisor,
        start:
          {DynamicSupervisor, :start_link,
           [[strategy: :one_for_one, name: @dynamic_supervisor_name]]}
      },
      %{
        id: NotificationManager.Coordinator,
        start: {NotificationManager.Coordinator, :start_link, [opts]},
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(id, attrs \\ %{}), do: submit({:create, id, attrs})
  def update(id, attrs), do: submit({:update, id, attrs})
  def delete(id), do: submit({:delete, id})
  def get(id), do: submit({:get, id})
  def list(), do: submit(:list)

  defp submit(operation) do
    GenServer.call(NotificationManager.Coordinator, {:submit, operation}, :infinity)
  end
end

defmodule NotificationManager.Coordinator do
  use GenServer

  @table :notification_manager_store
  @dynamic_supervisor_name NotificationManager.OperationSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    path = Keyword.get(opts, :path, "notifications.dets")

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      case open_store(path) do
        {:ok, notifications} ->
          {:ok,
           %{
             notifications: notifications,
             max_concurrency: max_concurrency,
             active: %{},
             queue: :queue.new()
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    end
  end

  @impl GenServer
  def handle_call({:submit, operation}, from, state) do
    item = {make_ref(), operation, from}
    state = %{state | queue: :queue.in(item, state.queue)}
    {:noreply, dispatch(state)}
  end

  def handle_call({:execute, ref, operation}, _worker_from, state) do
    case Enum.find(state.active, fn {_monitor, info} -> info.ref == ref end) do
      nil ->
        {:reply, {:error, :unknown_operation}, state}

      {_monitor, %{replied?: true}} ->
        {:reply, {:error, :operation_already_completed}, state}

      {monitor, info} ->
        {result, notifications} = execute_operation(operation, state.notifications)

        GenServer.reply(info.from, result)

        active =
          Map.put(state.active, monitor, %{
            info
            | replied?: true,
              notifications: notifications
          })

        {:reply, result, %{state | active: active, notifications: notifications}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, state) do
    case Map.pop(state.active, monitor) do
      {nil, _active} ->
        {:noreply, state}

      {info, active} ->
        state = %{state | active: active}

        state =
          if info.replied? do
            state
          else
            %{state | queue: :queue.in_r({info.ref, info.operation, info.from}, state.queue)}
          end

        {:noreply, dispatch(state)}
    end
  end

  @impl GenServer
  def terminate(_reason, _state) do
    try do
      :dets.close(@table)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  defp dispatch(state) do
    if map_size(state.active) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {ref, operation, from}}, queue} ->
          state = %{state | queue: queue}

          case start_worker(state, ref, operation, from) do
            {:ok, state} ->
              dispatch(state)

            {:error, reason, state} ->
              GenServer.reply(from, {:error, {:worker_start_failed, reason}})
              dispatch(state)
          end
      end
    end
  end

  defp start_worker(state, ref, operation, from) do
    case DynamicSupervisor.start_child(
           @dynamic_supervisor_name,
           {NotificationManager.Worker, {self(), ref, operation}}
         ) do
      {:ok, pid} ->
        monitor = Process.monitor(pid)

        info = %{
          pid: pid,
          ref: ref,
          operation: operation,
          from: from,
          replied?: false
        }

        {:ok, %{state | active: Map.put(state.active, monitor, info)}}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp execute_operation({:create, id, attrs}, notifications) do
    cond do
      is_nil(id) ->
        {{:error, :invalid_id}, notifications}

      not is_map(attrs) ->
        {{:error, :invalid_attributes}, notifications}

      Map.has_key?(notifications, id) ->
        {{:error, :already_exists}, notifications}

      true ->
        notification = Map.put(attrs, :id, id)
        commit(notifications, Map.put(notifications, id, notification), {:ok, notification})
    end
  end

  defp execute_operation({:update, id, attrs}, notifications) do
    cond do
      not is_map(attrs) ->
        {{:error, :invalid_attributes}, notifications}

      not Map.has_key?(notifications, id) ->
        {{:error, :not_found}, notifications}

      true ->
        notification =
          notifications
          |> Map.fetch!(id)
          |> Map.merge(attrs)
          |> Map.put(:id, id)

        commit(notifications, Map.put(notifications, id, notification), {:ok, notification})
    end
  end

  defp execute_operation({:delete, id}, notifications) do
    if Map.has_key?(notifications, id) do
      notification = Map.fetch!(notifications, id)
      commit(notifications, Map.delete(notifications, id), {:ok, notification})
    else
      {{:error, :not_found}, notifications}
    end
  end

  defp execute_operation({:get, id}, notifications) do
    case Map.fetch(notifications, id) do
      {:ok, notification} -> {{:ok, notification}, notifications}
      :error -> {{:error, :not_found}, notifications}
    end
  end

  defp execute_operation(:list, notifications) do
    {{:ok, Map.values(notifications)}, notifications}
  end

  defp execute_operation(_operation, notifications) do
    {{:error, :invalid_operation}, notifications}
  end

  defp commit(old_notifications, new_notifications, result) do
    case :dets.insert(@table, {:notifications, new_notifications}) do
      :ok ->
        case :dets.sync(@table) do
          :ok ->
            {result, new_notifications}

          {:error, reason} ->
            rollback(old_notifications)
            {{:error, {:persistence_failed, reason}}, old_notifications}
        end

      {:error, reason} ->
        {{:error, {:persistence_failed, reason}}, old_notifications}
    end
  end

  defp rollback(notifications) do
    try do
      :dets.insert(@table, {:notifications, notifications})
      :dets.sync(@table)
    catch
      _, _ -> :ok
    end
  end

  defp open_store(path) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, @table} <-
           :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      case :dets.lookup(@table, :notifications) do
        [] ->
          case :dets.insert(@table, {:notifications, %{}}) do
            :ok ->
              case :dets.sync(@table) do
                :ok -> {:ok, %{}}
                {:error, reason} -> {:error, {:persistence_failed, reason}}
              end

            {:error, reason} ->
              {:error, {:persistence_failed, reason}}
          end

        [{:notifications, notifications}] when is_map(notifications) ->
          {:ok, notifications}

        _ ->
          {:error, :invalid_persisted_data}
      end
    else
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule NotificationManager.Worker do
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl GenServer
  def init({coordinator, ref, operation}) do
    {:ok, {coordinator, ref, operation}, {:continue, :execute}}
  end

  @impl GenServer
  def handle_continue(:execute, {coordinator, ref, operation}) do
    GenServer.call(coordinator, {:execute, ref, operation}, :infinity)
    {:stop, :normal, nil}
  end
end
