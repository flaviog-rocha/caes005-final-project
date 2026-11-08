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

defmodule NotificationSystem do
  @moduledoc """
  Gerenciador de notificações com limite de concorrência e persistência em arquivo.
  """

  @manager NotificationSystem.Manager
  @worker_supervisor NotificationSystem.WorkerSupervisor

  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      raise ArgumentError, ":max_concurrency deve ser um inteiro positivo"
    end

    children = [
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: @worker_supervisor},
      {NotificationSystem.Manager, opts}
    ]

    Supervisor.start_link(children,
      strategy: :one_for_all,
      name: NotificationSystem.Supervisor
    )
  end

  def submit(payload), do: GenServer.call(@manager, {:submit, payload})
  def get(id), do: GenServer.call(@manager, {:get, id})
  def all(), do: GenServer.call(@manager, :all)
end

defmodule NotificationSystem.Manager do
  use GenServer

  @manager NotificationSystem.Manager
  @worker_supervisor NotificationSystem.WorkerSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @manager)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "notifications.term")
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    handler = Keyword.get(opts, :handler, fn _id, _payload -> :ok end)

    with true <- is_integer(max_concurrency) and max_concurrency > 0,
         {:ok, persisted} <- load(path) do
      records =
        Map.new(persisted.records, fn {id, record} ->
          status = if record.status == :processing, do: :pending, else: record.status
          {id, %{record | status: status}}
        end)

      highest_id = records |> Map.keys() |> Enum.max(fn -> 0 end)
      next_id = max(persisted.next_id, highest_id + 1)

      state = %{
        path: path,
        max_concurrency: max_concurrency,
        handler: handler,
        records: records,
        next_id: next_id,
        active: %{},
        refs: %{}
      }

      persist!(state)
      {:ok, state, {:continue, :dispatch}}
    else
      false -> {:stop, {:error, :invalid_max_concurrency}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:dispatch, state), do: {:noreply, dispatch(state)}

  @impl true
  def handle_call({:submit, payload}, _from, state) do
    id = state.next_id
    record = %{id: id, payload: payload, status: :pending, result: nil}

    new_state = %{
      state
      | next_id: id + 1,
        records: Map.put(state.records, id, record)
    }

    persist!(new_state)
    send(self(), :dispatch)
    {:reply, {:ok, id}, new_state}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.records, id), state}
  end

  def handle_call(:all, _from, state) do
    records =
      state.records
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, records, state}
  end

  @impl true
  def handle_info(:dispatch, state), do: {:noreply, dispatch(state)}

  def handle_info({:notification_result, id, result}, state) do
    case Map.pop(state.active, id) do
      {nil, _active} ->
        {:noreply, state}

      {%{ref: ref}, active} ->
        Process.demonitor(ref, [:flush])

        record = Map.fetch!(state.records, id)
        updated_record = %{record | status: :completed, result: result}

        new_state = %{
          state
          | active: active,
            refs: Map.delete(state.refs, ref),
            records: Map.put(state.records, id, updated_record)
        }

        persist!(new_state)
        send(self(), :dispatch)
        {:noreply, new_state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {id, refs} ->
        active = Map.delete(state.active, id)
        record = Map.fetch!(state.records, id)
        updated_record = %{record | status: :pending}

        new_state = %{
          state
          | active: active,
            refs: refs,
            records: Map.put(state.records, id, updated_record)
        }

        persist!(new_state)
        send(self(), :dispatch)
        {:noreply, new_state}
    end
  end

  defp dispatch(state) do
    available = state.max_concurrency - map_size(state.active)

    pending_ids =
      state.records
      |> Enum.filter(fn {_id, record} -> record.status == :pending end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    pending_ids
    |> Enum.take(max(available, 0))
    |> Enum.reduce_while(state, fn id, current_state ->
      case start_notification(id, current_state) do
        {:ok, next_state} ->
          {:cont, next_state}

        {:error, next_state} ->
          Process.send_after(self(), :dispatch, 1_000)
          {:halt, next_state}
      end
    end)
  end

  defp start_notification(id, state) do
    record = Map.fetch!(state.records, id)
    processing_record = %{record | status: :processing}
    processing_state = %{
      state
      | records: Map.put(state.records, id, processing_record)
    }

    # Persistir antes de iniciar o worker permite recuperar a operação após uma falha.
    persist!(processing_state)

    args = %{
      manager: self(),
      id: id,
      payload: record.payload,
      handler: state.handler
    }

    child_spec = %{
      id: {NotificationSystem.Worker, make_ref()},
      start: {NotificationSystem.Worker, :start_link, [args]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }

    case DynamicSupervisor.start_child(@worker_supervisor, child_spec) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        active = Map.put(state.active, id, %{pid: pid, ref: ref})

        {:ok,
         %{
           processing_state
           | active: active,
             refs: Map.put(state.refs, ref, id)
         }}

      {:error, _reason} ->
        pending_record = %{record | status: :pending}

        pending_state = %{
          state
          | records: Map.put(state.records, id, pending_record)
        }

        persist!(pending_state)
        {:error, pending_state}
    end
  end

  defp load(path) do
    case File.read(path) do
      {:ok, binary} ->
        case :erlang.binary_to_term(binary) do
          %{version: 1, next_id: next_id, records: records}
          when is_integer(next_id) and is_map(records) ->
            {:ok, %{next_id: next_id, records: records}}

          _ ->
            {:error, :invalid_persistence_file}
        end

      {:error, :enoent} ->
        {:ok, %{next_id: 1, records: %{}}}

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    _ -> {:error, :invalid_persistence_file}
  end

  defp persist!(state) do
    data = %{
      version: 1,
      next_id: state.next_id,
      records: state.records
    }

    path = state.path
    temp_path = path <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temp_path, :erlang.term_to_binary(data), [:binary, :sync]),
         :ok <- File.rename(temp_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temp_path)
        raise "falha ao persistir notificações: #{inspect(reason)}"
    end
  end
end

defmodule NotificationSystem.Worker do
  use GenServer

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args), do: {:ok, args, {:continue, :run}}

  @impl true
  def handle_continue(:run, state) do
    result =
      try do
        {:ok, state.handler.(state.id, state.payload)}
      rescue
        exception ->
          {:error, {:exception, exception.__struct__, Exception.message(exception)}}
      catch
        kind, reason ->
          {:error, {kind, reason}}
      end

    send(state.manager, {:notification_result, state.id, result})
    {:stop, :normal, state}
  end
end
