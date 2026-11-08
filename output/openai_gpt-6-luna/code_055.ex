# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  use GenServer

  @statuses [
    :created,
    :picked_up,
    :in_transit,
    :out_for_delivery,
    :delivered,
    :failed,
    :cancelled
  ]

  @transitions %{
    created: [:picked_up, :failed, :cancelled],
    picked_up: [:in_transit, :failed],
    in_transit: [:out_for_delivery, :delivered, :failed],
    out_for_delivery: [:delivered, :failed],
    delivered: [],
    failed: [],
    cancelled: []
  }

  defstruct [
    :table,
    :max_concurrency,
    data: %{deliveries: %{}, history: [], sequence: 0},
    active: %{},
    queue: :queue.new()
  ]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def create_delivery(server \\ __MODULE__, attrs) do
    submit(server, {:create_delivery, attrs})
  end

  def update_status(server \\ __MODULE__, id, status, details \\ %{}) do
    submit(server, {:update_status, id, status, details})
  end

  def get_delivery(server \\ __MODULE__, id) do
    submit(server, {:get_delivery, id})
  end

  def list_deliveries(server \\ __MODULE__) do
    submit(server, :list_deliveries)
  end

  def history(server \\ __MODULE__, opts \\ []) do
    submit(server, {:history, opts})
  end

  defp submit(server, operation) do
    GenServer.call(server, {:submit, operation}, :infinity)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    table = Keyword.get(opts, :table, :delivery_tracker_dets)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      file =
        Keyword.get_lazy(opts, :file, fn ->
          Path.join(System.tmp_dir!(), "#{table}.dets")
        end)
        |> to_string()
        |> String.to_charlist()

      case :dets.open_file(table,
             file: file,
             type: :set,
             repair: true
           ) do
        {:ok, ^table} ->
          data =
            case :dets.lookup(table, :snapshot) do
              [{:snapshot, saved_data}] -> saved_data
              [] -> %{deliveries: %{}, history: [], sequence: 0}
            end

          {:ok,
           %__MODULE__{
             table: table,
             max_concurrency: max_concurrency,
             data: data
           }}

        {:error, reason} ->
          {:stop, {:dets_open_failed, reason}}
      end
    end
  end

  @impl true
  def handle_call({:submit, operation}, from, state) do
    state = %{state | queue: :queue.in({from, operation}, state.queue)}
    {:noreply, dispatch(state)}
  end

  def handle_call({:execute, operation}, _from, state) do
    {result, next_data, operation_name, details} = perform(operation, state.data)
    sequence = next_data.sequence + 1

    entry = %{
      sequence: sequence,
      timestamp: DateTime.utc_now(),
      operation: operation_name,
      details: details,
      outcome: outcome(result)
    }

    next_data = %{
      next_data
      | sequence: sequence,
        history: [entry | next_data.history]
    }

    case persist(state.table, next_data) do
      :ok ->
        {:reply, result, %{state | data: next_data}}

      {:error, reason} ->
        {:reply, {:error, {:storage_error, reason}}, state}
    end
  end

  @impl true
  def handle_info({:operation_done, ref, result}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {%{from: from, monitor: monitor}, active} ->
        Process.demonitor(monitor, [:flush])
        GenServer.reply(from, result)
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Enum.find(state.active, fn {_ref, operation} ->
           operation.monitor == monitor
         end) do
      nil ->
        {:noreply, state}

      {ref, %{from: from}} ->
        GenServer.reply(from, {:error, {:operation_worker_failed, reason}})
        {:noreply, dispatch(%{state | active: Map.delete(state.active, ref)})}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end

  defp dispatch(state) do
    if map_size(state.active) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {from, operation}}, queue} ->
          server = self()
          ref = make_ref()

          {pid, monitor} =
            spawn_monitor(fn ->
              result =
                try do
                  GenServer.call(server, {:execute, operation}, :infinity)
                catch
                  kind, reason -> {:error, {:operation_execution_failed, kind, reason}}
                end

              send(server, {:operation_done, ref, result})
            end)

          active =
            Map.put(state.active, ref, %{
              pid: pid,
              monitor: monitor,
              from: from
            })

          dispatch(%{state | queue: queue, active: active})
      end
    end
  end

  defp perform({:create_delivery, attrs}, data) when is_map(attrs) do
    id =
      Map.get(attrs, :id) ||
        Integer.to_string(System.unique_integer([:positive, :monotonic]))

    details = %{id: id, attributes: attrs}

    cond do
      Map.has_key?(data.deliveries, id) ->
        {{:error, :already_exists}, data, :create_delivery, details}

      true ->
        now = DateTime.utc_now()

        delivery =
          attrs
          |> Map.put(:id, id)
          |> Map.put(:status, :created)
          |> Map.put(:created_at, now)
          |> Map.put(:updated_at, now)
          |> Map.put(:tracking, [%{status: :created, timestamp: now}])

        next_data = %{
          data
          | deliveries: Map.put(data.deliveries, id, delivery)
        }

        {{:ok, delivery}, next_data, :create_delivery, details}
    end
  end

  defp perform({:create_delivery, _attrs}, data) do
    {{:error, :invalid_attributes}, data, :create_delivery, %{}}
  end

  defp perform({:update_status, id, status, details}, data)
       when status in @statuses and is_map(details) do
    case Map.fetch(data.deliveries, id) do
      :error ->
        {{:error, :not_found}, data, :update_status, %{id: id, status: status}}

      {:ok, delivery} ->
        current_status = delivery.status

        if status == current_status or status in Map.fetch!(@transitions, current_status) do
          now = DateTime.utc_now()

          event =
            details
            |> Map.put(:status, status)
            |> Map.put(:timestamp, now)

          updated =
            delivery
            |> Map.merge(details)
            |> Map.put(:status, status)
            |> Map.put(:updated_at, now)
            |> Map.update!(:tracking, &[event | &1])

          next_data = %{
            data
            | deliveries: Map.put(data.deliveries, id, updated)
          }

          {{:ok, updated}, next_data, :update_status,
           %{id: id, from: current_status, to: status, details: details}}
        else
          {{:error, {:invalid_transition, current_status, status}}, data,
           :update_status, %{id: id, from: current_status, to: status}}
        end
    end
  end

  defp perform({:update_status, id, status, _details}, data) do
    {{:error, :invalid_status_or_details}, data, :update_status,
     %{id: id, status: status}}
  end

  defp perform({:get_delivery, id}, data) do
    result =
      case Map.fetch(data.deliveries, id) do
        {:ok, delivery} -> {:ok, delivery}
        :error -> {:error, :not_found}
      end

    {result, data, :get_delivery, %{id: id}}
  end

  defp perform(:list_deliveries, data) do
    deliveries =
      data.deliveries
      |> Map.values()
      |> Enum.sort_by(& &1.created_at, DateTime)

    {{:ok, deliveries}, data, :list_deliveries, %{}}
  end

  defp perform({:history, opts}, data) do
    entries = Enum.reverse(data.history)

    result =
      case Keyword.get(opts, :limit) do
        limit when is_integer(limit) and limit >= 0 -> Enum.take(entries, -limit)
        nil -> entries
        _ -> entries
      end

    {{:ok, result}, data, :history, %{options: opts}}
  end

  defp perform(_operation, data) do
    {{:error, :unknown_operation}, data, :unknown_operation, %{}}
  end

  defp outcome({:ok, _value}), do: :ok
  defp outcome({:error, reason}), do: {:error, reason}
  defp outcome(_result), do: :ok

  defp persist(table, data) do
    case :dets.insert(table, {:snapshot, data}) do
      :ok -> :dets.sync(table)
      error -> error
    end
  end
end
