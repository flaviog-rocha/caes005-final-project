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

defmodule DeliveryTracker.Delivery do
  @moduledoc false

  defstruct [
    :id,
    :status,
    :created_at,
    :updated_at,
    attributes: %{}
  ]
end

defmodule DeliveryTracker.Limiter do
  @moduledoc false
  use GenServer

  def start_link(max_concurrent) do
    GenServer.start_link(__MODULE__, max_concurrent)
  end

  def acquire(pid), do: GenServer.call(pid, :acquire, :infinity)
  def release(pid), do: GenServer.cast(pid, {:release, self()})

  @impl true
  def init(max_concurrent) do
    {:ok, %{max: max_concurrent, active: %{}, queue: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, from, state) do
    if map_size(state.active) < state.max do
      {:reply, :ok, activate(state, from)}
    else
      pid = elem(from, 0)
      monitor = Process.monitor(pid)
      queue = :queue.in({from, pid, monitor}, state.queue)
      {:noreply, %{state | queue: queue}}
    end
  end

  @impl true
  def handle_cast({:release, pid}, state) do
    case Enum.find(state.active, fn {_ref, active_pid} -> active_pid == pid end) do
      {ref, _active_pid} ->
        Process.demonitor(ref, [:flush])
        state = %{state | active: Map.delete(state.active, ref)}
        {:noreply, dispatch(state)}

      nil ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    if Map.has_key?(state.active, ref) do
      {:noreply, dispatch(%{state | active: Map.delete(state.active, ref)})}
    else
      {:noreply, state}
    end
  end

  defp activate(state, from) do
    pid = elem(from, 0)
    ref = Process.monitor(pid)
    %{state | active: Map.put(state.active, ref, pid)}
  end

  defp dispatch(state) do
    if map_size(state.active) >= state.max do
      state
    else
      case :queue.out(state.queue) do
        {{:value, {from, pid, ref}}, queue} ->
          state = %{state | queue: queue}

          if Process.alive?(pid) do
            GenServer.reply(from, :ok)
            %{state | active: Map.put(state.active, ref, pid)} |> dispatch()
          else
            Process.demonitor(ref, [:flush])
            dispatch(state)
          end

        {:empty, _queue} ->
          state
      end
    end
  end
end

defmodule DeliveryTracker do
  @moduledoc """
  Gerencia entregas, registra o histórico das operações e persiste os dados em DETS.

  Cada instância usa a tabela DETS `:delivery_tracker_store`; portanto, apenas uma
  instância pode estar aberta por vez na mesma VM.
  """

  use GenServer

  alias DeliveryTracker.Delivery
  alias DeliveryTracker.Limiter

  @table :delivery_tracker_store
  @statuses [:pending, :picked_up, :in_transit, :out_for_delivery, :delivered, :failed, :returned]

  def start_link(opts \\ []) do
    max_concurrent = Keyword.get(opts, :max_concurrent_operations, 4)

    if not (is_integer(max_concurrent) and max_concurrent > 0) do
      {:error, :invalid_max_concurrent_operations}
    else
      case Limiter.start_link(max_concurrent) do
        {:ok, limiter} ->
          server_opts = Keyword.take(opts, [:name])

          case GenServer.start_link(__MODULE__, {opts, limiter}, server_opts) do
            {:ok, _pid} = result ->
              result

            error ->
              GenServer.stop(limiter)
              error
          end

        error ->
          error
      end
    end
  end

  def create_delivery(server, attributes) when is_map(attributes) do
    with_limit(server, {:create_delivery, attributes})
  end

  def create_delivery(_server, _attributes), do: {:error, :invalid_attributes}

  def update_status(server, id, status) do
    with_limit(server, {:update_status, id, status})
  end

  def get_delivery(server, id) do
    with_limit(server, {:get_delivery, id})
  end

  def list_deliveries(server) do
    with_limit(server, :list_deliveries)
  end

  def history(server, limit \\ :all) do
    with_limit(server, {:history, limit})
  end

  defp with_limit(server, request) do
    limiter = GenServer.call(server, :limiter, :infinity)
    :ok = Limiter.acquire(limiter)

    try do
      GenServer.call(server, request, :infinity)
    after
      Limiter.release(limiter)
    end
  end

  @impl true
  def init({opts, limiter}) do
    Process.link(limiter)
    file = opts |> Keyword.get(:file, "delivery_tracker.dets") |> to_charlist()

    case :dets.open_file(@table, [{:file, file}, {:type, :set}]) do
      {:ok, table} ->
        snapshot =
          case :dets.lookup(table, :snapshot) do
            [{:snapshot, saved}] when is_map(saved) -> saved
            _ -> %{deliveries: %{}, history: [], next_sequence: 1}
          end

        {:ok, Map.merge(snapshot, %{table: table, limiter: limiter})}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(:limiter, _from, state), do: {:reply, state.limiter, state}

  def handle_call({:create_delivery, attributes}, _from, state) do
    id = Map.get(attributes, :id) || generated_id()

    cond do
      Map.has_key?(state.deliveries, id) ->
        {:reply, {:error, :already_exists}, state}

      true ->
        case normalize_status(Map.get(attributes, :status, :pending)) do
          {:ok, status} ->
            now = DateTime.utc_now()

            delivery = %Delivery{
              id: id,
              status: status,
              created_at: now,
              updated_at: now,
              attributes: Map.delete(attributes, :status) |> Map.delete(:id)
            }

            event = new_event(state, :created, id, %{status: status})
            persist_result(state, Map.put(state.deliveries, id, delivery), [event | state.history], fn new_state ->
              {:ok, delivery, new_state}
            end)

          :error ->
            {:reply, {:error, :invalid_status}, state}
        end
    end
  end

  def handle_call({:update_status, id, requested_status}, _from, state) do
    case {Map.fetch(state.deliveries, id), normalize_status(requested_status)} do
      {:error, _} ->
        {:reply, {:error, :not_found}, state}

      {_, :error} ->
        {:reply, {:error, :invalid_status}, state}

      {{:ok, delivery}, {:ok, status}} ->
        updated = %{delivery | status: status, updated_at: DateTime.utc_now()}
        event = new_event(state, :status_updated, id, %{from: delivery.status, to: status})

        persist_result(
          state,
          Map.put(state.deliveries, id, updated),
          [event | state.history],
          fn new_state -> {:ok, updated, new_state} end
        )
    end
  end

  def handle_call({:get_delivery, id}, _from, state) do
    reply =
      case Map.fetch(state.deliveries, id) do
        {:ok, delivery} -> {:ok, delivery}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_deliveries, _from, state) do
    {:reply, Map.values(state.deliveries), state}
  end

  def handle_call({:history, :all}, _from, state), do: {:reply, state.history, state}

  def handle_call({:history, limit}, _from, state)
      when is_integer(limit) and limit >= 0 do
    {:reply, Enum.take(state.history, limit), state}
  end

  def handle_call({:history, _limit}, _from, state) do
    {:reply, {:error, :invalid_limit}, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)

    if Process.alive?(state.limiter) do
      GenServer.stop(state.limiter, :normal)
    end

    :ok
  end

  defp persist_result(state, deliveries, history, result_fun) do
    next_sequence = state.next_sequence + 1

    snapshot = %{
      deliveries: deliveries,
      history: history,
      next_sequence: next_sequence
    }

    case :dets.insert(state.table, {:snapshot, snapshot}) do
      :ok ->
        case :dets.sync(state.table) do
          :ok ->
            new_state = Map.merge(state, snapshot)
            {:reply, result, new_state} = result_fun.(new_state)
            {:reply, result, new_state}

          {:error, reason} ->
            rollback_snapshot(state)
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  defp rollback_snapshot(state) do
    previous = %{
      deliveries: state.deliveries,
      history: state.history,
      next_sequence: state.next_sequence
    }

    :dets.insert(state.table, {:snapshot, previous})
    :dets.sync(state.table)
  end

  defp new_event(state, operation, delivery_id, changes) do
    %{
      sequence: state.next_sequence,
      operation: operation,
      delivery_id: delivery_id,
      changes: changes,
      timestamp: DateTime.utc_now()
    }
  end

  defp normalize_status(status) when status in @statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status) do
    case Enum.find(@statuses, &(Atom.to_string(&1) == status)) do
      nil -> :error
      normalized -> {:ok, normalized}
    end
  end

  defp normalize_status(_status), do: :error

  defp generated_id do
    System.unique_integer([:positive, :monotonic])
  end
end
