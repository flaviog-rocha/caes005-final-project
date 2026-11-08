# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Detectar encerramentos inesperados de processos.
# 3. Implementar expiração automática de registros.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  use GenServer

  @sweep_interval_ms 1_000

  # Inicia o gerenciador. As opções :ttl_ms e :history_ttl_ms definem os prazos
  # padrão de expiração; :max_retries, :base_backoff_ms e :max_backoff_ms
  # controlam as tentativas após falhas.
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def enqueue(server, id, payload, worker_fun \\ fn _delivery -> :ok end, opts \\ [])
      when is_function(worker_fun, 1) do
    GenServer.call(server, {:enqueue, id, payload, worker_fun, opts})
  end

  def track(server, id, event), do: GenServer.call(server, {:track, id, event})
  def get(server, id), do: GenServer.call(server, {:get, id})
  def list(server), do: GenServer.call(server, :list)
  def history(server), do: GenServer.call(server, :history)

  @impl true
  def init(opts) do
    schedule_sweep()

    {:ok,
     %{
       deliveries: %{},
       workers: %{},
       history: [],
       default_ttl_ms: positive_integer(Keyword.get(opts, :ttl_ms), 86_400_000),
       history_ttl_ms:
         positive_integer(Keyword.get(opts, :history_ttl_ms), 604_800_000),
       max_retries: non_negative_integer(Keyword.get(opts, :max_retries), 3),
       base_backoff_ms: positive_integer(Keyword.get(opts, :base_backoff_ms), 500),
       max_backoff_ms: positive_integer(Keyword.get(opts, :max_backoff_ms), 30_000)
     }}
  end

  @impl true
  def handle_call({:enqueue, id, payload, worker_fun, opts}, _from, state) do
    if Map.has_key?(state.deliveries, id) do
      {:reply, {:error, :already_exists}, state}
    else
      now = now_ms()

      record = %{
        id: id,
        payload: payload,
        status: :queued,
        attempts: 0,
        max_retries:
          non_negative_integer(Keyword.get(opts, :max_retries), state.max_retries),
        base_backoff_ms:
          positive_integer(Keyword.get(opts, :base_backoff_ms), state.base_backoff_ms),
        max_backoff_ms:
          positive_integer(Keyword.get(opts, :max_backoff_ms), state.max_backoff_ms),
        worker_fun: worker_fun,
        worker_ref: nil,
        token: nil,
        retry_token: nil,
        tracking: [],
        created_at: now,
        updated_at: now,
        expires_at:
          now + positive_integer(Keyword.get(opts, :ttl_ms), state.default_ttl_ms)
      }

      state =
        state
        |> put_record(record)
        |> add_history(id, :delivery_created, %{status: :queued})
        |> launch_worker(record)

      {:reply, {:ok, public_record(Map.fetch!(state.deliveries, id))}, state}
    end
  end

  def handle_call({:track, id, event}, _from, state) do
    case Map.get(state.deliveries, id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      record ->
        record =
          record
          |> Map.put(:tracking, [event | record.tracking])
          |> Map.put(:updated_at, now_ms())

        state =
          state
          |> put_record(record)
          |> add_history(id, :tracking_updated, %{event: event})

        {:reply, {:ok, public_record(record)}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.get(state.deliveries, id) do
        nil -> {:error, :not_found}
        record -> {:ok, public_record(record)}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    records =
      state.deliveries
      |> Map.values()
      |> Enum.map(&public_record/1)

    {:reply, records, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info({:worker_result, id, token, result}, state) do
    case Map.get(state.deliveries, id) do
      %{token: ^token, worker_ref: ref} = record ->
        state = clear_worker(state, ref, false)
        record = %{record | worker_ref: nil, token: nil}

        state =
          case result do
            :ok ->
              complete_delivery(state, record, :delivered, :delivered)

            {:ok, tracking_event} ->
              record = %{
                record
                | status: :delivered,
                  tracking: [tracking_event | record.tracking],
                  updated_at: now_ms()
              }

              state
              |> put_record(record)
              |> add_history(id, :delivery_delivered, %{tracking: tracking_event})

            {:error, reason} ->
              fail_or_retry(state, record, reason)

            other ->
              fail_or_retry(state, record, {:unexpected_result, other})
          end

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.workers, ref) do
      {nil, _workers} ->
        {:noreply, state}

      {{id, token, _pid}, workers} ->
        state = %{state | workers: workers}

        case Map.get(state.deliveries, id) do
          %{token: ^token, worker_ref: ^ref} = record ->
            record = %{record | worker_ref: nil, token: nil}
            state = add_history(state, id, :worker_stopped_unexpectedly, %{reason: reason})
            {:noreply, fail_or_retry(state, record, {:worker_stopped, reason})}

          _ ->
            {:noreply, state}
        end
    end
  end

  def handle_info({:retry, id, retry_token}, state) do
    case Map.get(state.deliveries, id) do
      %{status: :retry_wait, retry_token: ^retry_token} = record ->
        record =
          record
          |> Map.put(:retry_token, nil)
          |> Map.put(:status, :queued)
          |> Map.put(:updated_at, now_ms())

        state =
          state
          |> put_record(record)
          |> add_history(id, :retry_started, %{attempt: record.attempts + 1})
          |> launch_worker(record)

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(:sweep, state) do
    now = now_ms()

    state =
      Enum.reduce(state.deliveries, state, fn {id, record}, acc ->
        if record.expires_at <= now do
          expire_delivery(acc, id, record)
        else
          acc
        end
      end)

    state = %{
      state
      | history:
          Enum.filter(state.history, fn entry ->
            now - entry.timestamp < state.history_ttl_ms
          end)
    }

    schedule_sweep()
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.workers, fn {_ref, {_id, _token, pid}} ->
      Process.exit(pid, :kill)
    end)

    :ok
  end

  defp launch_worker(state, record) do
    parent = self()
    token = make_ref()
    attempt = record.attempts + 1

    {pid, ref} =
      spawn_monitor(fn ->
        result =
          try do
            record.worker_fun.(%{
              id: record.id,
              payload: record.payload,
              attempt: attempt
            })
          rescue
            error -> {:error, {:transient, {:exception, error}}}
          catch
            kind, reason -> {:error, {:transient, {kind, reason}}}
          end

        send(parent, {:worker_result, record.id, token, result})
      end)

    record = %{
      record
      | status: :processing,
        attempts: attempt,
        worker_ref: ref,
        token: token,
        retry_token: nil,
        updated_at: now_ms()
    }

    state
    |> put_record(record)
    |> Map.update!(:workers, &Map.put(&1, ref, {record.id, token, pid}))
    |> add_history(record.id, :delivery_attempt_started, %{attempt: attempt})
  end

  defp complete_delivery(state, record, status, event) do
    record = %{record | status: status, updated_at: now_ms()}

    state
    |> put_record(record)
    |> add_history(record.id, :delivery_delivered, %{event: event})
  end

  defp fail_or_retry(state, record, reason) do
    record = Map.put(record, :updated_at, now_ms())
    permanent? = match?({:permanent, _}, reason) or reason == :permanent

    if not permanent? and record.attempts <= record.max_retries do
      delay =
        min(
          record.base_backoff_ms * Integer.pow(2, max(record.attempts - 1, 0)),
          record.max_backoff_ms
        )

      retry_token = make_ref()

      record = %{
        record
        | status: :retry_wait,
          retry_token: retry_token
      }

      Process.send_after(self(), {:retry, record.id, retry_token}, delay)

      state
      |> put_record(record)
      |> add_history(record.id, :delivery_retry_scheduled, %{
        reason: reason,
        attempt: record.attempts,
        delay_ms: delay
      })
    else
      record = %{record | status: :failed, retry_token: nil}

      state
      |> put_record(record)
      |> add_history(record.id, :delivery_failed, %{reason: reason})
    end
  end

  defp expire_delivery(state, id, record) do
    state =
      if record.worker_ref do
        clear_worker(state, record.worker_ref, true)
      else
        state
      end

    state
    |> Map.update!(:deliveries, &Map.delete(&1, id))
    |> add_history(id, :delivery_expired, %{})
  end

  defp clear_worker(state, ref, kill?) do
    case Map.pop(state.workers, ref) do
      {nil, workers} ->
        %{state | workers: workers}

      {{_id, _token, pid}, workers} ->
        if kill?, do: Process.exit(pid, :kill)
        Process.demonitor(ref, [:flush])
        %{state | workers: workers}
    end
  end

  defp put_record(state, record) do
    put_in(state.deliveries[record.id], record)
  end

  defp add_history(state, id, operation, details) do
    entry = %{
      timestamp: now_ms(),
      delivery_id: id,
      operation: operation,
      details: details
    }

    %{state | history: [entry | state.history]}
  end

  defp public_record(record) do
    record
    |> Map.drop([:worker_fun, :worker_ref, :token, :retry_token])
    |> Map.update!(:tracking, &Enum.reverse/1)
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  defp now_ms, do: System.system_time(:millisecond)

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  defp non_negative_integer(_value, default), do: default
end
