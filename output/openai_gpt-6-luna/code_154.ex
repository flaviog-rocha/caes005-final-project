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

defmodule DeliveryManager.FakeRepo do
  @moduledoc """
  Repositório fictício. Substitua `:persist_fun` ao iniciar o gerenciador
  para integrar outro armazenamento.
  """

  def persist(_delivery), do: :ok
end

defmodule DeliveryManager do
  @moduledoc """
  Gerenciamento de entregas com rastreamento, expiração, histórico,
  monitoramento de processos de persistência e novas tentativas.
  """

  use GenServer

  @default_ttl 86_400_000
  @default_max_retries 3
  @default_retry_base_ms 100

  # API

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def create(attrs), do: create(__MODULE__, attrs)

  def create(server, attrs) when is_map(attrs) do
    GenServer.call(server, {:create, attrs})
  end

  def track(id, event), do: track(__MODULE__, id, event)

  def track(server, id, event) do
    GenServer.call(server, {:track, id, event})
  end

  def get(id), do: get(__MODULE__, id)

  def get(server, id) do
    GenServer.call(server, {:get, id})
  end

  def list, do: list(__MODULE__)

  def list(server) do
    GenServer.call(server, :list)
  end

  def history, do: history(__MODULE__)

  def history(server) do
    GenServer.call(server, {:history, :all})
  end

  def history(server, limit) when is_integer(limit) and limit >= 0 do
    GenServer.call(server, {:history, limit})
  end

  # Callbacks do GenServer

  @impl true
  def init(opts) do
    state = %{
      deliveries: %{},
      timers: %{},
      history: [],
      pending: %{},
      monitors: %{},
      persist_fun: Keyword.get(opts, :persist_fun, &DeliveryManager.FakeRepo.persist/1),
      ttl: Keyword.get(opts, :ttl, @default_ttl),
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      retry_base_ms: Keyword.get(opts, :retry_base_ms, @default_retry_base_ms)
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = Map.get(attrs, :id) || Integer.to_string(System.unique_integer([:positive, :monotonic]))

    if Map.has_key?(state.deliveries, id) do
      {:reply, {:error, :already_exists}, state}
    else
      now = now_ms()

      delivery =
        attrs
        |> Map.put(:id, id)
        |> Map.merge(%{
          status: :created,
          tracking: [],
          created_at: now,
          updated_at: now,
          expires_at: :infinity,
          version: 1
        })

      {state, delivery} = refresh_expiration(state, delivery)
      state = %{state | deliveries: Map.put(state.deliveries, id, delivery)}
      state = record(state, :created, id, %{})
      state = enqueue_persistence(state, delivery, :created)

      {:reply, {:ok, delivery}, state}
    end
  end

  def handle_call({:track, id, event}, _from, state) do
    case Map.fetch(state.deliveries, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, delivery} ->
        now = now_ms()
        status = if is_map(event), do: Map.get(event, :status, delivery.status), else: delivery.status

        updated =
          delivery
          |> Map.put(:status, status)
          |> Map.put(:updated_at, now)
          |> Map.put(:version, delivery.version + 1)
          |> Map.put(:tracking, [%{at: now, event: event} | delivery.tracking])

        {state, updated} = refresh_expiration(state, updated)
        state = %{state | deliveries: Map.put(state.deliveries, id, updated)}
        state = record(state, :tracked, id, %{event: event, status: status})
        state = enqueue_persistence(state, updated, :tracked)

        {:reply, {:ok, updated}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.fetch(state.deliveries, id), state}
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state.deliveries), state}
  end

  def handle_call({:history, :all}, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:history, limit}, _from, state) do
    entries = Enum.reverse(state.history)
    {:reply, Enum.take(entries, -limit), state}
  end

  @impl true
  def handle_info({:expire, id, version}, state) do
    case Map.fetch(state.deliveries, id) do
      {:ok, %{version: ^version, expires_at: expires_at}} when is_integer(expires_at) ->
        if expires_at <= now_ms() do
          state = cancel_expiration(state, id)
          deliveries = Map.delete(state.deliveries, id)
          state = %{state | deliveries: deliveries}
          {:noreply, record(state, :expired, id, %{})}
        else
          {:noreply, reschedule_expiration(state, id, version, expires_at)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_persistence, op_id, attempt}, state) do
    case Map.get(state.pending, op_id) do
      %{attempt: ^attempt, ref: nil} = pending ->
        {:noreply, start_persistence(state, op_id, pending)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:persist_result, op_id, attempt, result}, state) do
    case Map.get(state.pending, op_id) do
      %{attempt: ^attempt, ref: ref} = pending ->
        state = forget_monitor(state, ref)

        case result do
          :ok ->
            state = %{state | pending: Map.delete(state.pending, op_id)}
            {:noreply, record(state, :persistence_succeeded, pending.id, %{operation: pending.operation})}

          {:error, reason} ->
            pending = %{pending | ref: nil}
            state = %{state | pending: Map.put(state.pending, op_id, pending)}
            {:noreply, retry_or_finish(state, op_id, pending, reason)}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {op_id, monitors} ->
        state = %{state | monitors: monitors}

        case Map.get(state.pending, op_id) do
          %{ref: ^ref} = pending ->
            pending = %{pending | ref: nil}
            state = %{state | pending: Map.put(state.pending, op_id, pending)}
            {:noreply, retry_or_finish(state, op_id, pending, {:worker_exit, reason})}

          _ ->
            {:noreply, state}
        end
    end
  end

  # Persistência assíncrona monitorada

  defp enqueue_persistence(state, delivery, operation) do
    op_id = make_ref()

    pending = %{
      id: delivery.id,
      delivery: delivery,
      operation: operation,
      attempt: 0,
      ref: nil,
      retry_timer: nil
    }

    start_persistence(%{state | pending: Map.put(state.pending, op_id, pending)}, op_id, pending)
  end

  defp start_persistence(state, op_id, pending) do
    parent = self()
    persist_fun = state.persist_fun
    attempt = pending.attempt
    delivery = pending.delivery

    {_pid, ref} =
      spawn_monitor(fn ->
        result =
          try do
            case persist_fun.(delivery) do
              :ok -> :ok
              {:ok, _value} -> :ok
              {:error, reason} -> {:error, reason}
              other -> {:error, {:unexpected_result, other}}
            end
          rescue
            exception ->
              {:error, {:exception, exception.__struct__, Exception.message(exception)}}
          catch
            kind, reason ->
              {:error, {kind, reason}}
          end

        send(parent, {:persist_result, op_id, attempt, result})
      end)

    pending = %{pending | ref: ref, retry_timer: nil}

    %{
      state
      | pending: Map.put(state.pending, op_id, pending),
        monitors: Map.put(state.monitors, ref, op_id)
    }
  end

  defp retry_or_finish(state, op_id, pending, reason) do
    if temporary_failure?(reason) and pending.attempt < state.max_retries do
      next_attempt = pending.attempt + 1
      delay = state.retry_base_ms * trunc(:math.pow(2, pending.attempt))
      timer = Process.send_after(self(), {:retry_persistence, op_id, next_attempt}, delay)

      pending = %{pending | attempt: next_attempt, ref: nil, retry_timer: timer}
      state = %{state | pending: Map.put(state.pending, op_id, pending)}

      record(state, :retry_scheduled, pending.id, %{
        operation: pending.operation,
        attempt: next_attempt,
        delay_ms: delay,
        reason: reason
      })
    else
      state = %{state | pending: Map.delete(state.pending, op_id)}

      record(state, :persistence_failed, pending.id, %{
        operation: pending.operation,
        attempts: pending.attempt + 1,
        reason: reason
      })
    end
  end

  defp temporary_failure?({:temporary, _reason}), do: true
  defp temporary_failure?({:worker_exit, _reason}), do: true
  defp temporary_failure?(:timeout), do: true
  defp temporary_failure?(:econnrefused), do: true
  defp temporary_failure?(:econnreset), do: true
  defp temporary_failure?(_reason), do: false

  defp forget_monitor(state, ref) do
    Process.demonitor(ref, [:flush])
    %{state | monitors: Map.delete(state.monitors, ref)}
  end

  # Expiração dos registros

  defp refresh_expiration(state, delivery) do
    state = cancel_expiration(state, delivery.id)

    case state.ttl do
      :infinity ->
        {%{state | timers: Map.delete(state.timers, delivery.id)},
         Map.put(delivery, :expires_at, :infinity)}

      ttl when is_integer(ttl) and ttl >= 0 ->
        expires_at = now_ms() + ttl
        timer = Process.send_after(self(), {:expire, delivery.id, delivery.version}, ttl)

        state = %{state | timers: Map.put(state.timers, delivery.id, timer)}
        {state, Map.put(delivery, :expires_at, expires_at)}
    end
  end

  defp cancel_expiration(state, id) do
    case Map.pop(state.timers, id) do
      {nil, timers} ->
        %{state | timers: timers}

      {timer, timers} ->
        Process.cancel_timer(timer)
        %{state | timers: timers}
    end
  end

  defp reschedule_expiration(state, id, version, expires_at) do
    delay = max(expires_at - now_ms(), 0)
    timer = Process.send_after(self(), {:expire, id, version}, delay)
    %{state | timers: Map.put(state.timers, id, timer)}
  end

  # Histórico

  defp record(state, operation, id, details) do
    entry = %{at: now_ms(), operation: operation, id: id, details: details}
    %{state | history: [entry | state.history]}
  end

  defp now_ms, do: System.system_time(:millisecond)
end
