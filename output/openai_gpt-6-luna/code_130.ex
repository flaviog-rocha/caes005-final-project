# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  use Supervisor

  @registry DeliverySystem.Registry
  @workers DeliverySystem.Workers
  @store DeliverySystem.Store

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {@store, opts},
      {DynamicSupervisor, strategy: :one_for_one, name: @workers}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def submit(entity_id, payload, handler) when is_function(handler, 1) do
    with {:ok, delivery_id} <- @store.enqueue(entity_id, payload, handler) do
      ensure_worker(entity_id)
      {:ok, delivery_id}
    end
  end

  def status(entity_id), do: @store.status(entity_id)

  defp ensure_worker(entity_id) do
    case Registry.lookup(@registry, entity_id) do
      [{pid, _}] ->
        GenServer.cast(pid, :wake)

      [] ->
        spec = {DeliverySystem.Worker, entity_id}

        case DynamicSupervisor.start_child(@workers, spec) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, pid}} -> GenServer.cast(pid, :wake)
          {:error, _reason} -> :ok
        end
    end
  end
end

defmodule DeliverySystem.Store do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def enqueue(entity_id, payload, handler) do
    GenServer.call(__MODULE__, {:enqueue, entity_id, payload, handler})
  end

  def acquire(entity_id) do
    GenServer.call(__MODULE__, {:acquire, entity_id})
  end

  def complete(entity_id, delivery_id, result) do
    GenServer.call(__MODULE__, {:complete, entity_id, delivery_id, result})
  end

  def retry(entity_id, delivery_id, reason) do
    GenServer.call(__MODULE__, {:retry, entity_id, delivery_id, reason})
  end

  def status(entity_id) do
    GenServer.call(__MODULE__, {:status, entity_id})
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       jobs: %{},
       entities: %{},
       next_id: 1,
       max_attempts: Keyword.get(opts, :max_attempts, 5),
       base_delay: Keyword.get(opts, :base_delay, 250)
     }}
  end

  @impl true
  def handle_call({:enqueue, entity_id, payload, handler}, _from, state) do
    id = state.next_id

    job = %{
      id: id,
      payload: payload,
      handler: handler,
      attempts: 0,
      status: :pending,
      result: nil,
      last_error: nil
    }

    entity_jobs = Map.get(state.entities, entity_id, [])

    state = %{
      state
      | jobs: Map.put(state.jobs, id, job),
        entities: Map.put(state.entities, entity_id, entity_jobs ++ [id]),
        next_id: id + 1
    }

    {:reply, {:ok, id}, state}
  end

  def handle_call({:acquire, entity_id}, _from, state) do
    ids = Map.get(state.entities, entity_id, [])
    now = System.monotonic_time(:millisecond)

    ready_id =
      Enum.find(ids, fn id ->
        case Map.fetch!(state.jobs, id).status do
          :pending -> true
          :running -> true
          {:retry, due_at} -> due_at <= now
          _ -> false
        end
      end)

    case ready_id do
      nil ->
        wait =
          ids
          |> Enum.flat_map(fn id ->
            case Map.fetch!(state.jobs, id).status do
              {:retry, due_at} -> [max(due_at - now, 1)]
              _ -> []
            end
          end)
          |> Enum.min(fn -> nil end)

        reply = if is_nil(wait), do: :empty, else: {:wait, wait}
        {:reply, reply, state}

      id ->
        job = Map.fetch!(state.jobs, id)
        job = %{job | status: :running, attempts: job.attempts + 1}
        state = put_in(state.jobs[id], job)

        reply =
          {:job, id, job.payload, job.handler, job.attempts,
           state.max_attempts}

        {:reply, reply, state}
    end
  end

  def handle_call({:complete, _entity_id, id, result}, _from, state) do
    job = Map.fetch!(state.jobs, id)
    job = %{job | status: :completed, result: result, last_error: nil}
    {:reply, :ok, put_in(state.jobs[id], job)}
  end

  def handle_call({:retry, _entity_id, id, reason}, _from, state) do
    job = Map.fetch!(state.jobs, id)

    if job.attempts >= state.max_attempts do
      job = %{job | status: :failed, last_error: reason}
      {:reply, :failed, put_in(state.jobs[id], job)}
    else
      exponent = min(job.attempts - 1, 15)
      delay = min(state.base_delay * Integer.pow(2, exponent), 30_000)
      due_at = System.monotonic_time(:millisecond) + delay
      job = %{job | status: {:retry, due_at}, last_error: reason}
      {:reply, {:retry, delay}, put_in(state.jobs[id], job)}
    end
  end

  def handle_call({:status, entity_id}, _from, state) do
    deliveries =
      Map.get(state.entities, entity_id, [])
      |> Enum.map(fn id ->
        job = Map.fetch!(state.jobs, id)

        %{
          id: job.id,
          payload: job.payload,
          attempts: job.attempts,
          status: public_status(job.status),
          result: job.result,
          last_error: job.last_error
        }
      end)

    {:reply, deliveries, state}
  end

  defp public_status({:retry, _due_at}), do: :retrying
  defp public_status(status), do: status
end

defmodule DeliverySystem.Worker do
  use GenServer

  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id,
      name: {:via, Registry, {DeliverySystem.Registry, entity_id}}
    )
  end

  @impl true
  def init(entity_id) do
    send(self(), :work)
    {:ok, entity_id}
  end

  @impl true
  def handle_cast(:wake, entity_id) do
    send(self(), :work)
    {:noreply, entity_id}
  end

  @impl true
  def handle_info(:work, entity_id) do
    case DeliverySystem.Store.acquire(entity_id) do
      {:job, id, payload, handler, _attempt, _max_attempts} ->
        case invoke(handler, payload) do
          {:ok, result} ->
            DeliverySystem.Store.complete(entity_id, id, result)
            send(self(), :work)

          {:error, reason} ->
            schedule_retry(entity_id, id, reason)
        end

        {:noreply, entity_id}

      {:wait, delay} ->
        Process.send_after(self(), :work, delay)
        {:noreply, entity_id}

      :empty ->
        {:noreply, entity_id}
    end
  end

  defp schedule_retry(entity_id, id, reason) do
    case DeliverySystem.Store.retry(entity_id, id, reason) do
      {:retry, delay} -> Process.send_after(self(), :work, delay)
      :failed -> send(self(), :work)
    end
  end

  defp invoke(handler, payload) do
    result =
      try do
        handler.(payload)
      rescue
        error -> {:error, {:exception, error.__struct__, Exception.message(error)}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      :ok -> {:ok, :ok}
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_result, other}}
    end
  end
end
