# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  use Supervisor

  @table :notification_manager_dets
  @registry NotificationManager.Registry
  @workers NotificationManager.Workers

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> Supervisor.start_link(__MODULE__, opts, name: name)
      :error -> Supervisor.start_link(__MODULE__, opts)
    end
  end

  @impl true
  def init(opts) do
    children = [
      {NotificationManager.Store, file: Keyword.get(opts, :file, "notifications.dets")},
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @workers}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_entity(entity_id, delivery_fun, opts \\ []) when is_function(delivery_fun, 1) do
    case Registry.lookup(@registry, entity_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        DynamicSupervisor.start_child(
          @workers,
          {NotificationManager.EntityWorker,
           [
             entity_id: entity_id,
             delivery_fun: delivery_fun,
             poll_interval_ms: Keyword.get(opts, :poll_interval_ms, 1_000)
           ]}
        )
    end
  end

  def stop_entity(entity_id) do
    case Registry.lookup(@registry, entity_id) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(@workers, pid)
      [] -> {:error, :not_found}
    end
  end

  def enqueue(entity_id, payload, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, 5)
    retry_delay_ms = Keyword.get(opts, :retry_delay_ms, 1_000)

    if is_integer(max_attempts) and max_attempts > 0 and
         is_integer(retry_delay_ms) and retry_delay_ms >= 0 do
      id = {System.system_time(:microsecond), :erlang.unique_integer([:positive, :monotonic])}
      key = {entity_id, id}

      notification = %{
        payload: payload,
        attempts: 0,
        max_attempts: max_attempts,
        retry_delay_ms: retry_delay_ms,
        available_at: System.system_time(:millisecond)
      }

      case :dets.insert(@table, {key, notification}) do
        :ok ->
          case Registry.lookup(@registry, entity_id) do
            [{pid, _}] -> send(pid, :poll)
            [] -> :ok
          end

          {:ok, id}

        error ->
          error
      end
    else
      {:error, :invalid_retry_options}
    end
  end
end

defmodule NotificationManager.Store do
  use GenServer

  @table :notification_manager_dets

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file = opts |> Keyword.fetch!(:file) |> to_charlist()

    case :dets.open_file(@table, file: file, type: :set, repair: true) do
      {:ok, @table} -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end
end

defmodule NotificationManager.EntityWorker do
  use GenServer

  @table :notification_manager_dets
  @registry NotificationManager.Registry

  def start_link(opts) do
    entity_id = Keyword.fetch!(opts, :entity_id)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {@registry, entity_id}})
  end

  @impl true
  def init(opts) do
    state = %{
      entity_id: Keyword.fetch!(opts, :entity_id),
      delivery_fun: Keyword.fetch!(opts, :delivery_fun),
      poll_interval_ms: Keyword.fetch!(opts, :poll_interval_ms)
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    now = System.system_time(:millisecond)

    case next_due_notification(state.entity_id, now) do
      nil ->
        Process.send_after(self(), :poll, state.poll_interval_ms)
        {:noreply, state}

      {key, notification} ->
        result = deliver(state.delivery_fun, notification.payload)
        handle_result(result, key, notification)
        send(self(), :poll)
        {:noreply, state}
    end
  end

  defp next_due_notification(entity_id, now) do
    :dets.foldl(
      fn
        {{^entity_id, _id} = key, notification}, best ->
          if notification.available_at <= now do
            choose_earlier(best, {key, notification})
          else
            best
          end

        _other, best ->
          best
      end,
      nil,
      @table
    )
  end

  defp choose_earlier(nil, candidate), do: candidate

  defp choose_earlier({_, current} = best, {_, candidate} = next) do
    if candidate.available_at < current.available_at, do: next, else: best
  end

  defp deliver(delivery_fun, payload) do
    try do
      delivery_fun.(payload)
    rescue
      _ -> {:error, :delivery_exception}
    catch
      _, _ -> {:error, :delivery_exception}
    end
  end

  defp handle_result(result, key, notification) when result in [:ok, :sent] do
    :dets.delete(@table, key)
  end

  defp handle_result({:ok, _value}, key, _notification) do
    :dets.delete(@table, key)
  end

  defp handle_result({:error, {:permanent, _reason}}, key, _notification) do
    :dets.delete(@table, key)
  end

  defp handle_result(:permanent, key, _notification) do
    :dets.delete(@table, key)
  end

  defp handle_result(_temporary_failure, key, notification) do
    attempts = notification.attempts + 1

    if attempts >= notification.max_attempts do
      :dets.delete(@table, key)
    else
      multiplier = 1 <<< min(attempts - 1, 20)

      updated = %{
        notification
        | attempts: attempts,
          available_at:
            System.system_time(:millisecond) + notification.retry_delay_ms * multiplier
      }

      :dets.insert(@table, {key, updated})
    end
  end
end
