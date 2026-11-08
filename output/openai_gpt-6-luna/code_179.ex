# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  use Supervisor

  @registry NotificationSystem.Registry
  @entity_supervisor NotificationSystem.EntitySupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @entity_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_entity(entity, handler, opts \\ []) when is_function(handler, 2) do
    case Registry.lookup(@registry, entity) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        spec = %{
          id: make_ref(),
          start: {NotificationSystem.Entity, :start_link, [entity, handler, opts]},
          restart: :permanent,
          shutdown: 5_000,
          type: :worker
        }

        case DynamicSupervisor.start_child(@entity_supervisor, spec) do
          {:error, {:already_started, pid}} -> {:ok, pid}
          result -> result
        end
    end
  end

  def notify(entity, notification) do
    case Registry.lookup(@registry, entity) do
      [{pid, _}] ->
        GenServer.cast(pid, {:enqueue, notification})

      [] ->
        {:error, :entity_not_found}
    end
  end

  def status(entity) do
    case Registry.lookup(@registry, entity) do
      [{pid, _}] -> GenServer.call(pid, :status)
      [] -> {:error, :entity_not_found}
    end
  end
end

defmodule NotificationSystem.Entity do
  use GenServer

  def start_link(entity, handler, opts) do
    GenServer.start_link(
      __MODULE__,
      {entity, handler, opts},
      name: {:via, Registry, {NotificationSystem.Registry, entity}}
    )
  end

  @impl true
  def init({entity, handler, opts}) do
    {:ok,
     %{
       entity: entity,
       handler: handler,
       queue: :queue.new(),
       scheduled: false,
       max_retries: Keyword.get(opts, :max_retries, :infinity),
       base_delay: Keyword.get(opts, :base_delay, 500),
       max_delay: Keyword.get(opts, :max_delay, 30_000)
     }}
  end

  @impl true
  def handle_cast({:enqueue, notification}, state) do
    queue = :queue.in({notification, 0}, state.queue)
    {:noreply, schedule_now(%{state | queue: queue})}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{entity: state.entity, queued: :queue.len(state.queue)}, state}
  end

  @impl true
  def handle_info(:deliver, state) do
    state = %{state | scheduled: false}

    case :queue.peek(state.queue) do
      :empty ->
        {:noreply, state}

      {:value, {notification, retries}} ->
        case deliver(state.handler, state.entity, notification) do
          :ok ->
            {:noreply, state |> drop_front() |> schedule_now()}

          {:temporary, _reason} ->
            if retry_allowed?(state.max_retries, retries) do
              next_retries = retries + 1
              delay = retry_delay(state, retries)
              queue = replace_front(state.queue, {notification, next_retries})
              Process.send_after(self(), :deliver, delay)
              {:noreply, %{state | queue: queue, scheduled: true}}
            else
              {:noreply, state |> drop_front() |> schedule_now()}
            end

          :permanent ->
            {:noreply, state |> drop_front() |> schedule_now()}
        end
    end
  end

  defp deliver(handler, entity, notification) do
    result =
      try do
        handler.(entity, notification)
      rescue
        exception -> {:error, :temporary, exception}
      catch
        kind, reason -> {:error, :temporary, {kind, reason}}
      end

    case result do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, :temporary, reason} -> {:temporary, reason}
      {:error, :temporary} -> {:temporary, nil}
      {:error, :transient, reason} -> {:temporary, reason}
      _other -> :permanent
    end
  end

  defp retry_allowed?(:infinity, _retries), do: true
  defp retry_allowed?(max_retries, retries), do: retries < max_retries

  defp retry_delay(state, retries) do
    exponent = min(retries, 20)
    min(state.base_delay * Integer.pow(2, exponent), state.max_delay)
  end

  defp replace_front(queue, value) do
    {{:value, _old_value}, queue} = :queue.out(queue)
    :queue.in_r(value, queue)
  end

  defp drop_front(state) do
    %{state | queue: :queue.drop(state.queue)}
  end

  defp schedule_now(%{scheduled: true} = state), do: state

  defp schedule_now(state) do
    case :queue.is_empty(state.queue) do
      true ->
        state

      false ->
        send(self(), :deliver)
        %{state | scheduled: true}
    end
  end
end
