# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Sistema de notificações com filas independentes por entidade, tentativas
  com backoff exponencial e reinício automático dos workers.
  """

  @registry NotificationSystem.Registry
  @dynamic_supervisor NotificationSystem.DynamicSupervisor

  def start_link(opts \\ []) do
    handler = Keyword.get(opts, :handler, fn _entity, _notification -> :ok end)
    retry_base = Keyword.get(opts, :retry_base, 100)
    retry_max = Keyword.get(opts, :retry_max, 5_000)
    name = Keyword.get(opts, :name, __MODULE__)

    unless is_function(handler, 2) do
      raise ArgumentError, "handler deve ser uma função de aridade 2"
    end

    unless is_integer(retry_base) and retry_base > 0 and
             is_integer(retry_max) and retry_max >= retry_base do
      raise ArgumentError, "retry_base e retry_max devem ser inteiros positivos, com retry_max >= retry_base"
    end

    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor}
    ]

    case Supervisor.start_link(children, strategy: :one_for_one, name: name) do
      {:ok, system} ->
        :persistent_term.put(
          {__MODULE__, system},
          %{handler: handler, retry_base: retry_base, retry_max: retry_max}
        )

        {:ok, system}

      error ->
        error
    end
  end

  @doc """
  Enfileira uma notificação para a entidade indicada.

  O handler recebe `(entidade, notificação)` e deve retornar `:ok`,
  `{:error, :temporary}` para solicitar uma nova tentativa, ou `{:error, motivo}`
  para descartar a notificação.
  """
  def notify(system, entity, notification) do
    case :persistent_term.get({__MODULE__, system}, nil) do
      nil ->
        {:error, :system_not_started}

      config ->
        with {:ok, worker} <- ensure_worker(entity, config) do
          GenServer.cast(worker, {:notify, notification})
          :ok
        end
    end
  end

  def stop(system, reason \\ :normal, timeout \\ :infinity) do
    try do
      Supervisor.stop(system, reason, timeout)
    after
      :persistent_term.erase({__MODULE__, system})
    end
  end

  defp ensure_worker(entity, config) do
    case Registry.lookup(@registry, entity) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        spec = %{
          id: make_ref(),
          start:
            {NotificationSystem.Worker, :start_link,
             [
               {entity, config.handler, config.retry_base, config.retry_max}
             ]},
          restart: :permanent,
          shutdown: 5_000,
          type: :worker
        }

        case DynamicSupervisor.start_child(@dynamic_supervisor, spec) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, reason} ->
            case Registry.lookup(@registry, entity) do
              [{pid, _}] -> {:ok, pid}
              [] -> {:error, reason}
            end
        end
    end
  end
end

defmodule NotificationSystem.Worker do
  use GenServer

  def start_link({entity, handler, retry_base, retry_max}) do
    GenServer.start_link(__MODULE__, {entity, handler, retry_base, retry_max},
      name: {:via, Registry, {NotificationSystem.Registry, entity}}
    )
  end

  @impl true
  def init({entity, handler, retry_base, retry_max}) do
    {:ok,
     %{
       entity: entity,
       handler: handler,
       retry_base: retry_base,
       retry_max: retry_max,
       queue: :queue.new(),
       attempts: 0
     }}
  end

  @impl true
  def handle_cast({:notify, notification}, state) do
    was_empty = :queue.is_empty(state.queue)
    queue = :queue.in(notification, state.queue)

    if was_empty do
      send(self(), :process_notification)
    end

    {:noreply, %{state | queue: queue}}
  end

  @impl true
  def handle_info(:process_notification, state) do
    case :queue.peek(state.queue) do
      :empty ->
        {:noreply, %{state | attempts: 0}}

      {:value, notification} ->
        case state.handler.(state.entity, notification) do
          :ok ->
            finish_notification(state)

          {:error, :temporary} ->
            retry_notification(state)

          {:error, {:temporary, _reason}} ->
            retry_notification(state)

          {:error, _reason} ->
            finish_notification(state)

          other ->
            raise ArgumentError, "retorno inválido do handler: #{inspect(other)}"
        end
    end
  end

  defp finish_notification(state) do
    {{:value, _notification}, queue} = :queue.out(state.queue)
    next_state = %{state | queue: queue, attempts: 0}

    if :queue.is_empty(queue) do
      {:noreply, next_state}
    else
      send(self(), :process_notification)
      {:noreply, next_state}
    end
  end

  defp retry_notification(state) do
    delay = backoff(state.retry_base, state.retry_max, state.attempts)
    Process.send_after(self(), :process_notification, delay)
    {:noreply, %{state | attempts: state.attempts + 1}}
  end

  defp backoff(delay, max, 0), do: min(delay, max)

  defp backoff(delay, max, attempts) when delay >= max, do: max

  defp backoff(delay, max, attempts) do
    backoff(min(delay * 2, max), max, attempts - 1)
  end
end
