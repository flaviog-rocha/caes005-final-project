# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de gerenciamento de notificações em Elixir.
#
# Requisitos atendidos:
#   1. Novas tentativas após falhas temporárias (backoff exponencial com jitter).
#   2. Recuperação de processos após falhas inesperadas (supervisão + estado
#      persistido em ETS, fora do processo do worker).
#   3. Processamento independente por entidade (um GenServer por entidade,
#      localizado via Registry e supervisionado por DynamicSupervisor).

defmodule Notifications do
  @moduledoc """
  Sistema de notificações tolerante a falhas.

  Uso básico:

      Notifications.start_link([])
      Notifications.notify("usuario:42", %{titulo: "Olá"})
      Notifications.notify("usuario:42", %{titulo: "Outra"}, deliver: &minha_entrega/2)
  """

  use Application
  require Logger

  @doc false
  @impl true
  def start(_type, _args) do
    start_link([])
  end

  @doc """
  Inicia a árvore de supervisão. Útil quando não há `mod:` configurado no mix.
  """
  def start_link(_opts) do
    children = [
      # Store primeiro: se ele cair, os workers dependentes são reiniciados
      # (rest_for_one), pois o estado pendente vive na tabela ETS dele.
      Notifications.Store,
      {Registry, keys: :unique, name: Notifications.Registry},
      {DynamicSupervisor,
       name: Notifications.EntitySupervisor,
       strategy: :one_for_one,
       max_restarts: 50,
       max_seconds: 10}
    ]

    Supervisor.start_link(children,
      strategy: :rest_for_one,
      name: Notifications.Supervisor
    )
  end

  @doc """
  Enfileira uma notificação para a entidade informada.

  Opções:
    * `:deliver` - função `(payload, tentativa -> :ok | {:error, :temporary | :permanent, motivo})`
    * `:max_attempts` - número máximo de tentativas para falhas temporárias
  """
  def notify(entity_id, payload, opts \\ []) do
    with {:ok, pid} <- ensure_entity(entity_id, opts) do
      Notifications.EntityWorker.enqueue(pid, payload)
    end
  end

  @doc "Retorna (ou inicia) o processo responsável pela entidade."
  def ensure_entity(entity_id, opts \\ []) do
    case Registry.lookup(Notifications.Registry, entity_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        spec = {Notifications.EntityWorker, {entity_id, opts}}

        case DynamicSupervisor.start_child(Notifications.EntitySupervisor, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          other -> other
        end
    end
  end

  @doc "Lista notificações pendentes de uma entidade."
  def pending(entity_id), do: Notifications.Store.pending(entity_id)

  @doc "Lista notificações descartadas (dead letter) de uma entidade."
  def dead_letters(entity_id), do: Notifications.Store.dead_letters(entity_id)

  @doc """
  Implementação padrão de entrega, apenas para demonstração.
  Simula sucesso, falha temporária e falha permanente.
  """
  def default_deliver(_payload, _attempt) do
    case :rand.uniform(100) do
      n when n <= 70 -> :ok
      n when n <= 95 -> {:error, :temporary, :timeout}
      _ -> {:error, :permanent, :invalid_recipient}
    end
  end
end

defmodule Notifications.Store do
  @moduledoc """
  Armazenamento em ETS de notificações pendentes e descartadas.

  A tabela pertence a este processo, e não ao worker. Assim, se um worker
  falhar, o estado sobrevive e pode ser recuperado após o restart.
  """

  use GenServer

  @pending :notifications_pending
  @dead :notifications_dead

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    :ets.new(@pending, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(@dead, [:set, :public, :named_table])
    {:ok, %{}}
  end

  @doc "Grava ou atualiza um registro pendente."
  def put(%{id: id, entity_id: entity_id} = record) do
    :ets.insert(@pending, {id, entity_id, record})
    :ok
  end

  @doc "Busca um registro pendente pelo id."
  def fetch(id) do
    case :ets.lookup(@pending, id) do
      [{^id, _entity_id, record}] -> {:ok, record}
      [] -> :error
    end
  end

  @doc "Remove um registro pendente (entrega concluída)."
  def delete(id) do
    :ets.delete(@pending, id)
    :ok
  end

  @doc "Lista os registros pendentes de uma entidade."
  def pending(entity_id) do
    @pending
    |> :ets.match_object({:_, entity_id, :_})
    |> Enum.map(fn {_id, _entity_id, record} -> record end)
  end

  @doc "Move um registro para a tabela de descartados."
  def dead_letter(%{id: id, entity_id: entity_id} = record, reason) do
    :ets.insert(@dead, {id, entity_id, reason, record})
    :ets.delete(@pending, id)
    :ok
  end

  @doc "Lista os registros descartados de uma entidade."
  def dead_letters(entity_id) do
    @dead
    |> :ets.match_object({:_, entity_id, :_, :_})
    |> Enum.map(fn {_id, _entity_id, reason, record} -> {reason, record} end)
  end
end

defmodule Notifications.EntityWorker do
  @moduledoc """
  Processo independente por entidade.

  - Cada entidade possui seu próprio GenServer, então falhas ou backoff de uma
    entidade não bloqueiam as demais.
  - Ao iniciar (inclusive após um restart pelo supervisor), reprocessa as
    notificações pendentes que estão no Store.
  - Falhas temporárias geram novas tentativas com backoff exponencial.
  - Falhas permanentes e esgotamento de tentativas vão para dead letter.
  """

  use GenServer
  require Logger

  alias Notifications.Store

  @default_max_attempts 5
  @base_delay 200
  @max_delay 10_000

  def start_link({entity_id, opts}) do
    GenServer.start_link(__MODULE__, {entity_id, opts}, name: via(entity_id))
  end

  @doc "Enfileira um payload no worker da entidade."
  def enqueue(pid, payload) do
    GenServer.call(pid, {:enqueue, payload})
  end

  defp via(entity_id), do: {:via, Registry, {Notifications.Registry, entity_id}}

  @impl true
  def init({entity_id, opts}) do
    state = %{
      entity_id: entity_id,
      deliver: Keyword.get(opts, :deliver, &Notifications.default_deliver/2),
      max_attempts: Keyword.get(opts, :max_attempts, @default_max_attempts)
    }

    # Recuperação: reagenda o processamento de tudo o que ficou pendente
    # antes de uma eventual falha do worker anterior.
    for %{id: id} <- Store.pending(entity_id) do
      send(self(), {:attempt, id})
    end

    {:ok, state}
  end

  @impl true
  def handle_call({:enqueue, payload}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])

    record = %{
      id: id,
      entity_id: state.entity_id,
      payload: payload,
      attempts: 0
    }

    # Persiste antes de tentar entregar: se o processo cair, nada se perde.
    :ok = Store.put(record)
    send(self(), {:attempt, id})

    {:reply, {:ok, id}, state}
  end

  @impl true
  def handle_info({:attempt, id}, state) do
    case Store.fetch(id) do
      {:ok, record} -> handle_attempt(record, state)
      :error -> {:noreply, state}
    end
  end

  defp handle_attempt(record, state) do
    attempt = record.attempts + 1

    case safe_deliver(state.deliver, record.payload, attempt) do
      :ok ->
        Store.delete(record.id)
        {:noreply, state}

      {:error, :permanent, reason} ->
        Logger.error(
          "Notificação #{record.id} da entidade #{state.entity_id} falhou permanentemente: #{inspect(reason)}"
        )

        Store.dead_letter(record, reason)
        {:noreply, state}

      {:error, :temporary, reason} when attempt < state.max_attempts ->
        # Registra a tentativa antes de agendar a próxima, para que a
        # recuperação após falha continue contando corretamente.
        :ok = Store.put(%{record | attempts: attempt})
        delay = backoff(attempt)

        Logger.warning(
          "Notificação #{record.id} (entidade #{state.entity_id}) falhou na tentativa #{attempt}: " <>
            "#{inspect(reason)}. Nova tentativa em #{delay}ms"
        )

        Process.send_after(self(), {:attempt, record.id}, delay)
        {:noreply, state}

      {:error, :temporary, reason} ->
        Logger.error(
          "Notificação #{record.id} da entidade #{state.entity_id} esgotou #{attempt} tentativas: " <>
            "#{inspect(reason)}"
        )

        Store.dead_letter(record, {:max_attempts_exceeded, reason})
        {:noreply, state}
    end
  end

  # Exceções lançadas pela entrega são tratadas como falhas temporárias.
  # Erros de lógica do próprio worker ainda causam crash e são recuperados
  # pelo supervisor.
  defp safe_deliver(deliver, payload, attempt) do
    case deliver.(payload, attempt) do
      :ok -> :ok
      {:error, kind, reason} when kind in [:temporary, :permanent] -> {:error, kind, reason}
      other -> {:error, :temporary, {:unexpected_return, other}}
    end
  rescue
    exception -> {:error, :temporary, exception}
  end

  # Backoff exponencial limitado, com jitter para evitar rajadas sincronizadas.
  defp backoff(attempt) do
    exp = min(@base_delay * Integer.pow(2, attempt - 1), @max_delay)
    exp + :rand.uniform(@base_delay)
  end
end
