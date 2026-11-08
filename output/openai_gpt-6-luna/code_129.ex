# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker do
  @moduledoc """
  Gerenciador de entregas com processos isolados por entidade, persistência em DETS
  e novas tentativas para falhas temporárias.

  O handler configurado deve ter a forma `{Modulo, Funcao, Argumentos}` e receber
  o payload como primeiro argumento. Pode retornar `:ok`, `{:ok, resultado}`,
  `{:error, :temporary}` ou outro `{:error, motivo}`.
  """

  @config_key {__MODULE__, :config}

  def start_link(opts \\ []) do
    config = %{
      handler: Keyword.get(opts, :handler, {__MODULE__, :default_handler, []}),
      path:
        Keyword.get(
          opts,
          :path,
          Path.join(System.tmp_dir!(), "delivery_tracker.dets")
        ),
      max_retries: Keyword.get(opts, :max_retries, 5),
      base_delay: Keyword.get(opts, :base_delay, 1_000),
      max_delay: Keyword.get(opts, :max_delay, 30_000)
    }

    :persistent_term.put(@config_key, config)

    Supervisor.start_link(DeliveryTracker.Supervisor, config,
      name: DeliveryTracker.Supervisor
    )
  end

  @doc "Registra uma entrega. IDs repetidos para a mesma entidade não são inseridos novamente."
  def submit(entity_id, delivery_id, payload) do
    job = %{
      entity_id: entity_id,
      delivery_id: delivery_id,
      payload: payload,
      status: :queued,
      attempts: 0,
      submitted_at: System.system_time(:millisecond)
    }

    case DeliveryTracker.Store.insert_new(job) do
      :ok ->
        ensure_worker(entity_id)
        :ok

      :duplicate ->
        {:error, :duplicate}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Retorna o estado de uma entrega ou `nil` se ela não existir."
  def get(entity_id, delivery_id) do
    DeliveryTracker.Store.get(entity_id, delivery_id)
  end

  @doc "Retorna todas as entregas de uma entidade."
  def list(entity_id) do
    DeliveryTracker.Store.list(entity_id)
  end

  def default_handler(_payload), do: :ok

  def ensure_worker(entity_id) do
    config = :persistent_term.get(@config_key)

    spec = %{
      id: {DeliveryTracker.Worker, entity_id},
      start: {DeliveryTracker.Worker, :start_link, [entity_id, config]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }

    case DynamicSupervisor.start_child(DeliveryTracker.DynamicSupervisor, spec) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule DeliveryTracker.Supervisor do
  use Supervisor

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    children = [
      {DeliveryTracker.Store, config.path},
      {Registry, keys: :unique, name: DeliveryTracker.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: DeliveryTracker.DynamicSupervisor},
      {DeliveryTracker.Recovery, []}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule DeliveryTracker.Store do
  use GenServer

  @table :delivery_tracker_dets

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def insert_new(job), do: GenServer.call(__MODULE__, {:insert_new, job})
  def get(entity_id, delivery_id), do: GenServer.call(__MODULE__, {:get, entity_id, delivery_id})
  def list(entity_id), do: GenServer.call(__MODULE__, {:list, entity_id})
  def all, do: GenServer.call(__MODULE__, :all)
  def put(job), do: GenServer.call(__MODULE__, {:put, job})

  @impl true
  def init(path) do
    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} -> {:ok, @table}
      {:error, reason} -> {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:insert_new, job}, _from, table) do
    key = {job.entity_id, job.delivery_id}

    case :dets.insert_new(table, {key, job}) do
      true ->
        :dets.sync(table)
        {:reply, :ok, table}

      false ->
        {:reply, :duplicate, table}
    end
  end

  def handle_call({:get, entity_id, delivery_id}, _from, table) do
    reply =
      case :dets.lookup(table, {entity_id, delivery_id}) do
        [{{^entity_id, ^delivery_id}, job}] -> job
        [] -> nil
      end

    {:reply, reply, table}
  end

  def handle_call({:list, entity_id}, _from, table) do
    jobs =
      :dets.foldl(
        fn
          {{^entity_id, _delivery_id}, job}, acc -> [job | acc]
          _entry, acc -> acc
        end,
        [],
        table
      )

    {:reply, jobs, table}
  end

  def handle_call(:all, _from, table) do
    jobs = :dets.foldl(fn {_key, job}, acc -> [job | acc] end, [], table)
    {:reply, jobs, table}
  end

  def handle_call({:put, job}, _from, table) do
    :ok = :dets.insert(table, {{job.entity_id, job.delivery_id}, job})
    :ok = :dets.sync(table)
    {:reply, :ok, table}
  end

  @impl true
  def terminate(_reason, table) do
    :dets.close(table)
  end
end

defmodule DeliveryTracker.Worker do
  use GenServer

  def start_link(entity_id, config) do
    GenServer.start_link(__MODULE__, {entity_id, config},
      name: {:via, Registry, {DeliveryTracker.Registry, entity_id}}
    )
  end

  @impl true
  def init({entity_id, config}) do
    send(self(), :process_next)
    {:ok, %{entity_id: entity_id, config: config}}
  end

  @impl true
  def handle_info(:process_next, state) do
    case next_job(state.entity_id) do
      nil ->
        {:noreply, state}

      job ->
        job = %{job | status: :processing, attempts: job.attempts + 1}
        :ok = DeliveryTracker.Store.put(job)
        execute(job, state)
    end
  end

  def handle_info(:retry, state) do
    send(self(), :process_next)
    {:noreply, state}
  end

  defp next_job(entity_id) do
    entity_id
    |> DeliveryTracker.Store.list()
    |> Enum.find(fn job -> job.status in [:queued, :retrying, :processing] end)
  end

  defp execute(job, state) do
    case invoke_handler(state.config.handler, job.payload) do
      :ok ->
        finish(job, :delivered, nil)
        send(self(), :process_next)
        {:noreply, state}

      {:ok, result} ->
        finish(job, :delivered, result)
        send(self(), :process_next)
        {:noreply, state}

      {:error, :temporary} ->
        retry_or_fail(job, state)

      {:error, {:temporary, _reason}} ->
        retry_or_fail(job, state)

      {:error, reason} ->
        finish(job, :failed, reason)
        send(self(), :process_next)
        {:noreply, state}

      other ->
        finish(job, :failed, {:invalid_handler_result, other})
        send(self(), :process_next)
        {:noreply, state}
    end
  end

  defp invoke_handler({module, function, args}, payload) do
    apply(module, function, [payload | args])
  end

  defp retry_or_fail(job, state) do
    config = state.config

    if job.attempts <= config.max_retries do
      delay =
        config.base_delay
        |> Kernel.*(Integer.pow(2, job.attempts - 1))
        |> min(config.max_delay)

      :ok = DeliveryTracker.Store.put(%{job | status: :retrying})
      Process.send_after(self(), :retry, delay)
    else
      finish(job, :failed, :retries_exhausted)
      send(self(), :process_next)
    end

    {:noreply, state}
  end

  defp finish(job, status, result) do
    job
    |> Map.merge(%{
      status: status,
      result: result,
      completed_at: System.system_time(:millisecond)
    })
    |> DeliveryTracker.Store.put()
  end
end

defmodule DeliveryTracker.Recovery do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :recover)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:recover, state) do
    DeliveryTracker.Store.all()
    |> Enum.map(& &1.entity_id)
    |> Enum.uniq()
    |> Enum.each(&DeliveryTracker.ensure_worker/1)

    {:noreply, state}
  end
end
