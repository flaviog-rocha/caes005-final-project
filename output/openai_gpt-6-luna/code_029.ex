# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Executar determinadas operações periodicamente.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracker.Delivery do
  @enforce_keys [:id, :origin, :destination]
  defstruct [
    :id,
    :origin,
    :destination,
    :status,
    :created_at,
    :last_checked_at,
    events: []
  ]
end

defmodule DeliveryTracker.Database do
  use GenServer

  @snapshot_key {__MODULE__, :deliveries}

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def insert(delivery), do: call({:insert, delivery})
  def get(id), do: call({:get, id})
  def update(delivery), do: call({:update, delivery})

  @impl true
  def init(_opts) do
    {:ok, :persistent_term.get(@snapshot_key, %{})}
  end

  @impl true
  def handle_call({:insert, delivery}, _from, deliveries) do
    if Map.has_key?(deliveries, delivery.id) do
      {:reply, {:error, {:operation, :already_exists}}, deliveries}
    else
      updated = Map.put(deliveries, delivery.id, delivery)
      persist(updated)
      {:reply, {:ok, delivery}, updated}
    end
  end

  def handle_call({:get, id}, _from, deliveries) do
    case Map.fetch(deliveries, id) do
      {:ok, delivery} -> {:reply, {:ok, delivery}, deliveries}
      :error -> {:reply, {:error, {:operation, :not_found}}, deliveries}
    end
  end

  def handle_call({:update, delivery}, _from, deliveries) do
    if Map.has_key?(deliveries, delivery.id) do
      updated = Map.put(deliveries, delivery.id, delivery)
      persist(updated)
      {:reply, {:ok, delivery}, updated}
    else
      {:reply, {:error, {:operation, :not_found}}, deliveries}
    end
  end

  defp persist(deliveries), do: :persistent_term.put(@snapshot_key, deliveries)

  defp call(request) do
    try do
      GenServer.call(__MODULE__, request, 5_000)
    catch
      :exit, reason -> {:error, {:operation, {:database_unavailable, reason}}}
    end
  end
end

defmodule DeliveryTracker.DeliveryProcess do
  use GenServer

  alias DeliveryTracker.{Database, Delivery}

  def start_link({id, interval}) do
    GenServer.start_link(
      __MODULE__,
      {id, interval},
      name: {:via, Registry, {DeliveryTracker.Registry, id}}
    )
  end

  @impl true
  def init({id, interval}) do
    case Database.get(id) do
      {:ok, delivery} ->
        schedule_check(interval)
        {:ok, %{delivery: delivery, interval: interval}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:set_status, status}, _from, state) do
    delivery = state.delivery
    event = %{status: status, at: DateTime.utc_now()}
    updated = %{delivery | status: status, events: [event | delivery.events]}

    case Database.update(updated) do
      {:ok, saved} -> {:reply, {:ok, saved}, %{state | delivery: saved}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:get, _from, state), do: {:reply, {:ok, state.delivery}, state}

  @impl true
  def handle_info(:periodic_check, state) do
    checked_at = DateTime.utc_now()
    event = %{status: state.delivery.status, at: checked_at, type: :tracking_check}

    updated = %{
      state.delivery
      | last_checked_at: checked_at,
        events: [event | state.delivery.events]
    }

    next_state =
      case Database.update(updated) do
        {:ok, saved} -> %{state | delivery: saved}
        {:error, _reason} -> state
      end

    schedule_check(state.interval)
    {:noreply, next_state}
  end

  defp schedule_check(interval), do: Process.send_after(self(), :periodic_check, interval)
end

defmodule DeliveryTracker do
  use Supervisor

  alias DeliveryTracker.{Database, Delivery, DeliveryProcess}

  @statuses [:pending, :picked_up, :in_transit, :delivered, :failed]
  @default_interval 60_000

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Database,
      {Registry, keys: :unique, name: DeliveryTracker.Registry},
      {DynamicSupervisor,
       strategy: :one_for_one, name: DeliveryTracker.WorkerSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_delivery(attrs, opts \\ []) do
    interval = Keyword.get(opts, :interval, @default_interval)

    with :ok <- validate_attrs(attrs),
         :ok <- validate_interval(interval),
         {:ok, delivery} <- build_delivery(attrs),
         {:ok, saved} <- Database.insert(delivery),
         {:ok, _pid} <- start_delivery_process(saved.id, interval) do
      {:ok, saved}
    end
  end

  def get_delivery(id) do
    with :ok <- validate_id(id) do
      Database.get(id)
    end
  end

  def update_status(id, status) do
    with :ok <- validate_id(id),
         :ok <- validate_status(status),
         {:ok, _delivery} <- Database.get(id),
         {:ok, result} <- call_delivery_process(id, {:set_status, status}) do
      {:ok, result}
    end
  end

  defp build_delivery(attrs) do
    id = Map.get(attrs, :id) || Integer.to_string(:erlang.unique_integer([:positive, :monotonic]))

    {:ok,
     %Delivery{
       id: id,
       origin: Map.fetch!(attrs, :origin),
       destination: Map.fetch!(attrs, :destination),
       status: :pending,
       created_at: DateTime.utc_now(),
       events: [%{status: :pending, at: DateTime.utc_now()}]
     }}
  end

  defp start_delivery_process(id, interval) do
    child = %{
      id: {DeliveryProcess, id},
      start: {DeliveryProcess, :start_link, [{id, interval}]},
      restart: :transient,
      shutdown: 5_000,
      type: :worker
    }

    try do
      case DynamicSupervisor.start_child(DeliveryTracker.WorkerSupervisor, child) do
        {:ok, pid} -> {:ok, pid}
        {:error, reason} -> {:error, {:operation, {:worker_start_failed, reason}}}
      end
    catch
      :exit, reason -> {:error, {:operation, {:worker_supervisor_unavailable, reason}}}
    end
  end

  defp call_delivery_process(id, request) do
    try do
      GenServer.call(
        {:via, Registry, {DeliveryTracker.Registry, id}},
        request,
        5_000
      )
    catch
      :exit, reason -> {:error, {:operation, {:delivery_process_unavailable, reason}}}
    end
  end

  defp validate_attrs(attrs) when not is_map(attrs),
    do: {:error, {:validation, :attributes_must_be_a_map}}

  defp validate_attrs(attrs) do
    cond do
      not valid_text?(Map.get(attrs, :origin)) ->
        {:error, {:validation, :invalid_origin}}

      not valid_text?(Map.get(attrs, :destination)) ->
        {:error, {:validation, :invalid_destination}}

      Map.has_key?(attrs, :id) and not valid_text?(Map.get(attrs, :id)) ->
        {:error, {:validation, :invalid_id}}

      true ->
        :ok
    end
  end

  defp validate_id(id) do
    if valid_text?(id),
      do: :ok,
      else: {:error, {:validation, :invalid_id}}
  end

  defp validate_status(status) do
    if status in @statuses,
      do: :ok,
      else: {:error, {:validation, :invalid_status}}
  end

  defp validate_interval(interval) do
    if is_integer(interval) and interval > 0,
      do: :ok,
      else: {:error, {:validation, :interval_must_be_a_positive_integer}}
  end

  defp valid_text?(value) do
    is_binary(value) and String.trim(value) != ""
  end
end
