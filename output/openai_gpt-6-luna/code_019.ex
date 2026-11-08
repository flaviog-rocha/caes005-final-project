# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryTracking do
  @moduledoc """
  Gerenciamento de entregas e rastreamento com armazenamento fictício em memória.
  """

  @statuses [:pending, :picked_up, :in_transit, :delivered, :failed]

  def start_link(_opts \\ []) do
    DeliveryTracking.Supervisor.start_link()
  end

  def create_delivery(attrs) do
    with {:ok, delivery} <- validate_delivery(attrs),
         {:ok, stored} <- DeliveryTracking.Repo.insert(delivery),
         {:ok, _pid} <- DeliveryTracking.ensure_worker(stored.id) do
      {:ok, stored}
    end
  end

  def get_delivery(id) do
    with :ok <- validate_id(id) do
      case DeliveryTracking.Repo.get(id) do
        nil -> {:error, :not_found}
        delivery -> {:ok, delivery}
      end
    end
  end

  def list_deliveries do
    DeliveryTracking.Repo.all()
  end

  def update_status(id, status) do
    with :ok <- validate_id(id),
         :ok <- validate_status(status),
         {:ok, pid} <- DeliveryTracking.ensure_worker(id) do
      GenServer.call(pid, {:update_status, status})
    end
  end

  defp validate_delivery(attrs) when is_map(attrs) do
    allowed_keys = [:id, :recipient, :address]

    cond do
      Enum.any?(Map.keys(attrs), &(&1 not in allowed_keys)) ->
        {:error, :unknown_fields}

      not is_binary(Map.get(attrs, :recipient)) ->
        {:error, :invalid_recipient}

      not is_binary(Map.get(attrs, :address)) ->
        {:error, :invalid_address}

      true ->
        recipient = String.trim(attrs.recipient)
        address = String.trim(attrs.address)
        id = Map.get(attrs, :id, new_id())

        cond do
          recipient == "" -> {:error, :invalid_recipient}
          address == "" -> {:error, :invalid_address}
          not is_binary(id) -> {:error, :invalid_id}
          validate_id(id) != :ok -> {:error, :invalid_id}
          true -> {:ok, %{id: id, recipient: recipient, address: address}}
        end
    end
  end

  defp validate_delivery(_), do: {:error, :invalid_data}

  defp validate_id(id)
       when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= 128,
       do: :ok

  defp validate_id(_), do: {:error, :invalid_id}

  defp validate_status(status) when status in @statuses, do: :ok
  defp validate_status(_), do: {:error, :invalid_status}

  defp new_id do
    "delivery-#{System.unique_integer([:positive, :monotonic])}"
  end

  def ensure_worker(id) do
    case Registry.lookup(DeliveryTracking.Registry, id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(
               DeliveryTracking.DynamicSupervisor,
               {DeliveryTracking.Worker, id}
             ) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, _reason} = error ->
            case Registry.lookup(DeliveryTracking.Registry, id) do
              [{pid, _}] -> {:ok, pid}
              [] -> error
            end
        end
    end
  end
end

defmodule DeliveryTracking.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: DeliveryTracking.Registry},
      DeliveryTracking.Repo,
      {DynamicSupervisor,
       strategy: :one_for_one, name: DeliveryTracking.DynamicSupervisor},
      DeliveryTracking.Recovery
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule DeliveryTracking.Repo do
  use GenServer

  @storage_key {__MODULE__, :deliveries}

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def insert(delivery), do: GenServer.call(__MODULE__, {:insert, delivery})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def all, do: GenServer.call(__MODULE__, :all)
  def update(delivery), do: GenServer.call(__MODULE__, {:update, delivery})

  @impl true
  def init(:ok) do
    {:ok, :persistent_term.get(@storage_key, %{})}
  end

  @impl true
  def handle_call({:insert, delivery}, _from, deliveries) do
    if Map.has_key?(deliveries, delivery.id) do
      {:reply, {:error, :already_exists}, deliveries}
    else
      updated = Map.put(deliveries, delivery.id, delivery)
      persist(updated)
      {:reply, {:ok, delivery}, updated}
    end
  end

  def handle_call({:get, id}, _from, deliveries) do
    {:reply, Map.get(deliveries, id), deliveries}
  end

  def handle_call(:all, _from, deliveries) do
    {:reply, deliveries |> Map.values() |> Enum.sort_by(& &1.id), deliveries}
  end

  def handle_call({:update, delivery}, _from, deliveries) do
    if Map.has_key?(deliveries, delivery.id) do
      updated = Map.put(deliveries, delivery.id, delivery)
      persist(updated)
      {:reply, {:ok, delivery}, updated}
    else
      {:reply, {:error, :not_found}, deliveries}
    end
  end

  defp persist(deliveries) do
    :persistent_term.put(@storage_key, deliveries)
  end
end

defmodule DeliveryTracking.Worker do
  use GenServer

  @transitions %{
    pending: [:picked_up, :failed],
    picked_up: [:in_transit, :failed],
    in_transit: [:delivered, :failed],
    delivered: [],
    failed: []
  }

  def child_spec(id) do
    %{
      id: {:delivery_worker, id},
      start: {__MODULE__, :start_link, [id]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(id) do
    GenServer.start_link(__MODULE__, id,
      name: {:via, Registry, {DeliveryTracking.Registry, id}}
    )
  end

  @impl true
  def init(id) do
    case DeliveryTracking.Repo.get(id) do
      nil -> {:stop, :not_found}
      delivery -> {:ok, delivery}
    end
  end

  @impl true
  def handle_call({:update_status, status}, _from, delivery) do
    cond do
      status == delivery.status ->
        {:reply, {:ok, delivery}, delivery}

      status in Map.fetch!(@transitions, delivery.status) ->
        now = DateTime.utc_now()

        updated = %{
          delivery
          | status: status,
            updated_at: now,
            events: delivery.events ++ [%{status: status, at: now}]
        }

        case DeliveryTracking.Repo.update(updated) do
          {:ok, saved} -> {:reply, {:ok, saved}, saved}
          {:error, reason} -> {:reply, {:error, reason}, delivery}
        end

      true ->
        {:reply, {:error, :invalid_transition}, delivery}
    end
  end
end

defmodule DeliveryTracking.Recovery do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    send(self(), :recover)
    {:ok, :ok}
  end

  @impl true
  def handle_info(:recover, state) do
    DeliveryTracking.Repo.all()
    |> Enum.each(fn delivery ->
      DeliveryTracking.ensure_worker(delivery.id)
    end)

    {:noreply, state}
  end
end\Extension final
