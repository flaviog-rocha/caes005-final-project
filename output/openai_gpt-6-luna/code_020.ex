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

defmodule Delivery.Validation do
  @statuses [:created, :picked_up, :in_transit, :delivered, :failed]

  def delivery(attrs) when is_map(attrs) do
    with :ok <- text_field(attrs, :id, 100),
         :ok <- text_field(attrs, :recipient, 200),
         :ok <- text_field(attrs, :address, 500) do
      :ok
    end
  end

  def delivery(_), do: {:error, :invalid_delivery}

  def id(value), do: validate_text(value, 100, :id)
  def status(value) when value in @statuses, do: :ok
  def status(_), do: {:error, :invalid_status}

  defp text_field(attrs, key, max_length) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> validate_text(value, max_length, key)
      :error -> {:error, {:missing_field, key}}
    end
  end

  defp validate_text(value, max_length, field)
       when is_binary(value) and byte_size(value) > 0 and byte_size(value) <= max_length do
    if String.trim(value) == "" do
      {:error, {:invalid_field, field}}
    else
      :ok
    end
  end

  defp validate_text(_, _, field), do: {:error, {:invalid_field, field}}
end

defmodule Delivery.Store do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  def create(record), do: GenServer.call(__MODULE__, {:create, record})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def put(record), do: GenServer.call(__MODULE__, {:put, record})

  @impl true
  def init(:ok) do
    {:ok, %{records: %{}}}
  end

  @impl true
  def handle_call({:create, %{id: id} = record}, _from, state) do
    with :ok <- Delivery.Validation.id(id) do
      if Map.has_key?(state.records, id) do
        {:reply, {:error, :already_exists}, state}
      else
        {:reply, {:ok, record}, put_in(state.records[id], record)}
      end
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:create, _record}, _from, state),
    do: {:reply, {:error, :invalid_delivery}, state}

  def handle_call({:get, id}, _from, state) do
    case Delivery.Validation.id(id) do
      :ok ->
        case Map.fetch(state.records, id) do
          {:ok, record} -> {:reply, {:ok, record}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:put, %{id: id} = record}, _from, state) do
    with :ok <- Delivery.Validation.id(id),
         true <- Map.has_key?(state.records, id) do
      {:reply, {:ok, record}, put_in(state.records[id], record)}
    else
      false -> {:reply, {:error, :not_found}, state}
      error -> {:reply, error, state}
    end
  end

  def handle_call({:put, _record}, _from, state),
    do: {:reply, {:error, :invalid_delivery}, state}
end

defmodule Delivery.Worker do
  use GenServer

  @transitions %{
    created: [:picked_up, :failed],
    picked_up: [:in_transit, :failed],
    in_transit: [:delivered, :failed],
    delivered: [],
    failed: []
  }

  def start_link(id) do
    GenServer.start_link(__MODULE__, id,
      name: {:via, Registry, {Delivery.Registry, id}}
    )
  end

  def child_spec(id) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [id]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init(id) do
    case Delivery.Store.get(id) do
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:snapshot, _from, record), do: {:reply, {:ok, record}, record}

  def handle_call({:update_status, status}, _from, record) do
    with :ok <- Delivery.Validation.status(status),
         true <- status in Map.get(@transitions, record.status, []) do
      updated = %{
        record
        | status: status,
          history: record.history ++ [%{status: status, at: DateTime.utc_now()}]
      }

      case Delivery.Store.put(updated) do
        {:ok, _} -> {:reply, {:ok, updated}, updated}
        error -> {:reply, error, record}
      end
    else
      false -> {:reply, {:error, :invalid_status_transition}, record}
      error -> {:reply, error, record}
    end
  end
end

defmodule Delivery.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      Delivery.Store,
      {Registry, keys: :unique, name: Delivery.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Delivery.DynamicSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Delivery do
  def start_link(opts \\ []), do: Delivery.Supervisor.start_link(opts)

  def create_delivery(attrs) do
    with :ok <- Delivery.Validation.delivery(attrs),
         record <- new_record(attrs),
         {:ok, _record} <- Delivery.Store.create(record),
         {:ok, _pid} <- ensure_worker(record.id) do
      {:ok, record}
    end
  end

  def track(id) do
    with :ok <- Delivery.Validation.id(id),
         {:ok, _pid} <- ensure_worker(id) do
      safe_call(id, :snapshot)
    end
  end

  def update_status(id, status) do
    with :ok <- Delivery.Validation.id(id),
         :ok <- Delivery.Validation.status(status),
         {:ok, _pid} <- ensure_worker(id) do
      safe_call(id, {:update_status, status})
    end
  end

  defp new_record(attrs) do
    %{
      id: Map.fetch!(attrs, :id),
      recipient: Map.fetch!(attrs, :recipient),
      address: Map.fetch!(attrs, :address),
      status: :created,
      history: [%{status: :created, at: DateTime.utc_now()}]
    }
  end

  defp ensure_worker(id) do
    case lookup_worker(id) do
      {:ok, pid} ->
        {:ok, pid}

      :not_found ->
        try do
          case DynamicSupervisor.start_child(
                 Delivery.DynamicSupervisor,
                 {Delivery.Worker, id}
               ) do
            {:ok, pid} -> {:ok, pid}
            {:error, _reason} -> lookup_after_start(id)
          end
        catch
          :exit, reason -> {:error, {:supervisor_unavailable, reason}}
        end
    end
  end

  defp lookup_after_start(id) do
    case lookup_worker(id) do
      {:ok, pid} -> {:ok, pid}
      :not_found -> {:error, :worker_unavailable}
    end
  end

  defp lookup_worker(id) do
    try do
      case Registry.lookup(Delivery.Registry, id) do
        [{pid, _value}] -> {:ok, pid}
        [] -> :not_found
      end
    catch
      :exit, _reason -> :not_found
    end
  end

  defp safe_call(id, message) do
    try do
      GenServer.call({:via, Registry, {Delivery.Registry, id}}, message)
    catch
      :exit, reason -> {:error, {:worker_unavailable, reason}}
    end
  end
end
