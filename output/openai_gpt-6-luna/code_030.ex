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

defmodule DeliveryTracking do
  @moduledoc """
  Gerenciamento de entregas com armazenamento em memória e processos de rastreamento.
  """

  alias DeliveryTracking.{Store, Worker}

  def start_link(opts \\ []) do
    DeliveryTracking.Supervisor.start_link(opts)
  end

  def create_delivery(attrs) when is_map(attrs) do
    with {:ok, delivery} <- validate_delivery(attrs),
         {:ok, saved_delivery} <- safe_call(fn -> Store.create(delivery) end),
         :ok <- ensure_worker(saved_delivery.id) do
      {:ok, saved_delivery}
    end
  end

  def create_delivery(_attrs), do: {:error, {:validation, :expected_map}}

  def get_delivery(id) do
    with :ok <- validate_id(id),
         {:ok, delivery} <- safe_call(fn -> Store.get(id) end) do
      case delivery do
        nil -> {:error, {:validation, :delivery_not_found}}
        delivery -> {:ok, delivery}
      end
    end
  end

  def list_deliveries do
    safe_call(fn -> Store.list() end)
  end

  def update_status(id, status) do
    with :ok <- validate_id(id),
         {:ok, normalized_status} <- validate_status(status) do
      safe_call(fn -> Store.update_status(id, normalized_status) end)
    end
  end

  defp ensure_worker(id) do
    child = {Worker, id: id}

    case safe_call(fn ->
           DynamicSupervisor.start_child(DeliveryTracking.WorkerSupervisor, child)
         end) do
      {:ok, _pid} ->
        :ok

      {:ok, _pid, _info} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, {:already_present, _id}} ->
        :ok

      {:error, reason} ->
        {:error, {:operation, {:worker_start_failed, reason}}}

      other ->
        {:error, {:operation, {:unexpected_worker_result, other}}}
    end
  end

  defp validate_delivery(attrs) do
    with {:ok, id} <- required_string(attrs, :id),
         {:ok, origin} <- required_string(attrs, :origin),
         {:ok, destination} <- required_string(attrs, :destination),
         :ok <- validate_id(id) do
      {:ok,
       %{
         id: id,
         origin: origin,
         destination: destination,
         status: :pending,
         inserted_at: DateTime.utc_now(),
         last_checked_at: nil
       }}
    end
  end

  defp required_string(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) ->
        value = String.trim(value)

        if value == "" do
          {:error, {:validation, {:blank_field, key}}}
        else
          {:ok, value}
        end

      _ ->
        {:error, {:validation, {:required_string, key}}}
    end
  end

  defp validate_id(id) when is_binary(id) do
    if String.trim(id) != "" and byte_size(id) <= 128 do
      :ok
    else
      {:error, {:validation, :invalid_id}}
    end
  end

  defp validate_id(_id), do: {:error, {:validation, :invalid_id}}

  defp validate_status(status) when status in [:pending, :in_transit, :delivered],
    do: {:ok, status}

  defp validate_status(status) when status in ["pending", "in_transit", "delivered"],
    do: {:ok, String.to_existing_atom(status)}

  defp validate_status(_status), do: {:error, {:validation, :invalid_status}}

  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error ->
        {:error, {:operation, {:exception, error}}}
    catch
      :exit, reason ->
        {:error, {:operation, {:process_unavailable, reason}}}
    end
  end
end

defmodule DeliveryTracking.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: DeliveryTracking.Registry},
      DeliveryTracking.Store,
      {DynamicSupervisor,
       strategy: :one_for_one, name: DeliveryTracking.WorkerSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule DeliveryTracking.Store do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def create(delivery), do: GenServer.call(__MODULE__, {:create, delivery})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list, do: GenServer.call(__MODULE__, :list)
  def update_status(id, status), do: GenServer.call(__MODULE__, {:update_status, id, status})
  def mark_checked(id), do: GenServer.call(__MODULE__, {:mark_checked, id})

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:create, delivery}, _from, state) do
    if Map.has_key?(state, delivery.id) do
      {:reply, {:error, {:validation, :id_already_exists}}, state}
    else
      {:reply, {:ok, delivery}, Map.put(state, delivery.id, delivery)}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state, id), state}
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state), state}
  end

  def handle_call({:update_status, id, status}, _from, state) do
    case Map.fetch(state, id) do
      :error ->
        {:reply, {:error, {:validation, :delivery_not_found}}, state}

      {:ok, delivery} ->
        if valid_transition?(delivery.status, status) do
          updated = %{delivery | status: status}
          {:reply, {:ok, updated}, Map.put(state, id, updated)}
        else
          {:reply, {:error, {:validation, :invalid_status_transition}}, state}
        end
    end
  end

  def handle_call({:mark_checked, id}, _from, state) do
    case Map.fetch(state, id) do
      :error ->
        {:reply, {:error, {:validation, :delivery_not_found}}, state}

      {:ok, delivery} ->
        updated = %{delivery | last_checked_at: DateTime.utc_now()}
        {:reply, :ok, Map.put(state, id, updated)}
    end
  end

  defp valid_transition?(status, status), do: true
  defp valid_transition?(:pending, :in_transit), do: true
  defp valid_transition?(:in_transit, :delivered), do: true
  defp valid_transition?(_current, _next), do: false
end

defmodule DeliveryTracking.Worker do
  use GenServer

  @refresh_interval 30_000

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)

    GenServer.start_link(__MODULE__, id,
      name: {:via, Registry, {DeliveryTracking.Registry, id}}
    )
  end

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)

    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init(id) do
    case DeliveryTracking.Store.get(id) do
      nil ->
        {:stop, :delivery_not_found}

      _delivery ->
        schedule_refresh()
        {:ok, id}
    end
  end

  @impl true
  def handle_info(:refresh, id) do
    case DeliveryTracking.Store.mark_checked(id) do
      :ok ->
        schedule_refresh()
        {:noreply, id}

      {:error, reason} ->
        {:stop, {:refresh_failed, reason}, id}
    end
  end

  defp schedule_refresh do
    Process.send_after(self(), :refresh, @refresh_interval)
  end
end
