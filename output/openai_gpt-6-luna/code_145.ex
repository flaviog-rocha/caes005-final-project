# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.OrderSystem do
  @moduledoc """
  Gerenciador de pedidos com um processo independente por pedido e persistência em disco.

  As notificações são enviadas aos processos inscritos como
  `{:order_notification, evento}` quando um pedido muda para `:paid`,
  `:shipped`, `:delivered` ou `:cancelled`.
  """

  use Supervisor

  alias Ecommerce.OrderSystem.{Manager, NotificationHub, Worker}

  @registry Ecommerce.OrderSystem.Registry
  @manager Manager

  @statuses [:pending, :paid, :processing, :shipped, :delivered, :cancelled]

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_dir =
      opts
      |> Keyword.get(:data_dir, Path.join(File.cwd!(), "data/orders"))
      |> Path.expand()

    children = [
      {Registry, keys: :unique, name: @registry},
      {NotificationHub, []},
      {DynamicSupervisor, strategy: :one_for_one, name: Worker.Supervisor},
      {Manager, data_dir: data_dir}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_order(attrs) when is_map(attrs) do
    with {:ok, order} <- validate_new_order(attrs) do
      GenServer.call(@manager, {:create, order})
    end
  end

  def create_order(_), do: {:error, :invalid_order}

  def get_order(id) do
    with :ok <- validate_id(id),
         {:ok, pid} <- lookup_worker(id) do
      GenServer.call(pid, :get_order)
    end
  end

  def update_status(id, status) do
    with :ok <- validate_id(id),
         {:ok, status} <- normalize_status(status),
         {:ok, pid} <- lookup_worker(id) do
      GenServer.call(pid, {:update_status, status})
    end
  end

  def list_orders do
    ids = GenServer.call(@manager, :order_ids)

    Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, orders} ->
      case get_order(id) do
        {:ok, order} -> {:cont, {:ok, [order | orders]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, orders} -> {:ok, Enum.reverse(orders)}
      error -> error
    end
  end

  def subscribe do
    GenServer.call(NotificationHub, {:subscribe, self()})
  end

  def unsubscribe do
    GenServer.call(NotificationHub, {:unsubscribe, self()})
  end

  defp lookup_worker(id) do
    case Registry.lookup(@registry, id) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end

  defp validate_new_order(attrs) do
    id = field(attrs, :id)
    customer_id = field(attrs, :customer_id)
    items = field(attrs, :items)

    with :ok <- validate_id(id),
         :ok <- validate_customer_id(customer_id),
         {:ok, validated_items, total} <- validate_items(items) do
      {:ok,
       %{
         id: id,
         customer_id: customer_id,
         items: validated_items,
         total_cents: total,
         status: :pending,
         created_at: System.system_time(:millisecond),
         updated_at: System.system_time(:millisecond)
       }}
    end
  end

  defp validate_customer_id(value)
       when is_binary(value) and byte_size(value) > 0 and byte_size(value) <= 128 do
    if String.trim(value) == "" do
      {:error, :invalid_customer_id}
    else
      :ok
    end
  end

  defp validate_customer_id(_), do: {:error, :invalid_customer_id}

  defp validate_items(items) when is_list(items) and length(items) in 1..100 do
    Enum.reduce_while(items, {:ok, [], 0}, fn item, {:ok, acc, total} ->
      case validate_item(item) do
        {:ok, validated_item} ->
          item_total = validated_item.quantity * validated_item.unit_price_cents
          {:cont, {:ok, [validated_item | acc], total + item_total}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, validated_items, total} ->
        {:ok, Enum.reverse(validated_items), total}

      error ->
        error
    end
  end

  defp validate_items(_), do: {:error, :invalid_items}

  defp validate_item(item) when is_map(item) do
    sku = field(item, :sku)
    quantity = field(item, :quantity)
    unit_price_cents = field(item, :unit_price_cents)

    if is_binary(sku) and String.trim(sku) != "" and byte_size(sku) <= 100 and
         is_integer(quantity) and quantity in 1..1_000_000 and
         is_integer(unit_price_cents) and unit_price_cents in 0..1_000_000_000_000 do
      {:ok,
       %{
         sku: String.trim(sku),
         quantity: quantity,
         unit_price_cents: unit_price_cents
       }}
    else
      {:error, :invalid_item}
    end
  end

  defp validate_item(_), do: {:error, :invalid_item}

  defp validate_id(id) when is_binary(id) do
    if Regex.match?(~r/\A[a-zA-Z0-9_-]{1,64}\z/, id) do
      :ok
    else
      {:error, :invalid_id}
    end
  end

  defp validate_id(_), do: {:error, :invalid_id}

  defp normalize_status(status) when status in @statuses, do: {:ok, status}

  defp normalize_status(status) when is_binary(status) do
    case Enum.find(@statuses, &(Atom.to_string(&1) == status)) do
      nil -> {:error, :invalid_status}
      value -> {:ok, value}
    end
  end

  defp normalize_status(_), do: {:error, :invalid_status}

  defp field(map, key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key)))
  end

  defp valid_stored_order?(order) when is_map(order) do
    with :ok <- validate_id(Map.get(order, :id)),
         :ok <- validate_customer_id(Map.get(order, :customer_id)),
         {:ok, items, total} <- validate_items(Map.get(order, :items)),
         {:ok, status} <- normalize_status(Map.get(order, :status)) do
      items == Map.get(order, :items) and
        total == Map.get(order, :total_cents) and
        status == Map.get(order, :status) and
        is_integer(Map.get(order, :created_at)) and
        is_integer(Map.get(order, :updated_at))
    else
      _ -> false
    end
  end

  defp valid_stored_order?(_), do: false

  defmodule Manager do
    use GenServer

    alias Ecommerce.OrderSystem.Worker

    @registry Ecommerce.OrderSystem.Registry

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(opts) do
      data_dir = Keyword.fetch!(opts, :data_dir)
      File.mkdir_p!(data_dir)

      order_ids =
        data_dir
        |> Path.join("*.order")
        |> Path.wildcard()
        |> Enum.reduce(MapSet.new(), fn path, ids ->
          case load_order(path) do
            {:ok, order} ->
              case start_worker(order, data_dir) do
                {:ok, _pid} -> MapSet.put(ids, order.id)
                _ -> ids
              end

            :error ->
              ids
          end
        end)

      {:ok, %{data_dir: data_dir, order_ids: order_ids}}
    end

    @impl true
    def handle_call(:order_ids, _from, state) do
      {:reply, MapSet.to_list(state.order_ids), state}
    end

    def handle_call({:create, order}, _from, state) do
      if MapSet.member?(state.order_ids, order.id) or Registry.lookup(@registry, order.id) != [] do
        {:reply, {:error, :already_exists}, state}
      else
        case start_worker(order, state.data_dir) do
          {:ok, _pid} ->
            {:reply, {:ok, order}, %{state | order_ids: MapSet.put(state.order_ids, order.id)}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
      end
    end

    defp start_worker(order, data_dir) do
      DynamicSupervisor.start_child(
        Worker.Supervisor,
        {Worker, order: order, data_dir: data_dir}
      )
    end

    defp load_order(path) do
      with {:ok, binary} <- File.read(path),
           {:ok, order} <- decode_order(binary),
           true <- Worker.valid_stored_order?(order),
           true <- Path.rootname(Path.basename(path)) == order.id do
        {:ok, order}
      else
        _ -> :error
      end
    rescue
      _ -> :error
    end

    defp decode_order(binary) do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    rescue
      _ -> :error
    end
  end

  defmodule Worker do
    use GenServer

    @registry Ecommerce.OrderSystem.Registry
    @hub Ecommerce.OrderSystem.NotificationHub

    def start_link(opts) do
      order = Keyword.fetch!(opts, :order)

      GenServer.start_link(__MODULE__, opts,
        name: {:via, Registry, {@registry, order.id}}
      )
    end

    @impl true
    def init(opts) do
      state = %{
        order: Keyword.fetch!(opts, :order),
        data_dir: Keyword.fetch!(opts, :data_dir)
      }

      case persist(state) do
        :ok -> {:ok, state}
        {:error, reason} -> {:stop, reason}
      end
    end

    @impl true
    def handle_call(:get_order, _from, state) do
      {:reply, {:ok, state.order}, state}
    end

    def handle_call({:update_status, status}, _from, state) do
      current_status = state.order.status

      cond do
        status == current_status ->
          {:reply, {:ok, state.order}, state}

        not allowed_transition?(current_status, status) ->
          {:reply, {:error, :invalid_transition}, state}

        true ->
          updated_order = %{
            state.order
            | status: status,
              updated_at: System.system_time(:millisecond)
          }

          updated_state = %{state | order: updated_order}

          case persist(updated_state) do
            :ok ->
              if status in [:paid, :shipped, :delivered, :cancelled] do
                GenServer.cast(@hub, {:notify, notification(updated_order)})
              end

              {:reply, {:ok, updated_order}, updated_state}

            {:error, reason} ->
              {:reply, {:error, {:persistence_failed, reason}}, state}
          end
      end
    end

    def valid_stored_order?(order), do: Ecommerce.OrderSystem.send(:valid_stored_order?, [order])

    defp allowed_transition?(:pending, status), do: status in [:paid, :cancelled]
    defp allowed_transition?(:paid, status), do: status in [:processing, :cancelled]
    defp allowed_transition?(:processing, status), do: status in [:shipped, :cancelled]
    defp allowed_transition?(:shipped, :delivered), do: true
    defp allowed_transition?(_, _), do: false

    defp notification(order) do
      %{
        order_id: order.id,
        customer_id: order.customer_id,
        status: order.status,
        total_cents: order.total_cents,
        occurred_at: order.updated_at
      }
    end

    defp persist(state) do
      with :ok <- File.mkdir_p(state.data_dir),
           {:ok, binary} <- encode(state.order),
           :ok <- write_atomically(state.data_dir, state.order.id, binary) do
        :ok
      end
    end

    defp encode(order) do
      {:ok, :erlang.term_to_binary(order)}
    rescue
      error -> {:error, error}
    end

    defp write_atomically(data_dir, id, binary) do
      path = Path.join(data_dir, id <> ".order")
      temporary_path = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

      case File.write(temporary_path, binary, [:binary, :sync]) do
        :ok ->
          case File.rename(temporary_path, path) do
            :ok -> :ok
            {:error, reason} ->
              File.rm(temporary_path)
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defmodule NotificationHub do
    use GenServer

    def start_link(_opts) do
      GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
    end

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:subscribe, pid}, _from, subscribers) do
      case Map.fetch(subscribers, pid) do
        {:ok, _ref} ->
          {:reply, :ok, subscribers}

        :error ->
          ref = Process.monitor(pid)
          {:reply, :ok, Map.put(subscribers, pid, ref)}
      end
    end

    def handle_call({:unsubscribe, pid}, _from, subscribers) do
      case Map.pop(subscribers, pid) do
        {nil, subscribers} ->
          {:reply, :ok, subscribers}

        {ref, subscribers} ->
          Process.demonitor(ref, [:flush])
          {:reply, :ok, subscribers}
      end
    end

    @impl true
    def handle_cast({:notify, event}, subscribers) do
      Enum.each(Map.keys(subscribers), fn pid ->
        send(pid, {:order_notification, event})
      end)

      {:noreply, subscribers}
    end

    @impl true
    def handle_info({:DOWN, ref, :process, pid, _reason}, subscribers) do
      case Map.get(subscribers, pid) do
        ^ref -> {:noreply, Map.delete(subscribers, pid)}
        _ -> {:noreply, subscribers}
      end
    end
  end
end
