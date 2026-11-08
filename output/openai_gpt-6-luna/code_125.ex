# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Stock.Inventory do
  use GenServer

  require Logger

  @default_server __MODULE__

  # Public API

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def add_product(server \\ @default_server, attrs) do
    with {:ok, product} <- validate_product(attrs) do
      GenServer.call(server, {:add_product, product})
    end
  end

  def adjust_stock(server \\ @default_server, sku, delta) do
    with {:ok, sku} <- validate_sku(sku),
         true <- is_integer(delta) do
      GenServer.call(server, {:adjust_stock, sku, delta})
    else
      false -> {:error, :invalid_delta}
      error -> error
    end
  end

  def reserve(server \\ @default_server, sku, quantity, ttl_ms) do
    with {:ok, sku} <- validate_sku(sku),
         true <- is_integer(quantity) and quantity > 0,
         true <- is_integer(ttl_ms) and ttl_ms > 0 do
      GenServer.call(server, {:reserve, sku, quantity, ttl_ms})
    else
      false -> {:error, :invalid_reservation}
      error -> error
    end
  end

  def release(server \\ @default_server, reservation_id) do
    with :ok <- validate_reservation_id(reservation_id) do
      GenServer.call(server, {:release, reservation_id})
    end
  end

  def get_product(server \\ @default_server, sku) do
    with {:ok, sku} <- validate_sku(sku) do
      GenServer.call(server, {:get_product, sku})
    end
  end

  def list_products(server \\ @default_server) do
    GenServer.call(server, :list_products)
  end

  def get_reservation(server \\ @default_server, reservation_id) do
    with :ok <- validate_reservation_id(reservation_id) do
      GenServer.call(server, {:get_reservation, reservation_id})
    end
  end

  def database_status(server \\ @default_server) do
    GenServer.call(server, :database_status)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    {database_pid, database_ref} = start_database(%{})

    {:ok,
     %{
       products: %{},
       reservations: %{},
       database_pid: database_pid,
       database_ref: database_ref
     }}
  end

  @impl true
  def handle_call({:add_product, product}, _from, state) do
    state = expire_due_reservations(state)
    sku = product.sku

    if Map.has_key?(state.products, sku) do
      {:reply, {:error, :product_already_exists}, state}
    else
      state = %{state | products: Map.put(state.products, sku, product)}
      {:reply, {:ok, product}, persist(state)}
    end
  end

  def handle_call({:adjust_stock, sku, delta}, _from, state) do
    state = expire_due_reservations(state)

    case Map.fetch(state.products, sku) do
      :error ->
        {:reply, {:error, :product_not_found}, state}

      {:ok, product} when product.quantity + delta < 0 ->
        {:reply, {:error, :insufficient_stock}, state}

      {:ok, product} ->
        updated = %{product | quantity: product.quantity + delta}
        state = %{state | products: Map.put(state.products, sku, updated)}
        {:reply, {:ok, updated}, persist(state)}
    end
  end

  def handle_call({:reserve, sku, quantity, ttl_ms}, _from, state) do
    state = expire_due_reservations(state)

    case Map.fetch(state.products, sku) do
      :error ->
        {:reply, {:error, :product_not_found}, state}

      {:ok, product} when product.quantity < quantity ->
        {:reply, {:error, :insufficient_stock}, state}

      {:ok, product} ->
        id = System.unique_integer([:positive, :monotonic])
        token = make_ref()
        expires_at = System.monotonic_time(:millisecond) + ttl_ms
        timer_ref = Process.send_after(self(), {:expire_reservation, id, token}, ttl_ms)

        reservation = %{
          id: id,
          sku: sku,
          quantity: quantity,
          expires_at: expires_at,
          timer_ref: timer_ref,
          token: token
        }

        updated_product = %{product | quantity: product.quantity - quantity}

        state =
          state
          |> put_in([:products, sku], updated_product)
          |> put_in([:reservations, id], reservation)

        public_reservation = public_reservation(reservation)
        {:reply, {:ok, public_reservation}, persist(state)}
    end
  end

  def handle_call({:release, reservation_id}, _from, state) do
    state = expire_due_reservations(state)

    case Map.fetch(state.reservations, reservation_id) do
      :error ->
        {:reply, {:error, :reservation_not_found}, state}

      {:ok, reservation} ->
        state = restore_reservation(state, reservation)
        {:reply, :ok, persist(state)}
    end
  end

  def handle_call({:get_product, sku}, _from, state) do
    state = expire_due_reservations(state)
    {:reply, Map.fetch(state.products, sku), state}
  end

  def handle_call(:list_products, _from, state) do
    state = expire_due_reservations(state)
    products = state.products |> Map.values() |> Enum.sort_by(& &1.sku)
    {:reply, products, state}
  end

  def handle_call({:get_reservation, reservation_id}, _from, state) do
    state = expire_due_reservations(state)

    reply =
      case Map.fetch(state.reservations, reservation_id) do
        {:ok, reservation} -> {:ok, public_reservation(reservation)}
        :error -> {:error, :reservation_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:database_status, _from, state) do
    status = if Process.alive?(state.database_pid), do: :connected, else: :restarting
    {:reply, status, state}
  end

  @impl true
  def handle_info({:expire_reservation, id, token}, state) do
    case Map.fetch(state.reservations, id) do
      {:ok, %{token: ^token} = reservation} ->
        state = restore_reservation(state, reservation)
        {:noreply, persist(state)}

      _ ->
        # Ignore stale timer messages, for example after a reservation is released.
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %{database_ref: ref} = state) do
    Logger.warning(
      "Inventory database process #{inspect(pid)} stopped unexpectedly: #{inspect(reason)}; restarting it"
    )

    {database_pid, database_ref} = start_database(database_snapshot(state))

    state = %{state | database_pid: database_pid, database_ref: database_ref}
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}
  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.reservations, fn {_id, reservation} ->
      Process.cancel_timer(reservation.timer_ref)
    end)

    send(state.database_pid, :stop)
    :ok
  end

  # Validation

  defp validate_product(attrs) when is_map(attrs) do
    with {:ok, sku} <- fetch_and_validate_sku(attrs, :sku),
         {:ok, name} <- fetch_and_validate_name(attrs),
         {:ok, quantity} <- fetch_and_validate_quantity(attrs),
         {:ok, price} <- fetch_and_validate_price(attrs) do
      {:ok, %{sku: sku, name: name, quantity: quantity, price: price}}
    end
  end

  defp validate_product(_), do: {:error, :invalid_product}

  defp fetch_and_validate_sku(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, sku} -> validate_sku(sku)
      :error -> {:error, :invalid_sku}
    end
  end

  defp fetch_and_validate_name(attrs) do
    case Map.fetch(attrs, :name) do
      {:ok, name} when is_binary(name) ->
        name = String.trim(name)
        if name == "", do: {:error, :invalid_name}, else: {:ok, name}

      _ ->
        {:error, :invalid_name}
    end
  end

  defp fetch_and_validate_quantity(attrs) do
    case Map.fetch(attrs, :quantity) do
      {:ok, quantity} when is_integer(quantity) and quantity >= 0 -> {:ok, quantity}
      _ -> {:error, :invalid_quantity}
    end
  end

  defp fetch_and_validate_price(attrs) do
    case Map.fetch(attrs, :price) do
      {:ok, price} when is_number(price) and price >= 0 -> {:ok, price}
      _ -> {:error, :invalid_price}
    end
  end

  defp validate_sku(sku) when is_binary(sku) do
    sku = String.trim(sku)
    if sku == "", do: {:error, :invalid_sku}, else: {:ok, sku}
  end

  defp validate_sku(_), do: {:error, :invalid_sku}

  defp validate_reservation_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_reservation_id(_), do: {:error, :invalid_reservation_id}

  # Reservation expiry and persistence

  defp expire_due_reservations(state) do
    now = System.monotonic_time(:millisecond)

    due =
      state.reservations
      |> Enum.filter(fn {_id, reservation} -> reservation.expires_at <= now end)
      |> Enum.map(fn {_id, reservation} -> reservation end)

    if due == [] do
      state
    else
      state = Enum.reduce(due, state, &restore_reservation(&2, &1))
      persist(state)
    end
  end

  defp restore_reservation(state, reservation) do
    Process.cancel_timer(reservation.timer_ref)

    state =
      case Map.fetch(state.products, reservation.sku) do
        {:ok, product} ->
          updated = %{product | quantity: product.quantity + reservation.quantity}
          put_in(state, [:products, reservation.sku], updated)

        :error ->
          state
      end

    update_in(state.reservations, &Map.delete(&1, reservation.id))
  end

  defp public_reservation(reservation) do
    Map.take(reservation, [:id, :sku, :quantity, :expires_at])
  end

  defp persist(state) do
    send(state.database_pid, {:save, database_snapshot(state)})
    state
  end

  defp database_snapshot(state) do
    reservations =
      Map.new(state.reservations, fn {id, reservation} ->
        {id, Map.take(reservation, [:id, :sku, :quantity, :expires_at])}
      end)

    %{products: state.products, reservations: reservations}
  end

  # A small in-memory database stand-in. The inventory process monitors it and
  # recreates it from the current snapshot if it exits unexpectedly.
  defp start_database(snapshot) do
    spawn_monitor(fn -> database_loop(snapshot) end)
  end

  defp database_loop(snapshot) do
    receive do
      {:save, new_snapshot} ->
        database_loop(new_snapshot)

      {:read, caller, ref} ->
        send(caller, {ref, snapshot})
        database_loop(snapshot)

      :stop ->
        :ok

      _other ->
        database_loop(snapshot)
    end
  end
end
