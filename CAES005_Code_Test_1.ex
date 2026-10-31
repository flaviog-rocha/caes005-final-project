defmodule SimpleStore do
  use Plug.Router

  # Start in-memory store using Agent
  def start do
    {:ok, _} = Agent.start_link(fn -> %{products: %{}, orders: %{}} end, name: __MODULE__)
    {:ok, _} = Plug.Cowboy.http(__MODULE__, [])
  end

  plug(:match)
  plug(:dispatch)

  # List all products
  get "/products" do
    products = Agent.get(__MODULE__, & &1.products) |> Map.values()
    send_resp(conn, 200, Jason.encode!(products))
  end

  # Get a product by id
  get "/products/:id" do
    id = conn.params["id"]
    product = Agent.get(__MODULE__, fn state -> Map.get(state.products, id) end)
    if product do
      send_resp(conn, 200, Jason.encode!(product))
    else
      send_resp(conn, 404, Jason.encode!(%{error: "Product not found"}))
    end
  end

  # Create a product
  post "/products" do
    {:ok, body, _conn} = Plug.Conn.read_body(conn)
    case Jason.decode(body) do
      {:ok, %{"id" => id} = product} ->
        Agent.update(__MODULE__, fn state ->
          %{state | products: Map.put(state.products, id, product)}
        end)
        send_resp(conn, 201, Jason.encode!(product))
      _ ->
        send_resp(conn, 400, Jason.encode!(%{error: "Invalid product data"}))
    end
  end

  # Update a product
  put "/products/:id" do
    id = conn.params["id"]
    {:ok, body, _conn} = Plug.Conn.read_body(conn)
    case Jason.decode(body) do
      {:ok, updated_product} ->
        if Agent.get(__MODULE__, fn state -> Map.has_key?(state.products, id) end) do
          Agent.update(__MODULE__, fn state ->
            %{state | products: Map.put(state.products, id, Map.put(updated_product, "id", id))}
          end)
          send_resp(conn, 200, Jason.encode!(Map.put(updated_product, "id", id)))
        else
          send_resp(conn, 404, Jason.encode!(%{error: "Product not found"}))
        end
      _ ->
        send_resp(conn, 400, Jason.encode!(%{error: "Invalid product data"}))
    end
  end

  # Delete a product
  delete "/products/:id" do
    id = conn.params["id"]
    if Agent.get(__MODULE__, fn state -> Map.has_key?(state.products, id) end) do
      Agent.update(__MODULE__, fn state ->
        %{state | products: Map.delete(state.products, id)}
      end)
      send_resp(conn, 204, "")
    else
      send_resp(conn, 404, Jason.encode!(%{error: "Product not found"}))
    end
  end

  # List all orders
  get "/orders" do
    orders = Agent.get(__MODULE__, & &1.orders) |> Map.values()
    send_resp(conn, 200, Jason.encode!(orders))
  end

  # Fallback route
  match _ do
    send_resp(conn, 404, Jason.encode!(%{error: "Route not found"}))
  end
end

SimpleStore.start()