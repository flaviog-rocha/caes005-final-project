# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.Orders do
  use GenServer

  @table :ecommerce_orders_dets
  @statuses [:pending, :paid, :shipped, :cancelled]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def stop do
    GenServer.stop(__MODULE__)
  end

  def create(attrs) do
    with {:ok, attrs} <- validate_order(attrs) do
      safe_operation(fn ->
        now = DateTime.utc_now()

        order = %{
          id: generate_id(),
          customer_id: attrs.customer_id,
          items: attrs.items,
          total: calculate_total(attrs.items),
          status: :pending,
          created_at: now,
          updated_at: now
        }

        case :dets.insert(@table, {order.id, order}) do
          :ok ->
            sync_table()
            {:ok, order}

          {:error, reason} ->
            operational_error(reason)
        end
      end)
    end
  end

  def get(id) do
    with :ok <- validate_id(id) do
      safe_operation(fn ->
        case :dets.lookup(@table, id) do
          [{^id, order}] -> {:ok, order}
          [] -> validation_error(:not_found)
        end
      end)
    end
  end

  def list do
    safe_operation(fn ->
      orders =
        :dets.foldl(
          fn {_id, order}, acc -> [order | acc] end,
          [],
          @table
        )
        |> Enum.sort_by(& &1.id)

      {:ok, orders}
    end)
  end

  def update_status(id, status) do
    with :ok <- validate_id(id),
         :ok <- validate_status(status) do
      safe_operation(fn ->
        case :global.trans({{__MODULE__, id}, self()}, fn ->
               case :dets.lookup(@table, id) do
                 [{^id, order}] ->
                   updated = %{order | status: status, updated_at: DateTime.utc_now()}

                   case :dets.insert(@table, {id, updated}) do
                     :ok ->
                       sync_table()
                       {:ok, updated}

                     {:error, reason} ->
                       operational_error(reason)
                   end

                 [] ->
                   validation_error(:not_found)
               end
             end) do
          :aborted -> operational_error(:lock_aborted)
          result -> result
        end
      end)
    end
  end

  def delete(id) do
    with :ok <- validate_id(id) do
      safe_operation(fn ->
        case :global.trans({{__MODULE__, id}, self()}, fn ->
               case :dets.lookup(@table, id) do
                 [{^id, _order}] ->
                   case :dets.delete(@table, id) do
                     :ok ->
                       sync_table()
                       {:ok, :deleted}

                     {:error, reason} ->
                       operational_error(reason)
                   end

                 [] ->
                   validation_error(:not_found)
               end
             end) do
          :aborted -> operational_error(:lock_aborted)
          result -> result
        end
      end)
    end
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:path, "orders.dets")
      |> Path.expand()

    case File.mkdir_p(Path.dirname(path)) do
      :ok ->
        case :dets.open_file(@table,
               file: String.to_charlist(path),
               type: :set,
               repair: true
             ) do
          {:ok, @table} -> {:ok, %{path: path}}
          {:error, reason} -> {:stop, {:operational, reason}}
        end

      {:error, reason} ->
        {:stop, {:operational, reason}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    case :dets.info(@table) do
      :undefined ->
        :ok

      _ ->
        :dets.sync(@table)
        :dets.close(@table)
    end
  end

  defp validate_order(attrs) when is_map(attrs) do
    customer_id = Map.get(attrs, :customer_id)
    items = Map.get(attrs, :items)

    cond do
      not valid_text?(customer_id) ->
        validation_error(:invalid_customer_id)

      not is_list(items) or items == [] ->
        validation_error(:invalid_items)

      not Enum.all?(items, &valid_item?/1) ->
        validation_error(:invalid_item)

      true ->
        {:ok, %{customer_id: customer_id, items: items}}
    end
  end

  defp validate_order(_), do: validation_error(:invalid_order)

  defp valid_item?(item) when is_map(item) do
    product_id = Map.get(item, :product_id)
    quantity = Map.get(item, :quantity)
    unit_price = Map.get(item, :unit_price)

    valid_text?(product_id) and
      is_integer(quantity) and quantity > 0 and
      is_number(unit_price) and unit_price >= 0
  end

  defp valid_item?(_), do: false

  defp valid_text?(value), do: is_binary(value) and String.trim(value) != ""

  defp validate_id(id) do
    if valid_text?(id), do: :ok, else: validation_error(:invalid_id)
  end

  defp validate_status(status) do
    if status in @statuses, do: :ok, else: validation_error(:invalid_status)
  end

  defp calculate_total(items) do
    Enum.reduce(items, 0, fn item, total ->
      total + item.quantity * item.unit_price
    end)
  end

  defp generate_id do
    unique = System.unique_integer([:positive, :monotonic])
    timestamp = System.system_time(:nanosecond)
    "#{node()}-#{timestamp}-#{unique}"
  end

  defp sync_table do
    case :dets.sync(@table) do
      :ok -> :ok
      {:error, reason} -> throw({:operational, reason})
    end
  end

  defp safe_operation(fun) do
    try do
      fun.()
    rescue
      error ->
        operational_error({error.__struct__, Exception.message(error)})
    catch
      :throw, {:operational, reason} ->
        operational_error(reason)

      kind, reason ->
        operational_error({kind, reason})
    end
  end

  defp validation_error(reason), do: {:error, {:validation, reason}}
  defp operational_error(reason), do: {:error, {:operational, reason}}
end
