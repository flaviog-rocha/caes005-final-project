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

  # A single owner serializes writes while allowing callers to issue operations concurrently.
  # DETS persists each successful mutation before the operation returns.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def stop do
    try do
      GenServer.stop(__MODULE__)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  def create_order(attrs) do
    with :ok <- validate_order(attrs) do
      call({:create_order, attrs})
    else
      {:error, reason} -> {:error, {:validation, reason}}
    end
  end

  def get_order(id) do
    with :ok <- validate_id(id) do
      call({:get_order, id})
    else
      {:error, reason} -> {:error, {:validation, reason}}
    end
  end

  def update_order(id, changes) do
    with :ok <- validate_id(id),
         true <- is_map(changes) or {:error, :changes_must_be_a_map},
         false <- Map.has_key?(changes, :id) and changes.id != id or
                    {:error, :order_id_cannot_be_changed} do
      call({:update_order, id, changes})
    else
      {:error, reason} -> {:error, {:validation, reason}}
      false -> {:error, {:validation, :order_id_cannot_be_changed}}
    end
  end

  def delete_order(id) do
    with :ok <- validate_id(id) do
      call({:delete_order, id})
    else
      {:error, reason} -> {:error, {:validation, reason}}
    end
  end

  def list_orders do
    call(:list_orders)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :file, "orders.dets")
    directory = Path.dirname(path)

    with :ok <- File.mkdir_p(directory),
         {:ok, table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             repair: true
           ) do
      {:ok, table}
    else
      {:error, reason} -> {:stop, {:operational, reason}}
    end
  end

  @impl true
  def handle_call({:create_order, attrs}, _from, table) do
    id = attrs.id

    reply =
      safe_dets(fn ->
        case :dets.lookup(table, id) do
          [] ->
            timestamp = System.system_time(:millisecond)

            order =
              attrs
              |> Map.put_new(:created_at, timestamp)
              |> Map.put(:updated_at, timestamp)

            persist(table, order)

          [_existing] ->
            {:error, {:already_exists, id}}
        end
      end)

    {:reply, reply, table}
  end

  def handle_call({:get_order, id}, _from, table) do
    reply =
      safe_dets(fn ->
        case :dets.lookup(table, id) do
          [{^id, order}] -> {:ok, order}
          [] -> {:error, {:not_found, id}}
        end
      end)

    {:reply, reply, table}
  end

  def handle_call({:update_order, id, changes}, _from, table) do
    reply =
      safe_dets(fn ->
        case :dets.lookup(table, id) do
          [{^id, existing}] ->
            order =
              existing
              |> Map.merge(changes)
              |> Map.put(:id, id)
              |> Map.put(:updated_at, System.system_time(:millisecond))

            case validate_order(order) do
              :ok -> persist(table, order)
              {:error, reason} -> {:error, {:validation, reason}}
            end

          [] ->
            {:error, {:not_found, id}}
        end
      end)

    {:reply, reply, table}
  end

  def handle_call({:delete_order, id}, _from, table) do
    reply =
      safe_dets(fn ->
        case :dets.lookup(table, id) do
          [{^id, _order}] ->
            case :dets.delete(table, id) do
              :ok ->
                case :dets.sync(table) do
                  :ok -> {:ok, :deleted}
                  error -> error
                end

              error ->
                error
            end

          [] ->
            {:error, {:not_found, id}}
        end
      end)

    {:reply, reply, table}
  end

  def handle_call(:list_orders, _from, table) do
    reply =
      safe_dets(fn ->
        orders = :dets.foldl(fn {_id, order}, acc -> [order | acc] end, [], table)
        {:ok, Enum.sort_by(orders, & &1.id)}
      end)

    {:reply, reply, table}
  end

  @impl true
  def terminate(_reason, table) do
    try do
      :dets.close(table)
    catch
      _, _ -> :ok
    end
  end

  defp persist(table, order) do
    case :dets.insert(table, {order.id, order}) do
      :ok ->
        case :dets.sync(table) do
          :ok -> {:ok, order}
          error -> error
        end

      error ->
        error
    end
  end

  defp safe_dets(fun) do
    try do
      case fun.() do
        {:error, {:validation, _reason}} = error -> error
        {:error, {:not_found, _id}} = error -> error
        {:error, {:already_exists, _id}} = error -> error
        {:error, reason} -> {:error, {:operational, reason}}
        {:ok, _value} = result -> result
        value -> {:ok, value}
      end
    rescue
      exception ->
        {:error, {:operational, {exception.__struct__, Exception.message(exception)}}}
    catch
      kind, reason ->
        {:error, {:operational, {kind, reason}}}
    end
  end

  defp call(message) do
    try do
      GenServer.call(__MODULE__, message)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp validate_order(attrs) when not is_map(attrs),
    do: {:error, :order_must_be_a_map}

  defp validate_order(attrs) do
    with :ok <- validate_id(Map.get(attrs, :id)),
         true <- not is_nil(Map.get(attrs, :customer_id)) or {:error, :customer_id_is_required},
         true <- is_list(Map.get(attrs, :items)) or {:error, :items_must_be_a_list},
         total when is_number(total) and total >= 0 <- Map.get(attrs, :total) do
      :ok
    else
      {:error, reason} -> {:error, reason}
      nil -> {:error, :total_must_be_a_non_negative_number}
      _ -> {:error, :total_must_be_a_non_negative_number}
    end
  end

  defp validate_id(id) when is_integer(id), do: :ok
  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_id), do: {:error, :invalid_order_id}
end
