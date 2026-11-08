# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule StoreInventory do
  use GenServer

  @moduledoc """
  Gerenciador de estoque com histórico e limite de operações simultâneas.
  """

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def add_stock(server \\ __MODULE__, sku, quantity) do
    GenServer.call(server, {:enqueue, {:add, sku, quantity}}, :infinity)
  end

  def remove_stock(server \\ __MODULE__, sku, quantity) do
    GenServer.call(server, {:enqueue, {:remove, sku, quantity}}, :infinity)
  end

  def get_stock(server \\ __MODULE__, sku) do
    GenServer.call(server, {:get_stock, sku})
  end

  def inventory(server \\ __MODULE__) do
    GenServer.call(server, :inventory)
  end

  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(opts) do
    max_concurrent = Keyword.get(opts, :max_concurrent_operations, 1)

    if is_integer(max_concurrent) and max_concurrent > 0 do
      {:ok,
       %{
         inventory: %{},
         history: [],
         sequence: 0,
         max_concurrent: max_concurrent,
         workers: %{},
         pending: :queue.new()
       }}
    else
      {:stop, {:invalid_max_concurrent_operations, max_concurrent}}
    end
  end

  @impl true
  def handle_call({:enqueue, operation}, from, state) do
    state =
      state
      |> Map.update!(:pending, &:queue.in({operation, from}, &1))
      |> dispatch()

    {:noreply, state}
  end

  def handle_call({:execute, operation}, _from, state) do
    {result, inventory, stock_after} = apply_operation(operation, state.inventory)
    sequence = state.sequence + 1

    entry = %{
      sequence: sequence,
      timestamp: System.system_time(:millisecond),
      operation: operation,
      result: result,
      stock_after: stock_after
    }

    state = %{
      state
      | inventory: inventory,
        history: [entry | state.history],
        sequence: sequence
    }

    {:reply, result, state}
  end

  def handle_call({:get_stock, sku}, _from, state) do
    if valid_sku?(sku) do
      {:reply, {:ok, Map.get(state.inventory, sku, 0)}, state}
    else
      {:reply, {:error, :invalid_sku}, state}
    end
  end

  def handle_call(:inventory, _from, state) do
    {:reply, state.inventory, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info({:operation_done, pid, result}, state) do
    case Map.pop(state.workers, pid) do
      {nil, _workers} ->
        {:noreply, state}

      {from, workers} ->
        GenServer.reply(from, result)
        {:noreply, %{state | workers: workers} |> dispatch()}
    end
  end

  defp dispatch(state) do
    if map_size(state.workers) < state.max_concurrent do
      case :queue.out(state.pending) do
        {{:value, {operation, from}}, pending} ->
          server = self()

          case Task.start(fn ->
                 result = GenServer.call(server, {:execute, operation}, :infinity)
                 send(server, {:operation_done, self(), result})
               end) do
            {:ok, pid} ->
              %{state | pending: pending, workers: Map.put(state.workers, pid, from)}
              |> dispatch()

            {:error, reason} ->
              GenServer.reply(from, {:error, {:worker_start_failed, reason}})
              %{state | pending: pending} |> dispatch()
          end

        {:empty, _pending} ->
          state
      end
    else
      state
    end
  end

  defp apply_operation({:add, sku, quantity}, inventory) do
    cond do
      not valid_sku?(sku) ->
        {{:error, :invalid_sku}, inventory, nil}

      not valid_quantity?(quantity) ->
        {{:error, :invalid_quantity}, inventory, Map.get(inventory, sku, 0)}

      true ->
        new_stock = Map.get(inventory, sku, 0) + quantity
        {{:ok, new_stock}, Map.put(inventory, sku, new_stock), new_stock}
    end
  end

  defp apply_operation({:remove, sku, quantity}, inventory) do
    cond do
      not valid_sku?(sku) ->
        {{:error, :invalid_sku}, inventory, nil}

      not valid_quantity?(quantity) ->
        {{:error, :invalid_quantity}, inventory, Map.get(inventory, sku, 0)}

      true ->
        current_stock = Map.get(inventory, sku, 0)

        if current_stock >= quantity do
          new_stock = current_stock - quantity
          {{:ok, new_stock}, Map.put(inventory, sku, new_stock), new_stock}
        else
          {{:error, :insufficient_stock}, inventory, current_stock}
        end
    end
  end

  defp apply_operation(_operation, inventory) do
    {{:error, :invalid_operation}, inventory, nil}
  end

  defp valid_sku?(sku), do: is_binary(sku) and byte_size(sku) > 0
  defp valid_quantity?(quantity), do: is_integer(quantity) and quantity > 0
end
