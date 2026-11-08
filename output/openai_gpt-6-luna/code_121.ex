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
  Gerencia o estoque de uma loja com histórico e limite de operações simultâneas.
  """

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def add_stock(server \\ __MODULE__, product, quantity) do
    operate(server, {:add, product, quantity})
  end

  def remove_stock(server \\ __MODULE__, product, quantity) do
    operate(server, {:remove, product, quantity})
  end

  def stock(server \\ __MODULE__, product) do
    GenServer.call(server, {:stock, product})
  end

  def all_stock(server \\ __MODULE__) do
    GenServer.call(server, :all_stock)
  end

  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  defp operate(server, operation) do
    GenServer.call(server, {:operate, operation}, :infinity)
  end

  @impl true
  def init(opts) do
    max_concurrent = Keyword.get(opts, :max_concurrent, 1)
    initial_stock = Keyword.get(opts, :initial_stock, %{})

    if is_integer(max_concurrent) and max_concurrent > 0 and is_map(initial_stock) do
      {:ok,
       %{
         stock: initial_stock,
         history: [],
         sequence: 0,
         max_concurrent: max_concurrent,
         active: %{}
       }}
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:operate, operation}, from, state) do
    if map_size(state.active) >= state.max_concurrent do
      {:reply, {:error, :too_many_concurrent_operations}, state}
    else
      server = self()

      {pid, ref} =
        spawn_monitor(fn ->
          result = GenServer.call(server, {:apply_operation, operation}, :infinity)
          send(server, {:operation_finished, self(), result})
        end)

      active = Map.put(state.active, pid, {ref, from})
      {:noreply, %{state | active: active}}
    end
  end

  def handle_call({:apply_operation, operation}, _from, state) do
    {result, stock} = apply_operation(operation, state.stock)
    sequence = state.sequence + 1

    entry = %{
      sequence: sequence,
      operation: operation,
      result: result,
      timestamp: DateTime.utc_now()
    }

    next_state = %{
      state
      | stock: stock,
        history: [entry | state.history],
        sequence: sequence
    }

    {:reply, result, next_state}
  end

  def handle_call({:stock, product}, _from, state) do
    {:reply, Map.get(state.stock, product, 0), state}
  end

  def handle_call(:all_stock, _from, state) do
    {:reply, state.stock, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info({:operation_finished, pid, result}, state) do
    case Map.pop(state.active, pid) do
      {nil, _active} ->
        {:noreply, state}

      {{ref, from}, active} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, result)
        {:noreply, %{state | active: active}}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.active, pid) do
      {^ref, from} ->
        GenServer.reply(from, {:error, :operation_failed})
        {:noreply, %{state | active: Map.delete(state.active, pid)}}

      _ ->
        {:noreply, state}
    end
  end

  defp apply_operation({:add, product, quantity}, stock)
       when is_integer(quantity) and quantity > 0 do
    new_quantity = Map.get(stock, product, 0) + quantity
    {{:ok, new_quantity}, Map.put(stock, product, new_quantity)}
  end

  defp apply_operation({:remove, product, quantity}, stock)
       when is_integer(quantity) and quantity > 0 do
    current_quantity = Map.get(stock, product, 0)

    if current_quantity >= quantity do
      new_quantity = current_quantity - quantity
      {{:ok, new_quantity}, Map.put(stock, product, new_quantity)}
    else
      {{:error, :insufficient_stock}, stock}
    end
  end

  defp apply_operation(_operation, stock) do
    {{:error, :invalid_operation}, stock}
  end
end
