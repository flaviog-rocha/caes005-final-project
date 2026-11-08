# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# inventory.exs
# Execute com: elixir inventory.exs
# Os dados ficam em "inventory.dets" e sobrevivem a reinicializações.

# ---------------------------------------------------------------------------
# Macros: DSL para declarar operações puras sobre a quantidade de estoque.
# Cada `defop` gera uma função `nome(qtd_atual, args...)` e registra o nome
# da operação em um atributo acumulado, usado como whitelist em tempo de
# compilação.
# ---------------------------------------------------------------------------
defmodule Inventory.Operations do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import Inventory.Operations, only: [defop: 3]
      Module.register_attribute(__MODULE__, :ops, accumulate: true)
      @before_compile Inventory.Operations
    end
  end

  # Declara uma operação. A variável `qty` é injetada no escopo da função
  # e representa a quantidade atual do estoque.
  defmacro defop(name, args, do: body) do
    quote do
      @ops unquote(name)
      def unquote(name)(var!(qty), unquote_splicing(args)) do
        unquote(body)
      end
    end
  end

  # Gera `__ops__/0` com a lista de operações declaradas no módulo.
  defmacro __before_compile__(env) do
    ops = env.module |> Module.get_attribute(:ops) |> Enum.reverse()

    quote do
      @doc "Lista de operações permitidas."
      def __ops__, do: unquote(ops)
    end
  end
end

# ---------------------------------------------------------------------------
# Operações de negócio (funções puras: {:ok, nova_qtd} | {:error, motivo})
# ---------------------------------------------------------------------------
defmodule Inventory.Ops do
  use Inventory.Operations

  defop :add, [n] do
    if is_integer(n) and n > 0, do: {:ok, qty + n}, else: {:error, :invalid_quantity}
  end

  defop :remove, [n] do
    cond do
      not (is_integer(n) and n > 0) -> {:error, :invalid_quantity}
      n > qty -> {:error, :insufficient_stock}
      true -> {:ok, qty - n}
    end
  end

  defop :set, [n] do
    _ = qty
    if is_integer(n) and n >= 0, do: {:ok, n}, else: {:error, :invalid_quantity}
  end
end

# ---------------------------------------------------------------------------
# Armazenamento: dono da tabela DETS (persistência em disco).
# ---------------------------------------------------------------------------
defmodule Inventory.Storage do
  use GenServer

  @table :inventory_dets

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    # Necessário para que `terminate/2` seja chamado e a tabela seja fechada.
    Process.flag(:trap_exit, true)

    path = opts |> Keyword.get(:path, "inventory.dets") |> String.to_charlist()
    {:ok, @table} = :dets.open_file(@table, file: path, type: :set)
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end
end

# ---------------------------------------------------------------------------
# Entidade: um processo por SKU. Operações sobre o mesmo SKU são serializadas
# pelo GenServer; SKUs diferentes são processados em paralelo.
# O estado (quantidade + operações já executadas) é persistido a cada mudança.
# ---------------------------------------------------------------------------
defmodule Inventory.Entity do
  use GenServer

  @table :inventory_dets

  def start_link(sku), do: GenServer.start_link(__MODULE__, sku, name: via(sku))

  def via(sku), do: {:via, Registry, {Inventory.Registry, sku}}

  def apply_op(sku, op_id, op, args), do: GenServer.call(via(sku), {:apply, op_id, op, args})

  def stock(sku), do: GenServer.call(via(sku), :stock)

  @impl true
  def init(sku) do
    state =
      case :dets.lookup(@table, {:entity, sku}) do
        [{_key, persisted}] -> persisted
        [] -> %{sku: sku, qty: 0, ops: %{}}
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:apply, op_id, op, args}, _from, state) do
    case Map.fetch(state.ops, op_id) do
      # Operação já executada: devolve o resultado original sem reaplicar.
      {:ok, result} ->
        {:reply, {:replayed, result}, state}

      :error ->
        {result, new_state} =
          case run(op, args, state.qty) do
            {:ok, qty} -> {{:ok, qty}, %{state | qty: qty}}
            {:error, _reason} = err -> {err, state}
          end

        new_state = %{new_state | ops: Map.put(new_state.ops, op_id, result)}
        persist!(state.sku, new_state)

        {:reply, {:applied, result}, new_state}
    end
  end

  def handle_call(:stock, _from, state), do: {:reply, state.qty, state}

  # Executa somente operações declaradas via `defop`.
  defp run(op, args, qty) do
    if op in Inventory.Ops.__ops__() do
      apply(Inventory.Ops, op, [qty | args])
    else
      {:error, :unknown_operation}
    end
  end

  defp persist!(sku, state) do
    :ok = :dets.insert(@table, {{:entity, sku}, state})
    :ok = :dets.sync(@table)
  end
end

# ---------------------------------------------------------------------------
# DynamicSupervisor: cria um processo de entidade sob demanda.
# ---------------------------------------------------------------------------
defmodule Inventory.EntitySupervisor do
  use DynamicSupervisor

  def start_link(arg), do: DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @impl true
  def init(_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  def ensure_started(sku) do
    case DynamicSupervisor.start_child(__MODULE__, {Inventory.Entity, sku}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end
end

# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------
defmodule Inventory do
  @moduledoc """
  Gerenciamento de estoque com:
  - persistência (DETS),
  - idempotência por `op_id`,
  - processamento independente por SKU,
  - concorrência entre SKUs distintos.
  """

  @doc "Executa uma operação idempotente sobre um SKU."
  def apply_operation(sku, op_id, op, args \\ []) do
    {:ok, _pid} = Inventory.EntitySupervisor.ensure_started(sku)
    Inventory.Entity.apply_op(sku, op_id, op, args)
  end

  @doc "Consulta a quantidade atual de um SKU."
  def stock(sku) do
    {:ok, _pid} = Inventory.EntitySupervisor.ensure_started(sku)
    Inventory.Entity.stock(sku)
  end
end

# ---------------------------------------------------------------------------
# Aplicação / árvore de supervisão.
# ---------------------------------------------------------------------------
defmodule Inventory.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Inventory.Storage, [path: Application.get_env(:inventory, :path, "inventory.dets")]},
      {Registry, keys: :unique, name: Inventory.Registry},
      {Inventory.EntitySupervisor, []}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Inventory.Supervisor)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------
defmodule Inventory.Demo do
  def run do
    {:ok, sup} = Inventory.Application.start(:normal, [])

    # Operação sequencial e reexecução com o mesmo op_id (idempotência).
    IO.inspect(Inventory.apply_operation("SKU-1", "op-add-1", :add, [10]), label: "add")
    IO.inspect(Inventory.apply_operation("SKU-1", "op-add-1", :add, [10]), label: "duplicada")

    # Mesma operação disparada concorrentemente: apenas uma é aplicada.
    applied =
      1..20
      |> Task.async_stream(
        fn _ -> Inventory.apply_operation("SKU-2", "op-x", :add, [5]) end,
        max_concurrency: 20
      )
      |> Enum.map(fn {:ok, r} -> r end)
      |> Enum.count(&match?({:applied, _}, &1))

    IO.puts("aplicações concorrentes efetivas: #{applied}")

    # Operações concorrentes em SKUs distintos (processamento independente).
    1..50
    |> Task.async_stream(
      fn i -> Inventory.apply_operation("SKU-#{rem(i, 5)}", "batch-#{i}", :add, [1]) end,
      max_concurrency: 50
    )
    |> Stream.run()

    for sku <- ["SKU-0", "SKU-1", "SKU-2", "SKU-3", "SKU-4"] do
      IO.inspect({sku, Inventory.stock(sku)}, label: "estoque")
    end

    Supervisor.stop(sup)
  end
end

Inventory.Demo.run()
