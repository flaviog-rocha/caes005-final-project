# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Recuperar processos após falhas inesperadas.
# 3. Permitir a execução de operações simultâneas.
# 4. Impedir atualizações parcialmente concluídas.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de pedidos de e-commerce em Elixir.
# Execute com: elixir pedidos.exs

# ---------------------------------------------------------------------------
# Macros utilitárias (requisito 1)
# ---------------------------------------------------------------------------
defmodule Ecommerce.Macros do
  @moduledoc """
  Macro `atomically` que envolve um bloco em uma transação do banco fictício.
  O bloco deve retornar `{:ok, tabelas_atualizadas, resultado}` para confirmar
  ou `{:error, motivo}` para descartar todas as escritas.
  """

  defmacro atomically(tx_var, do: block) do
    quote do
      Ecommerce.FakeDB.transaction(fn unquote(tx_var) -> unquote(block) end)
    end
  end
end

# ---------------------------------------------------------------------------
# DSL de transições de estado definida via macros (requisito 1)
# As transições são resolvidas em tempo de compilação.
# ---------------------------------------------------------------------------
defmodule Ecommerce.Transitions do
  @moduledoc false

  defmacro transitions(do: block) do
    quote do
      Module.register_attribute(__MODULE__, :allowed_transition, accumulate: true)
      unquote(block)
      @allowed_transitions Module.get_attribute(__MODULE__, :allowed_transition)

      def allowed?(from, to), do: {from, to} in @allowed_transitions
      def allowed_transitions, do: @allowed_transitions
    end
  end

  defmacro allow(from, to) do
    quote do
      @allowed_transition {unquote(from), unquote(to)}
    end
  end
end

# ---------------------------------------------------------------------------
# Migrations declarativas via macros (opcional, sem Ecto)
# ---------------------------------------------------------------------------
defmodule Ecommerce.Migrations do
  @moduledoc false

  defmacro migrate(do: block) do
    quote do
      Module.register_attribute(__MODULE__, :table, accumulate: true)
      unquote(block)
      @tables Module.get_attribute(__MODULE__, :table)

      def tables, do: @tables
    end
  end

  defmacro create_table(name) do
    quote do
      @table unquote(name)
    end
  end
end

defmodule Ecommerce.Migrations.V1 do
  @moduledoc false
  import Ecommerce.Migrations

  migrate do
    create_table :orders
    create_table :order_events
  end
end

# ---------------------------------------------------------------------------
# Banco de dados fictício em memória (requisitos 2 e 4)
# Cada transação roda sobre uma cópia imutável das tabelas, dentro do
# processo do banco. Só há commit se o bloco retornar {:ok, ...}. Exceções
# e lançamentos são capturados, então nenhuma escrita parcial é aplicada.
# ---------------------------------------------------------------------------
defmodule Ecommerce.FakeDB do
  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    tables = Map.new(Ecommerce.Migrations.V1.tables(), &{&1, %{}})
    {:ok, %{tables: tables}}
  end

  def read(db \\ __MODULE__, table, key) do
    GenServer.call(db, {:read, table, key})
  end

  def transaction(db \\ __MODULE__, fun) when is_function(fun, 1) do
    GenServer.call(db, {:transaction, fun})
  end

  # Operações puras sobre a cópia de trabalho das tabelas (usadas dentro de transações)
  def tx_get(tables, table, key) do
    tables |> Map.fetch!(table) |> Map.get(key)
  end

  def tx_put(tables, table, key, value) do
    Map.update!(tables, table, &Map.put(&1, key, value))
  end

  @impl true
  def handle_call({:read, table, key}, _from, state) do
    {:reply, tx_get(state.tables, table, key), state}
  end

  def handle_call({:transaction, fun}, _from, state) do
    case run(fun, state.tables) do
      # Commit: única troca de estado, atômica
      {:ok, tables, result} -> {:reply, {:ok, result}, %{state | tables: tables}}
      # Rollback: o estado anterior permanece intacto
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp run(fun, working_copy) do
    case fun.(working_copy) do
      {:ok, tables, result} when is_map(tables) -> {:ok, tables, result}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_transaction_result, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end

# ---------------------------------------------------------------------------
# Máquina de estados do pedido (usa a DSL de macros)
# ---------------------------------------------------------------------------
defmodule Ecommerce.OrderStates do
  @moduledoc false
  import Ecommerce.Transitions

  transitions do
    allow :pending, :paid
    allow :pending, :cancelled
    allow :paid, :shipped
    allow :paid, :cancelled
    allow :shipped, :delivered
  end
end

# ---------------------------------------------------------------------------
# Processo de pedido (requisitos 2, 3 e 4)
# Um processo por pedido: operações sobre pedidos diferentes rodam em
# paralelo; operações sobre o mesmo pedido são serializadas pela mailbox.
# Ao reiniciar, o estado é recarregado do banco (último commit válido).
# ---------------------------------------------------------------------------
defmodule Ecommerce.Order do
  use GenServer, restart: :permanent

  import Ecommerce.Macros
  alias Ecommerce.{FakeDB, OrderStates}

  def start_link(order_id) do
    GenServer.start_link(__MODULE__, order_id, name: via(order_id))
  end

  def via(order_id), do: {:via, Registry, {Ecommerce.OrderRegistry, order_id}}

  def command(order_id, cmd), do: GenServer.call(via(order_id), {:command, cmd})

  def get(order_id), do: GenServer.call(via(order_id), :get)

  @impl true
  def init(order_id) do
    case FakeDB.read(:orders, order_id) do
      nil -> {:stop, :not_found}
      order -> {:ok, order}
    end
  end

  @impl true
  def handle_call(:get, _from, order), do: {:reply, order, order}

  def handle_call({:command, cmd}, _from, order) do
    # Se `cmd` lançar exceção, o processo cai sem persistir nada e o supervisor
    # o reinicia recarregando o estado consistente do banco.
    with {:ok, new_order, event} <- cmd.(order),
         {:ok, persisted} <- persist(order, new_order, event) do
      {:reply, {:ok, persisted}, persisted}
    else
      {:error, reason} -> {:reply, {:error, reason}, order}
    end
  end

  # Pedido e evento são gravados na mesma transação, ou nenhum dos dois.
  # A verificação de versão evita sobrescrever uma atualização concorrente.
  defp persist(order, new_order, event) do
    atomically(tx) do
      current = FakeDB.tx_get(tx, :orders, order.id)

      cond do
        current == nil ->
          {:error, :not_found}

        current.version != order.version ->
          {:error, :stale_version}

        true ->
          tx =
            tx
            |> FakeDB.tx_put(:orders, order.id, new_order)
            |> FakeDB.tx_put(:order_events, {order.id, new_order.version}, event)

          {:ok, tx, new_order}
      end
    end
  end
end

# ---------------------------------------------------------------------------
# Comandos de domínio (regras de negócio)
# ---------------------------------------------------------------------------
defmodule Ecommerce.Orders do
  @moduledoc false
  import Ecommerce.Macros
  alias Ecommerce.{FakeDB, Order, OrderStates}

  def create(customer_id) do
    id = System.unique_integer([:positive, :monotonic])

    order = %{
      id: id,
      customer_id: customer_id,
      status: :pending,
      items: [],
      total: 0,
      version: 0
    }

    with {:ok, _} <- persist_new(order),
         {:ok, _pid} <- ensure_started(id) do
      {:ok, id}
    end
  end

  def add_item(id, item) do
    with {:ok, _} <- ensure_started(id) do
      Order.command(id, &add_item_cmd(&1, item))
    end
  end

  def transition(id, to) do
    with {:ok, _} <- ensure_started(id) do
      Order.command(id, &transition_cmd(&1, to))
    end
  end

  def fetch(id) do
    with {:ok, _} <- ensure_started(id) do
      {:ok, Order.get(id)}
    end
  end

  # Inicia (ou reinicia) o processo do pedido sob o DynamicSupervisor.
  # Trata a corrida em que dois chamadores tentam iniciar o mesmo processo.
  def ensure_started(id) do
    case DynamicSupervisor.start_child(Ecommerce.OrderSupervisor, {Order, id}) do
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  defp persist_new(order) do
    atomically(tx) do
      {:ok, FakeDB.tx_put(tx, :orders, order.id, order), order.id}
    end
  end

  defp add_item_cmd(%{status: :pending} = order, item) do
    new_order = %{
      order
      | items: [item | order.items],
        total: order.total + item.price * item.qty,
        version: order.version + 1
    }

    {:ok, new_order, %{type: :item_added, item: item}}
  end

  defp add_item_cmd(order, _item), do: {:error, {:order_not_editable, order.status}}

  defp transition_cmd(order, to) do
    if OrderStates.allowed?(order.status, to) do
      new_order = %{order | status: to, version: order.version + 1}
      {:ok, new_order, %{type: :status_changed, from: order.status, to: to}}
    else
      {:error, {:invalid_transition, order.status, to}}
    end
  end
end

# ---------------------------------------------------------------------------
# Árvore de supervisão (requisito 2)
# ---------------------------------------------------------------------------
defmodule Ecommerce.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Ecommerce.FakeDB,
      {Registry, keys: :unique, name: Ecommerce.OrderRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: Ecommerce.OrderSupervisor}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Ecommerce.Supervisor)
  end
end

# ---------------------------------------------------------------------------
# Demonstração: concorrência, recuperação de falhas e atomicidade
# ---------------------------------------------------------------------------
defmodule Ecommerce.Demo do
  @moduledoc false
  alias Ecommerce.{FakeDB, Order, Orders}

  def run do
    {:ok, _} = Ecommerce.Application.start(:normal, [])

    IO.puts("== 1. Fluxo básico ==")
    {:ok, id} = Orders.create(:customer_1)
    {:ok, _} = Orders.add_item(id, %{sku: "A1", price: 100, qty: 2})
    {:ok, order} = Orders.transition(id, :paid)
    IO.inspect(order, label: "pedido após pagamento")

    IO.puts("\n== 2. Transição inválida não altera o pedido ==")
    IO.inspect(Orders.transition(id, :delivered), label: "resultado")
    IO.inspect(Orders.add_item(id, %{sku: "X", price: 1, qty: 1}) |> elem(0),
      label: "adição de item em pedido pago")

    IO.puts("\n== 3. Recuperação após falha inesperada ==")
    pid_antes = GenServer.whereis(Order.via(id))
    Process.exit(pid_antes, :kill)
    Process.sleep(20)
    pid_depois = GenServer.whereis(Order.via(id))
    IO.inspect({pid_antes != pid_depois, :novo_processo}, label: "processo reiniciado")
    {:ok, recovered} = Orders.fetch(id)
    IO.inspect({recovered.status, recovered.version, recovered.total},
      label: "estado recuperado do banco")

    IO.puts("\n== 4. Operações simultâneas sobre vários pedidos ==")
    ids =
      for _ <- 1..20 do
        {:ok, new_id} = Orders.create(:customer_x)
        new_id
      end

    ids
    |> Task.async_stream(
      fn i ->
        {:ok, _} = Orders.add_item(i, %{sku: "B", price: 50, qty: 1})
        Orders.transition(i, :paid)
      end,
      max_concurrency: 10,
      timeout: 5_000
    )
    |> Enum.each(fn {:ok, {:ok, _}} -> :ok end)

    paid_count =
      ids
      |> Enum.map(fn i -> elem(Orders.fetch(i), 1).status end)
      |> Enum.count(&(&1 == :paid))

    IO.inspect(paid_count, label: "pedidos pagos (esperado 20)")

    IO.puts("\n== 5. Concorrência sobre o mesmo pedido (serializada) ==")
    {:ok, shared} = Orders.create(:customer_y)

    results =
      1..10
      |> Task.async_stream(fn _ -> Orders.transition(shared, :paid) end,
        max_concurrency: 10
      )
      |> Enum.map(fn {:ok, r} -> r end)

    IO.inspect(Enum.count(results, &match?({:ok, _}, &1)), label: "transições aceitas (esperado 1)")

    IO.puts("\n== 6. Trilha de eventos gravada atomicamente ==")
    {:ok, final} = Orders.fetch(shared)
    events = for v <- 1..final.version, do: FakeDB.read(:order_events, {shared, v})
    IO.inspect(events, label: "eventos do pedido")

    :ok
  end
end

Ecommerce.Demo.run()
