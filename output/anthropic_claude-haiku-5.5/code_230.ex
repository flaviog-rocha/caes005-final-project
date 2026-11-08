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

# order_management.exs
# Execução: elixir order_management.exs
# Arquivo autocontido: gerenciamento de pedidos de e-commerce com
# macros, recuperação de processos, operações concorrentes e
# transações atômicas sobre um "banco" fictício persistido em :dets.

defmodule OrderManagement.Macros do
  @moduledoc """
  Macros de domínio:
    * `transitions/1` gera, em tempo de compilação, as cláusulas de
      transição de status a partir de uma tabela declarativa.
    * `atomically/2` envolve um bloco em uma transação do Store: ou o
      novo estado inteiro é confirmado, ou nada é alterado.
  """

  # Recebe a tabela de transições como AST literal e expande para
  # uma cláusula `transition/2` por par {origem, evento} -> destino.
  defmacro transitions(rules) do
    clauses =
      for {from, events} <- rules, {event, to} <- events do
        quote do
          def transition(unquote(from), unquote(event)), do: {:ok, unquote(to)}
        end
      end

    quote do
      unquote_splicing(clauses)

      # Qualquer combinação não declarada é rejeitada.
      def transition(status, event), do: {:error, {:invalid_transition, status, event}}
    end
  end

  # Expande o bloco para uma função anônima executada dentro da
  # transação do Store. O bloco deve retornar {:ok, novo_estado, resultado}
  # ou {:error, motivo}.
  defmacro atomically(state_var, do: block) do
    quote do
      OrderManagement.Store.transaction(fn unquote(state_var) -> unquote(block) end)
    end
  end
end

defmodule OrderManagement.Store do
  @moduledoc """
  Banco de dados fictício. Mantém o estado em memória dentro de um
  GenServer e persiste cada confirmação em :dets. Ao reiniciar após uma
  falha, o estado é recarregado do arquivo.

  Atomicidade: a função da transação roda sobre o estado atual. Só há
  commit se retornar {:ok, novo_estado, resultado}. Erros, exceções ou
  retornos inválidos descartam a transação, sem qualquer alteração parcial.
  Como o GenServer serializa as transações, não há commits concorrentes
  sobre o mesmo estado.
  """
  use GenServer

  @table :order_store
  @empty %{orders: %{}, stock: %{}, version: 0}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def transaction(fun), do: GenServer.call(__MODULE__, {:transaction, fun}, :infinity)

  def read(fun), do: GenServer.call(__MODULE__, {:read, fun})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "order_store.dets"))
    {:ok, _name} = :dets.open_file(@table, file: String.to_charlist(path), type: :set)

    state =
      case :dets.lookup(@table, :state) do
        [{:state, persisted}] -> persisted
        [] -> @empty
      end

    {:ok, %{state: state}}
  end

  @impl true
  def handle_call({:read, fun}, _from, %{state: state} = data) do
    {:reply, fun.(state), data}
  end

  def handle_call({:transaction, fun}, _from, %{state: state} = data) do
    case run(fun, state) do
      {:ok, new_state, result} ->
        new_state = Map.update!(new_state, :version, &(&1 + 1))

        # Persiste antes de substituir o estado em memória. Se a escrita
        # falhar, o processo cai e é recarregado com o estado anterior.
        :ok = :dets.insert(@table, {:state, new_state})
        :ok = :dets.sync(@table)

        {:reply, {:ok, result}, %{data | state: new_state}}

      {:error, _reason} = error ->
        {:reply, error, data}
    end
  end

  defp run(fun, state) do
    case fun.(state) do
      {:ok, new_state, result} -> {:ok, new_state, result}
      {:error, _} = error -> error
      other -> {:error, {:bad_transaction_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end
end

defmodule OrderManagement.Orders do
  @moduledoc """
  Regras de negócio de pedidos e estoque. Todas as operações de escrita
  são atômicas, executadas via `atomically/2`.
  """
  import OrderManagement.Macros

  alias OrderManagement.Store

  # Tabela declarativa de transições de status.
  transitions pending: [pay: :paid, cancel: :cancelled],
              paid: [ship: :shipped, cancel: :cancelled, refund: :refunded],
              shipped: [deliver: :delivered],
              delivered: [],
              cancelled: [],
              refunded: []

  @doc "Define o estoque disponível de um SKU."
  def set_stock(sku, qty) do
    atomically(state, do: {:ok, %{state | stock: Map.put(state.stock, sku, qty)}, :ok})
  end

  @doc """
  Cria um pedido. Reserva todos os itens ou nenhum: se algum SKU não tiver
  estoque, nada é alterado.
  """
  def create(order_id, customer, items) do
    atomically state do
      if Map.has_key?(state.orders, order_id) do
        {:error, :duplicate_order}
      else
        case reserve(state.stock, items) do
          {:ok, stock} ->
            order = %{
              id: order_id,
              customer: customer,
              items: items,
              status: :pending,
              version: 1
            }

            {:ok, %{state | orders: Map.put(state.orders, order_id, order), stock: stock}, order}

          {:error, _} = error ->
            error
        end
      end
    end
  end

  @doc """
  Aplica um evento a um pedido. Se `expected_version` for informado, a
  operação só ocorre se a versão atual coincidir (controle otimista).
  """
  def apply_event(order_id, event, expected_version \\ nil) do
    atomically state do
      apply_in_state(state, order_id, event, expected_version)
    end
  end

  # ---------------------------------------------------------------------------
  # Funções privadas (regras puras sobre o estado)
  # ---------------------------------------------------------------------------

  defp apply_in_state(state, order_id, event, expected_version) do
    with {:ok, order} <- fetch(state, order_id),
         :ok <- check_version(order, expected_version),
         {:ok, status} <- transition(order.status, event) do
      order = %{order | status: status, version: order.version + 1}

      stock =
        if event == :cancel,
          do: release(state.stock, order.items),
          else: state.stock

      {:ok, %{state | orders: Map.put(state.orders, order_id, order), stock: stock}, order}
    end
  end

  defp fetch(state, order_id) do
    case Map.fetch(state.orders, order_id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :not_found}
    end
  end

  defp check_version(_order, nil), do: :ok
  defp check_version(%{version: v}, v), do: :ok
  defp check_version(order, expected), do: {:error, {:version_conflict, order.version, expected}}

  defp reserve(stock, items) do
    Enum.reduce_while(items, {:ok, stock}, fn {sku, qty}, {:ok, acc} ->
      available = Map.get(acc, sku, 0)

      if available >= qty do
        {:cont, {:ok, Map.put(acc, sku, available - qty)}}
      else
        {:halt, {:error, {:insufficient_stock, sku}}}
      end
    end)
  end

  defp release(stock, items) do
    Enum.reduce(items, stock, fn {sku, qty}, acc ->
      Map.update(acc, sku, qty, &(&1 + qty))
    end)
  end
end

defmodule OrderManagement.OrderWorker do
  @moduledoc """
  Um processo por pedido, supervisionado pelo DynamicSupervisor.
  O worker é stateless: todo estado vem do Store. Assim, quando ele cai,
  o supervisor o reinicia e a próxima chamada recupera o pedido
  integralmente do banco.
  """
  use GenServer, restart: :transient

  alias OrderManagement.Orders

  def start_link(order_id) do
    GenServer.start_link(__MODULE__, order_id, name: via(order_id))
  end

  @impl true
  def init(order_id), do: {:ok, order_id}

  @doc "Envia um evento ao worker do pedido, iniciando-o se necessário."
  def dispatch(order_id, event, expected_version \\ nil) do
    pid = ensure_started(order_id)
    GenServer.call(pid, {:event, event, expected_version}, :infinity)
  end

  @impl true
  def handle_call({:event, event, expected_version}, _from, order_id) do
    {:reply, Orders.apply_event(order_id, event, expected_version), order_id}
  end

  defp via(order_id), do: {:via, Registry, {OrderManagement.Registry, order_id}}

  defp ensure_started(order_id) do
    case Registry.lookup(OrderManagement.Registry, order_id) do
      [{pid, _}] ->
        pid

      [] ->
        case DynamicSupervisor.start_child(OrderManagement.WorkerSup, {__MODULE__, order_id}) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end
end

defmodule OrderManagement.Root do
  @moduledoc """
  Árvore de supervisão.
    * Store: banco fictício. Se cair, os processos posteriores são reiniciados
      (`rest_for_one`), pois dependem dele.
    * Registry: localiza o worker de cada pedido.
    * WorkerSup: supervisiona os workers, reiniciando-os após falhas.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {OrderManagement.Store, opts},
      {Registry, keys: :unique, name: OrderManagement.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: OrderManagement.WorkerSup}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule OrderManagement.Demo do
  @moduledoc "Demonstração: concorrência, atomicidade, controle de versão e recuperação."
  alias OrderManagement.{Orders, OrderWorker, Store}

  def run do
    path = Path.join(System.tmp_dir!(), "order_store_#{System.unique_integer([:positive])}.dets")
    {:ok, _pid} = OrderManagement.Root.start_link(path: path)

    :ok = Orders.set_stock("SKU-1", 10)
    :ok = Orders.set_stock("SKU-2", 0)

    # 1) Criação concorrente: 20 pedidos disputando 10 unidades.
    ids = for i <- 1..20, do: "ORD-#{i}"

    results =
      ids
      |> Task.async_stream(
        fn id -> Orders.create(id, "cliente-#{id}", [{"SKU-1", 1}]) end,
        max_concurrency: 8,
        timeout: :infinity
      )
      |> Enum.map(fn {:ok, r} -> r end)

    IO.inspect(Enum.count(results, &match?({:ok, _}, &1)), label: "pedidos criados")
    IO.inspect(Enum.count(results, &match?({:error, _}, &1)), label: "pedidos rejeitados")

    # 2) Atomicidade: o item SKU-2 não tem estoque, então o SKU-1 não pode ser debitado.
    IO.inspect(
      Orders.create("ORD-X", "cliente-x", [{"SKU-1", 1}, {"SKU-2", 1}]),
      label: "pedido parcial"
    )

    IO.inspect(Store.read(& &1.stock), label: "estoque após falha parcial")

    # 3) Eventos concorrentes, um worker por pedido criado com sucesso.
    created = for {:ok, %{id: id}} <- results, do: id

    created
    |> Task.async_stream(fn id -> OrderWorker.dispatch(id, :pay) end,
      max_concurrency: 8,
      timeout: :infinity
    )
    |> Enum.each(fn {:ok, _} -> :ok end)

    IO.puts("pagamentos processados concorrentemente")

    # 4) Falha inesperada: mata o worker de ORD-1 e confirma recuperação.
    [{pid, _}] = Registry.lookup(OrderManagement.Registry, "ORD-1")
    Process.exit(pid, :kill)
    Process.sleep(50)

    IO.inspect(OrderWorker.dispatch("ORD-1", :ship), label: "envio após recuperação do worker")

    # 5) Controle de versão: a versão esperada está desatualizada.
    IO.inspect(OrderWorker.dispatch("ORD-2", :ship, 1), label: "conflito de versão")

    # 6) Transição inválida é rejeitada sem alterar o pedido.
    IO.inspect(OrderWorker.dispatch("ORD-2", :deliver), label: "transição inválida")

    # 7) Cancelamento devolve o estoque reservado.
    IO.inspect(OrderWorker.dispatch("ORD-3", :cancel), label: "cancelamento")
    IO.inspect(Store.read(& &1.stock), label: "estoque após cancelamento")

    :ok
  end
end

OrderManagement.Demo.run()
