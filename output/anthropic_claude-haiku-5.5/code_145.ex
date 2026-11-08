# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de pedidos de e-commerce (somente biblioteca padrão)
#
# Execução: elixir pedidos.exs
#
# Requisitos atendidos:
# 1. Processamento independente: cada pedido é um GenServer próprio, registrado
#    via Registry e supervisionado por DynamicSupervisor.
# 2. Notificações: o Notifier avalia regras de negócio (ex.: pedido de alto valor,
#    mudanças de status) e entrega eventos aos assinantes.
# 3. Persistência: o Store grava cada pedido em disco com :dets, permitindo
#    a recuperação dos pedidos após reinicializações (restore/0).
# 4. Validação: entradas e transições de estado são validadas antes de qualquer operação.

defmodule Ecommerce.Validator do
  @moduledoc "Validações puras de entradas e transições de pedidos."

  @currencies ~w(BRL USD EUR)

  # Máquina de estados: status atual => status permitidos como próximo passo
  @transitions %{
    created: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [:delivered],
    delivered: [],
    cancelled: []
  }

  @doc "Valida os atributos de um novo pedido e retorna o pedido normalizado."
  def order(attrs) when is_map(attrs) do
    errors =
      []
      |> check(valid_id?(Map.get(attrs, :id)), :id, "deve ser string não vazia")
      |> check(valid_id?(Map.get(attrs, :customer_id)), :customer_id, "deve ser string não vazia")
      |> check(Map.get(attrs, :currency) in @currencies, :currency, "deve ser um de #{inspect(@currencies)}")
      |> check(valid_items?(Map.get(attrs, :items)), :items, "deve ser lista não vazia de itens válidos")

    case errors do
      [] -> {:ok, normalize(attrs)}
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  def order(_), do: {:error, [{:attrs, "deve ser um mapa"}]}

  @doc "Verifica se a transição de status é permitida."
  def transition(from, to) do
    if to in Map.get(@transitions, from, []) do
      :ok
    else
      {:error, [{:status, "transição inválida de #{from} para #{to}"}]}
    end
  end

  @doc "Verifica se um identificador é uma string não vazia."
  def valid_id?(id), do: is_binary(id) and String.trim(id) != ""

  defp check(errors, true, _key, _msg), do: errors
  defp check(errors, false, key, msg), do: [{key, msg} | errors]

  # Preço unitário em centavos (inteiro), evitando erros de ponto flutuante
  defp valid_items?(items) when is_list(items) and items != [],
    do: Enum.all?(items, &valid_item?/1)

  defp valid_items?(_), do: false

  defp valid_item?(%{sku: sku, qty: qty, unit_price: price})
       when is_binary(sku) and sku != "" and is_integer(qty) and qty > 0 and
              is_integer(price) and price >= 0,
       do: true

  defp valid_item?(_), do: false

  defp normalize(attrs) do
    items =
      Enum.map(attrs.items, fn i ->
        %{sku: i.sku, qty: i.qty, unit_price: i.unit_price}
      end)

    total = Enum.reduce(items, 0, fn i, acc -> acc + i.qty * i.unit_price end)

    %{
      id: attrs.id,
      customer_id: attrs.customer_id,
      currency: attrs.currency,
      items: items,
      total: total,
      status: :created
    }
  end
end

defmodule Ecommerce.Store do
  @moduledoc "Persistência em disco via :dets. Este GenServer é o dono da tabela."
  use GenServer

  @table :ecommerce_orders

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :path, "pedidos.dets"), name: __MODULE__)
  end

  def put(order), do: GenServer.call(__MODULE__, {:put, order})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def all, do: GenServer.call(__MODULE__, :all)

  @impl true
  def init(path) do
    Process.flag(:trap_exit, true)

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} -> {:ok, nil}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, order}, _from, state) do
    true = :dets.insert(@table, {order.id, order})
    :ok = :dets.sync(@table)
    {:reply, :ok, state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(@table, id) do
        [{^id, order}] -> {:ok, order}
        [] -> :error
      end

    {:reply, reply, state}
  end

  def handle_call(:all, _from, state) do
    orders = :dets.foldl(fn {_id, order}, acc -> [order | acc] end, [], @table)
    {:reply, orders, state}
  end

  @impl true
  def terminate(_reason, _state), do: :dets.close(@table)
end

defmodule Ecommerce.Notifier do
  @moduledoc "Avalia regras de negócio e entrega notificações aos assinantes."
  use GenServer

  @high_value_threshold 100_000

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def subscribe(pid \\ self()), do: GenServer.call(__MODULE__, {:subscribe, pid})

  def evaluate(order, event), do: GenServer.cast(__MODULE__, {:evaluate, order, event})

  @impl true
  def init(_), do: {:ok, MapSet.new()}

  @impl true
  def handle_call({:subscribe, pid}, _from, subs) do
    Process.monitor(pid)
    {:reply, :ok, MapSet.put(subs, pid)}
  end

  @impl true
  def handle_cast({:evaluate, order, event}, subs) do
    Enum.each(matched_rules(order, event), fn type ->
      notification = %{
        type: type,
        order_id: order.id,
        status: order.status,
        total: order.total,
        at: DateTime.utc_now()
      }

      IO.puts("[NOTIFICAÇÃO] #{type} pedido=#{order.id} total=#{order.total}")
      Enum.each(subs, &send(&1, {:notification, notification}))
    end)

    {:noreply, subs}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, subs),
    do: {:noreply, MapSet.delete(subs, pid)}

  def handle_info(_msg, subs), do: {:noreply, subs}

  # Regras de negócio que disparam notificações
  defp matched_rules(order, :created) do
    if order.total >= @high_value_threshold, do: [:high_value_order], else: []
  end

  defp matched_rules(_order, {:transition, _from, to}) do
    case to do
      :paid -> [:order_paid]
      :shipped -> [:order_shipped]
      :delivered -> [:order_delivered]
      :cancelled -> [:order_cancelled]
    end
  end
end

defmodule Ecommerce.OrderServer do
  @moduledoc "Processo independente por pedido; isola estado e transições."
  use GenServer, restart: :transient

  alias Ecommerce.{Store, Notifier, Validator}

  def start_link(order_id), do: GenServer.start_link(__MODULE__, order_id, name: via(order_id))

  def via(order_id), do: {:via, Registry, {Ecommerce.Registry, order_id}}

  def transition(order_id, to), do: GenServer.call(via(order_id), {:transition, to})

  @impl true
  def init(order_id) do
    # Carrega o estado atual a partir da persistência (recuperação após falhas)
    case Store.get(order_id) do
      {:ok, order} -> {:ok, order}
      :error -> {:stop, :not_found}
    end
  end

  @impl true
  def handle_call({:transition, to}, _from, order) do
    case Validator.transition(order.status, to) do
      :ok ->
        updated = %{order | status: to}
        :ok = Store.put(updated)
        Notifier.evaluate(updated, {:transition, order.status, to})
        {:reply, {:ok, updated}, updated}

      {:error, _} = error ->
        {:reply, error, order}
    end
  end
end

defmodule Ecommerce.Orders do
  @moduledoc "API pública para criar, consultar e alterar pedidos."

  alias Ecommerce.{Validator, Store, Notifier, OrderServer}

  def create(attrs) do
    with {:ok, order} <- Validator.order(attrs),
         :error <- Store.get(order.id),
         :ok <- Store.put(order),
         {:ok, _pid} <-
           DynamicSupervisor.start_child(Ecommerce.OrderSupervisor, {OrderServer, order.id}) do
      Notifier.evaluate(order, :created)
      {:ok, order}
    else
      {:ok, _existing} -> {:error, [{:id, "pedido já existe"}]}
      {:error, _} = error -> error
    end
  end

  def get(order_id) do
    with :ok <- validate_id(order_id) do
      case Store.get(order_id) do
        {:ok, order} -> {:ok, order}
        :error -> {:error, :not_found}
      end
    end
  end

  def pay(order_id), do: transition(order_id, :paid)
  def ship(order_id), do: transition(order_id, :shipped)
  def deliver(order_id), do: transition(order_id, :delivered)
  def cancel(order_id), do: transition(order_id, :cancelled)

  def transition(order_id, to) do
    with :ok <- validate_id(order_id) do
      try do
        OrderServer.transition(order_id, to)
      catch
        :exit, _ -> {:error, [{:id, "pedido não encontrado ou inativo"}]}
      end
    end
  end

  # Reinicia os processos de todos os pedidos persistidos
  def restore do
    Store.all()
    |> Enum.each(fn order ->
      DynamicSupervisor.start_child(Ecommerce.OrderSupervisor, {OrderServer, order.id})
    end)
  end

  defp validate_id(id) do
    if Validator.valid_id?(id), do: :ok, else: {:error, [{:id, "inválido"}]}
  end
end

defmodule Ecommerce.Application do
  @moduledoc "Árvore de supervisão do sistema."
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Ecommerce.Store, path: Keyword.get(opts, :path, "pedidos.dets")},
      {Registry, keys: :unique, name: Ecommerce.Registry},
      Ecommerce.Notifier,
      {DynamicSupervisor, strategy: :one_for_one, name: Ecommerce.OrderSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ---------------------------------------------------------------------------
# Demonstração de uso
# ---------------------------------------------------------------------------

{:ok, _} = Ecommerce.Application.start_link(path: "pedidos.dets")
:ok = Ecommerce.Notifier.subscribe()
:ok = Ecommerce.Orders.restore()

IO.inspect(
  Ecommerce.Orders.create(%{
    id: "ped-1",
    customer_id: "cli-9",
    currency: "BRL",
    items: [%{sku: "NOTEBOOK", qty: 1, unit_price: 150_000}]
  }),
  label: "criar ped-1"
)

IO.inspect(
  Ecommerce.Orders.create(%{id: "", customer_id: "x", currency: "XXX", items: []}),
  label: "criar inválido"
)

IO.inspect(
  Ecommerce.Orders.create(%{
    id: "ped-2",
    customer_id: "cli-3",
    currency: "USD",
    items: [%{sku: "MOUSE", qty: 2, unit_price: 2_500}]
  }),
  label: "criar ped-2"
)

IO.inspect(Ecommerce.Orders.pay("ped-1"), label: "pagar ped-1")
IO.inspect(Ecommerce.Orders.ship("ped-1"), label: "enviar ped-1")
IO.inspect(Ecommerce.Orders.deliver("ped-2"), label: "entregar ped-2 (transição inválida)")
IO.inspect(Ecommerce.Orders.get("ped-1"), label: "consultar ped-1")

# Aguarda o processamento assíncrono das notificações e as exibe
Process.sleep(100)

drain = fn drain ->
  receive do
    {:notification, n} ->
      IO.inspect(n, label: "recebida")
      drain.(drain)
  after
    0 -> :ok
  end
end

drain.(drain)
