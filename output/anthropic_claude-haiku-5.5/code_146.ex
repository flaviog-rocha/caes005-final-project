# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de pedidos de e-commerce
# Requisitos atendidos:
#   1. Cada pedido é um processo independente (GenServer supervisionado).
#   2. Notificações são emitidas quando condições são atingidas (uma vez por evento).
#   3. Persistência em disco via :dets, com recuperação na reinicialização.
#   4. Validação pura dos dados antes de qualquer operação.
# Somente biblioteca padrão do Elixir/OTP.

defmodule Ecommerce do
  @moduledoc """
  Ponto de entrada da aplicação.

  Uso básico:

      {:ok, _} = Ecommerce.start("pedidos.dets")
      Ecommerce.subscribe()
      {:ok, _} = Ecommerce.create_order(%{id: "A1", customer_id: "C1",
                   items: [%{sku: "X", qty: 1, unit_price: 150_000}]})
      {:ok, _} = Ecommerce.pay("A1")
      {:ok, _} = Ecommerce.ship("A1")
  """

  alias Ecommerce.{Validator, Order, Store, Notifier}

  @order_sup Ecommerce.OrderSupervisor
  @registry Ecommerce.Registry

  @doc "Inicia a árvore de supervisão e recupera os pedidos persistidos."
  def start(path \\ "ecommerce_orders.dets") do
    children = [
      # Registro de nomes para localizar pedidos pelo id
      {Registry, keys: :unique, name: @registry},
      # Supervisor dinâmico: cada pedido é um filho isolado
      {DynamicSupervisor, strategy: :one_for_one, name: @order_sup},
      # Processo de notificações
      {Notifier, []},
      # Processo dono da tabela de persistência
      {Store, path}
    ]

    with {:ok, sup} <-
           Supervisor.start_link(children,
             strategy: :one_for_one,
             name: Ecommerce.Supervisor
           ) do
      recover()
      {:ok, sup}
    end
  end

  @doc "Valida e cria um novo pedido."
  def create_order(attrs) do
    with {:ok, order} <- Validator.new_order(attrs),
         :not_found <- Store.get(order.id),
         {:ok, _pid} <- DynamicSupervisor.start_child(@order_sup, {Order, order}) do
      Order.get(order.id)
    else
      {:ok, _} -> {:error, :already_exists}
      {:error, {:already_started, _}} -> {:error, :already_exists}
      {:error, _} = err -> err
    end
  end

  def get_order(id), do: Order.get(id)
  def pay(id), do: Order.transition(id, :paid)
  def ship(id), do: Order.transition(id, :shipped)
  def cancel(id), do: Order.transition(id, :cancelled)

  def subscribe(pid \\ self()), do: Notifier.subscribe(pid)
  def notifications, do: Notifier.history()

  # Recria um processo para cada pedido persistido
  defp recover do
    Enum.each(Store.all(), fn order ->
      DynamicSupervisor.start_child(@order_sup, {Order, order})
    end)
  end
end

defmodule Ecommerce.Validator do
  @moduledoc "Funções puras de validação e regras de transição de status."

  # Transições permitidas entre status
  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [],
    cancelled: []
  }

  @doc "Valida atributos de um novo pedido e retorna o pedido normalizado."
  def new_order(attrs) when is_map(attrs) do
    errors =
      []
      |> require_string(attrs, :id)
      |> require_string(attrs, :customer_id)
      |> validate_items(Map.get(attrs, :items))

    case errors do
      [] -> {:ok, normalize(attrs)}
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  def new_order(_), do: {:error, [{:attrs, "deve ser um mapa"}]}

  @doc "Verifica se o pedido pode transitar para o status `to`."
  def transition(%{status: from}, to) do
    if to in Map.get(@transitions, from, []) do
      :ok
    else
      {:error, {:invalid_transition, from, to}}
    end
  end

  @doc "Calcula o total do pedido em centavos."
  def total(%{items: items}) do
    Enum.reduce(items, 0, fn %{qty: q, unit_price: p}, acc -> acc + q * p end)
  end

  # Helpers de validação

  defp require_string(errors, attrs, key) do
    value = Map.get(attrs, key)
    add_if(errors, not (is_binary(value) and value != ""), {key, "deve ser string não vazia"})
  end

  defp validate_items(errors, items) when is_list(items) and items != [] do
    items
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {item, i}, acc -> validate_item(item, i, acc) end)
  end

  defp validate_items(errors, _), do: [{:items, "deve ser uma lista não vazia"} | errors]

  defp validate_item(item, i, errors) when is_map(item) do
    sku = Map.get(item, :sku)
    qty = Map.get(item, :qty)
    price = Map.get(item, :unit_price)

    errors
    |> add_if(not (is_binary(sku) and sku != ""), {{:items, i, :sku}, "inválido"})
    |> add_if(not (is_integer(qty) and qty > 0), {{:items, i, :qty}, "deve ser inteiro positivo"})
    |> add_if(
      not (is_integer(price) and price >= 0),
      {{:items, i, :unit_price}, "deve ser inteiro não negativo (centavos)"}
    )
  end

  defp validate_item(_item, i, errors), do: [{{:items, i}, "deve ser um mapa"} | errors]

  defp add_if(errors, true, error), do: [error | errors]
  defp add_if(errors, false, _error), do: errors

  # Normaliza o pedido: mantém apenas os campos conhecidos
  defp normalize(attrs) do
    %{
      id: attrs.id,
      customer_id: attrs.customer_id,
      status: :pending,
      notified: [],
      items:
        Enum.map(attrs.items, fn item ->
          %{sku: item.sku, qty: item.qty, unit_price: item.unit_price}
        end)
    }
  end
end

defmodule Ecommerce.Order do
  @moduledoc """
  Processo independente que representa um pedido.
  Cada pedido é serializado pelo seu próprio processo, então pedidos
  diferentes são processados em paralelo sem interferência.
  """

  use GenServer, restart: :transient

  alias Ecommerce.{Validator, Store, Notifier}

  # Valor (em centavos) a partir do qual o pedido é considerado de alto valor
  @high_value 100_000

  def start_link(order) do
    GenServer.start_link(__MODULE__, order, name: via(order.id))
  end

  def via(id), do: {:via, Registry, {Ecommerce.Registry, id}}

  def get(id), do: call(id, :get)
  def transition(id, to), do: call(id, {:transition, to})

  # Inicialização: se houver estado persistido, ele tem prioridade
  # (permite recuperação após reinicialização ou reinício do processo)
  @impl true
  def init(arg) do
    order =
      case Store.get(arg.id) do
        {:ok, stored} -> stored
        :not_found -> arg
      end

    {order, events} = evaluate(order)

    case persist_and_notify(order, events) do
      :ok -> {:ok, order}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:get, _from, order), do: {:reply, {:ok, order}, order}

  def handle_call({:transition, to}, _from, order) do
    with :ok <- Validator.transition(order, to) do
      {updated, events} =
        order
        |> Map.put(:status, to)
        |> evaluate()

      case persist_and_notify(updated, events) do
        :ok -> {:reply, {:ok, updated}, updated}
        {:error, _} = err -> {:reply, err, order}
      end
    else
      err -> {:reply, err, order}
    end
  end

  # Regras de notificação: cada evento é emitido apenas uma vez por pedido
  defp rules do
    [
      {:high_value, &(Validator.total(&1) >= @high_value), "Pedido de alto valor"},
      {:paid, &(&1.status == :paid), "Pagamento confirmado"},
      {:shipped, &(&1.status == :shipped), "Pedido enviado"},
      {:cancelled, &(&1.status == :cancelled), "Pedido cancelado"}
    ]
  end

  # Avalia as regras e retorna o pedido atualizado com os eventos disparados
  defp evaluate(order) do
    Enum.reduce(rules(), {order, []}, fn {event, pred, msg}, {acc, events} ->
      if event not in acc.notified and pred.(acc) do
        notification = %{order_id: acc.id, event: event, message: msg}
        {%{acc | notified: [event | acc.notified]}, [notification | events]}
      else
        {acc, events}
      end
    end)
  end

  # Persiste primeiro; só notifica se a persistência for bem-sucedida
  defp persist_and_notify(order, events) do
    case Store.put(order) do
      :ok ->
        events |> Enum.reverse() |> Enum.each(&Notifier.notify/1)
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp call(id, msg) do
    case Registry.lookup(Ecommerce.Registry, id) do
      [{pid, _}] -> GenServer.call(pid, msg)
      [] -> {:error, :not_found}
    end
  end
end

defmodule Ecommerce.Notifier do
  @moduledoc "Recebe notificações, mantém histórico e as entrega aos assinantes."

  use GenServer

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def notify(notification), do: GenServer.cast(__MODULE__, {:notify, notification})

  def subscribe(pid \\ self()), do: GenServer.call(__MODULE__, {:subscribe, pid})

  def history, do: GenServer.call(__MODULE__, :history)

  @impl true
  def init(_) do
    {:ok, %{subscribers: MapSet.new(), history: []}}
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_cast({:notify, notification}, state) do
    notification = Map.put(notification, :at, DateTime.utc_now())

    Enum.each(state.subscribers, &send(&1, {:notification, notification}))

    {:noreply, %{state | history: [notification | state.history]}}
  end
end

defmodule Ecommerce.Store do
  @moduledoc """
  Persistência em disco via :dets.
  Um único processo é dono da tabela, garantindo acesso serializado.
  """

  use GenServer

  @table :ecommerce_orders

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  def put(order), do: GenServer.call(__MODULE__, {:put, order})

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def all, do: GenServer.call(__MODULE__, :all)

  @impl true
  def init(path) do
    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, table} -> {:ok, table}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, order}, _from, table) do
    reply =
      with :ok <- :dets.insert(table, {order.id, order}) do
        :dets.sync(table)
      end

    {:reply, reply, table}
  end

  def handle_call({:get, id}, _from, table) do
    reply =
      case :dets.lookup(table, id) do
        [{_, order}] -> {:ok, order}
        [] -> :not_found
      end

    {:reply, reply, table}
  end

  def handle_call(:all, _from, table) do
    orders = :dets.foldl(fn {_id, order}, acc -> [order | acc] end, [], table)
    {:reply, orders, table}
  end

  @impl true
  def terminate(_reason, table) do
    :dets.close(table)
  end
end
