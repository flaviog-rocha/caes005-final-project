# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# ============================================================================
# Gerenciamento de pedidos de e-commerce (arquivo único, autocontido)
#
# Requisitos atendidos:
#   1. Notificações quando condições são atingidas (alto valor, mudança de
#      status, falha de pagamento, encerramento inesperado de processos).
#   2. Detecção de encerramentos inesperados via Process.monitor/2
#      (workers de pagamento e assinantes de notificações).
#   3. Persistência em disco (term_to_binary com escrita atômica) para
#      recuperação após reinicializações.
#   4. Validação de todos os dados recebidos antes de qualquer operação.
#
# Uso (exemplo):
#   {:ok, _} = Ecommerce.Supervisor.start_link(data_file: "pedidos.bin")
#   Ecommerce.Notifier.subscribe()
#   {:ok, _} = Ecommerce.OrderManager.create(%{
#     id: "PED-1",
#     customer_id: "CLI-42",
#     items: [%{sku: "ABC", quantity: 2, unit_price: 5_000}]
#   })
#   :ok = Ecommerce.OrderManager.pay("PED-1")
# ============================================================================

defmodule Ecommerce.Order do
  @moduledoc """
  Modelo de pedido com funções puras de validação e máquina de estados.
  Valores monetários são armazenados em centavos (inteiros).
  """

  @statuses [:created, :paid, :shipped, :delivered, :cancelled]

  # Transições de estado permitidas
  @transitions %{
    created: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [:delivered],
    delivered: [],
    cancelled: []
  }

  @id_regex ~r/\A[A-Za-z0-9_-]{1,64}\z/

  defstruct [:id, :customer_id, :items, :total, :status, :created_at, :updated_at]

  def statuses, do: @statuses

  # Cria um pedido somente após validar todos os campos
  def new(attrs) when is_map(attrs) do
    with :ok <- validate_id(Map.get(attrs, :id)),
         :ok <- validate_non_empty_string(Map.get(attrs, :customer_id), :customer_id),
         :ok <- validate_items(Map.get(attrs, :items)) do
      %{id: id, customer_id: customer_id, items: items} = attrs
      now = DateTime.utc_now()

      {:ok,
       %__MODULE__{
         id: id,
         customer_id: customer_id,
         items: items,
         total: Enum.reduce(items, 0, fn i, acc -> acc + i.quantity * i.unit_price end),
         status: :created,
         created_at: now,
         updated_at: now
       }}
    end
  end

  def new(_), do: {:error, :formato_invalido}

  # Verifica se o pedido está no status esperado para uma operação
  def ensure_status(%__MODULE__{status: status}, status), do: :ok
  def ensure_status(%__MODULE__{status: status}, _), do: {:error, {:status_invalido_para_operacao, status}}

  # Aplica uma transição respeitando a máquina de estados
  def transition(%__MODULE__{status: from} = order, to) do
    cond do
      to not in @statuses ->
        {:error, {:status_invalido, to}}

      to in Map.fetch!(@transitions, from) ->
        {:ok, %{order | status: to, updated_at: DateTime.utc_now()}}

      true ->
        {:error, {:transicao_invalida, from, to}}
    end
  end

  # --- validações ------------------------------------------------------------

  defp validate_id(id) when is_binary(id) do
    if Regex.match?(@id_regex, id), do: :ok, else: {:error, {:campo_invalido, :id}}
  end

  defp validate_id(_), do: {:error, {:campo_invalido, :id}}

  defp validate_non_empty_string(value, _field) when is_binary(value) and value != "", do: :ok
  defp validate_non_empty_string(_value, field), do: {:error, {:campo_invalido, field}}

  defp validate_items(items) when is_list(items) and items != [] do
    if Enum.all?(items, &valid_item?/1),
      do: :ok,
      else: {:error, {:campo_invalido, :items}}
  end

  defp validate_items(_), do: {:error, {:campo_invalido, :items}}

  defp valid_item?(%{sku: sku, quantity: q, unit_price: p}) do
    validate_non_empty_string(sku, :sku) == :ok and
      is_integer(q) and q > 0 and
      is_integer(p) and p >= 0
  end

  defp valid_item?(_), do: false
end

defmodule Ecommerce.Store do
  @moduledoc """
  Persistência em disco. A escrita é atômica: grava em arquivo temporário
  e depois renomeia, evitando arquivos corrompidos em caso de queda.
  """

  # Carrega o mapa de pedidos. Arquivo inexistente = estado vazio.
  def load(path) do
    case File.read(path) do
      {:ok, bin} ->
        try do
          case :erlang.binary_to_term(bin, [:safe]) do
            data when is_map(data) -> {:ok, data}
            _ -> {:error, :dados_corrompidos}
          end
        rescue
          ArgumentError -> {:error, :dados_corrompidos}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Salva de forma atômica
  def save(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end

defmodule Ecommerce.Notifier do
  @moduledoc """
  Distribui notificações para processos assinantes.
  Monitora os assinantes para detectar encerramentos inesperados e
  removê-los da lista de destinatários.
  """
  use GenServer
  require Logger

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  # Assina notificações: o assinante receberá {:ecommerce_notification, evento}
  def subscribe(pid \\ self()) when is_pid(pid) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  def notify(event) do
    GenServer.cast(__MODULE__, {:notify, event})
  end

  @impl true
  def init(_), do: {:ok, %{subscribers: %{}}}

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  @impl true
  def handle_cast({:notify, event}, state) do
    Logger.info("Notificação: #{inspect(event)}")

    Enum.each(state.subscribers, fn {_ref, pid} ->
      send(pid, {:ecommerce_notification, event})
    end)

    {:noreply, state}
  end

  # Assinante encerrou: remove da lista e registra se foi inesperado
  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    if reason != :normal do
      Logger.warning("Assinante #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}")
    end

    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end

defmodule Ecommerce.OrderManager do
  @moduledoc """
  Gerencia pedidos com estado em memória, persistido a cada alteração.

  Pagamentos são executados por processos workers monitorados. Se um worker
  encerrar sem entregar resultado, o encerramento é detectado, notificado e o
  pedido permanece em :created, podendo ser cobrado novamente.
  """
  use GenServer
  require Logger
  alias Ecommerce.{Notifier, Order, Store}

  @high_value_threshold 100_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def transition(id, status), do: GenServer.call(__MODULE__, {:transition, id, status})
  def pay(id), do: GenServer.call(__MODULE__, {:pay, id})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :data_file, "orders.bin")
    # Função de pagamento injetável (útil para testes/integração)
    payment_fun = Keyword.get(opts, :payment_fun, fn _order -> :ok end)

    case Store.load(path) do
      {:ok, orders} ->
        Logger.info("#{map_size(orders)} pedido(s) recuperado(s) de #{path}")
        {:ok, %{path: path, orders: orders, workers: %{}, payment_fun: payment_fun}}

      {:error, reason} ->
        {:stop, {:falha_ao_carregar, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, order} <- Order.new(attrs),
         :ok <- ensure_new_id(state, order.id),
         {:ok, state} <- commit(state, Map.put(state.orders, order.id, order)) do
      Notifier.notify({:pedido_criado, order.id})

      if order.total >= @high_value_threshold do
        Notifier.notify({:pedido_alto_valor, order.id, order.total})
      end

      {:reply, {:ok, order}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch(state, id), state}
  end

  def handle_call({:transition, id, status}, _from, state) do
    with {:ok, order} <- fetch(state, id),
         {:ok, updated} <- Order.transition(order, status),
         {:ok, state} <- commit(state, Map.put(state.orders, id, updated)) do
      Notifier.notify({:status_alterado, id, order.status, status})
      {:reply, {:ok, updated}, state}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:pay, id}, _from, state) do
    with {:ok, order} <- fetch(state, id),
         :ok <- Order.ensure_status(order, :created),
         :ok <- ensure_no_worker(state, id) do
      manager = self()
      payment_fun = state.payment_fun

      # Worker monitorado: sua morte inesperada será detectada no handle_info/2
      {_pid, ref} =
        spawn_monitor(fn ->
          result =
            try do
              payment_fun.(order)
            rescue
              e -> {:error, Exception.message(e)}
            end

          send(manager, {:pagamento_resultado, id, result})
        end)

      {:reply, :ok, %{state | workers: Map.put(state.workers, ref, id)}}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  # Resultado do pagamento entregue pelo worker
  @impl true
  def handle_info({:pagamento_resultado, id, result}, state) do
    state = release_worker(state, id)

    case settle_payment(state, id, result) do
      {:ok, new_state} ->
        Notifier.notify({:pagamento_aprovado, id})
        {:noreply, new_state}

      {:error, reason} ->
        Notifier.notify({:pagamento_falhou, id, reason})
        {:noreply, state}
    end
  end

  # Worker encerrou. Como o resultado é tratado antes (com demonitor/flush),
  # qualquer DOWN que chegue aqui é um encerramento inesperado.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.workers, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, workers} ->
        Logger.error("Worker de pagamento do pedido #{id} encerrou: #{inspect(reason)}")
        Notifier.notify({:processo_encerrado_inesperadamente, id, reason})
        {:noreply, %{state | workers: workers}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # --- funções privadas ------------------------------------------------------

  defp settle_payment(state, id, :ok) do
    with {:ok, order} <- fetch(state, id),
         {:ok, paid} <- Order.transition(order, :paid),
         {:ok, new_state} <- commit(state, Map.put(state.orders, id, paid)) do
      {:ok, new_state}
    end
  end

  defp settle_payment(_state, _id, {:error, reason}), do: {:error, reason}
  defp settle_payment(_state, _id, other), do: {:error, {:resultado_invalido, other}}

  defp release_worker(state, id) do
    case Enum.find(state.workers, fn {_ref, wid} -> wid == id end) do
      {ref, _} ->
        Process.demonitor(ref, [:flush])
        %{state | workers: Map.delete(state.workers, ref)}

      nil ->
        state
    end
  end

  defp ensure_no_worker(state, id) do
    if Enum.any?(state.workers, fn {_ref, wid} -> wid == id end),
      do: {:error, :pagamento_em_andamento},
      else: :ok
  end

  defp ensure_new_id(state, id) do
    if Map.has_key?(state.orders, id),
      do: {:error, :pedido_ja_existe},
      else: :ok
  end

  defp fetch(%{orders: orders}, id) when is_binary(id) do
    case Map.fetch(orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :nao_encontrado}
    end
  end

  defp fetch(_state, _id), do: {:error, :id_invalido}

  # Persiste antes de aceitar a alteração em memória
  defp commit(state, orders) do
    case Store.save(state.path, orders) do
      :ok -> {:ok, %{state | orders: orders}}
      {:error, reason} -> {:error, {:persistencia, reason}}
    end
  end
end

defmodule Ecommerce.Supervisor do
  @moduledoc """
  Supervisiona Notifier e OrderManager. Estratégia :rest_for_one, pois o
  OrderManager depende do Notifier. Se o OrderManager cair, ele é reiniciado
  e recarrega os pedidos do arquivo persistido.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Ecommerce.Notifier, []},
      {Ecommerce.OrderManager, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
