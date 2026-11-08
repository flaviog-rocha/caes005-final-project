# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de pedidos para e-commerce.
#
# Requisitos atendidos:
#   1. Notificações: eventos enviados aos processos assinantes quando condições
#      são atingidas (pedido de alto valor, mudança de status, falha de pagamento etc.).
#   2. Detecção de encerramentos inesperados: processos assinantes e workers de
#      captura de pagamento são monitorados; saídas anormais geram notificações
#      e disparam nova tentativa de processamento.
#   3. Persistência: o estado dos pedidos é gravado em disco de forma atômica
#      (arquivo temporário + rename) a cada alteração e recarregado na inicialização.
#      Pagamentos pendentes são retomados após reinicialização.
#   4. Validação: todos os dados de entrada são validados antes de qualquer operação.
#
# Uso básico:
#
#   {:ok, _pid} = Ecommerce.OrderSystem.start_link(data_path: "pedidos.bin")
#   :ok = Ecommerce.OrderSystem.subscribe(self())
#
#   {:ok, _} =
#     Ecommerce.OrderSystem.create_order(%{
#       id: "PED-1",
#       customer_id: "CLI-9",
#       items: [%{sku: "ABC", qty: 2, unit_price: 60_000}]
#     })
#
#   {:ok, _} = Ecommerce.OrderSystem.transition("PED-1", :paid)
#   # => o processo assinante recebe mensagens {:order_notification, tipo, payload}

defmodule Ecommerce.OrderSystem.Validator do
  @moduledoc """
  Validação pura dos dados de pedidos e das transições de status.
  Valores monetários são inteiros em centavos.
  """

  # Transições de status permitidas (máquina de estados do pedido).
  @transitions %{
    pending: [:paid, :cancelled],
    paid: [:shipped, :cancelled],
    shipped: [:delivered],
    delivered: [],
    cancelled: []
  }

  @doc "Valida os atributos de criação de um pedido e devolve o pedido normalizado."
  def validate_new_order(attrs) when is_map(attrs) do
    with {:ok, id} <- required_string(attrs, :id),
         {:ok, customer_id} <- required_string(attrs, :customer_id),
         {:ok, items} <- validate_items(Map.get(attrs, :items)) do
      {:ok,
       %{
         id: id,
         customer_id: customer_id,
         items: items,
         total: total(items),
         status: :pending,
         # Estado da captura de pagamento: :none | :pending | :done | :failed
         capture: :none
       }}
    end
  end

  def validate_new_order(_), do: {:error, :invalid_payload}

  @doc "Verifica se a transição de status do pedido é permitida."
  def validate_transition(%{status: current}, new_status) do
    allowed = Map.get(@transitions, current, [])

    cond do
      not is_atom(new_status) -> {:error, :invalid_status}
      not Enum.member?(allowed, new_status) -> {:error, {:invalid_transition, current, new_status}}
      true -> :ok
    end
  end

  # -- Privadas ---------------------------------------------------------------

  defp required_string(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: {:error, {:invalid_field, key}}, else: {:ok, value}

      _ ->
        {:error, {:invalid_field, key}}
    end
  end

  defp validate_items(items) when is_list(items) and items != [] do
    items
    |> Enum.reduce_while({:ok, []}, fn item, {:ok, acc} ->
      case validate_item(item) do
        {:ok, valid} -> {:cont, {:ok, [valid | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end

  defp validate_items(_), do: {:error, {:invalid_field, :items}}

  defp validate_item(%{sku: sku, qty: qty, unit_price: price})
       when is_binary(sku) and sku != "" and is_integer(qty) and qty > 0 and
              is_integer(price) and price > 0 do
    {:ok, %{sku: sku, qty: qty, unit_price: price}}
  end

  defp validate_item(_), do: {:error, {:invalid_field, :item}}

  defp total(items) do
    Enum.reduce(items, 0, fn item, acc -> acc + item.qty * item.unit_price end)
  end
end

defmodule Ecommerce.OrderSystem.Storage do
  @moduledoc "Persistência em disco com escrita atômica."

  @doc "Carrega o estado salvo. Retorna mapa vazio se o arquivo não existir."
  def load(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          {:ok, :erlang.binary_to_term(binary, [:safe])}
        rescue
          ArgumentError -> {:error, :corrupted_state}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Grava o estado em arquivo temporário e troca atomicamente pelo definitivo."
  def save(path, data) do
    tmp = path <> ".tmp"

    with :ok <- path |> Path.dirname() |> File.mkdir_p(),
         :ok <- File.write(tmp, :erlang.term_to_binary(data), [:binary, :sync]),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end

defmodule Ecommerce.OrderSystem do
  @moduledoc """
  Servidor GenServer que coordena pedidos, notificações, monitoramento de
  processos e persistência.

  Opções de start_link/1:
    * `:name` - nome do processo (padrão: módulo)
    * `:data_path` - caminho do arquivo de persistência
    * `:high_value_threshold` - valor em centavos a partir do qual o pedido é de alto valor
    * `:max_capture_retries` - número máximo de tentativas de captura de pagamento
    * `:capture_fun` - função `(order -> :ok | {:error, reason})` que executa a captura
  """

  use GenServer

  alias Ecommerce.OrderSystem.{Storage, Validator}

  @default_opts [
    name: __MODULE__,
    data_path: "orders_state.bin",
    high_value_threshold: 100_000,
    max_capture_retries: 3,
    capture_fun: &__MODULE__.default_capture/1
  ]

  # -- API pública ------------------------------------------------------------

  @doc "Inicia o servidor de pedidos."
  def start_link(opts \\ []) do
    opts = Keyword.merge(@default_opts, opts)
    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc "Cria um novo pedido após validação."
  def create_order(server \\ __MODULE__, attrs) do
    GenServer.call(server, {:create, attrs})
  end

  @doc "Altera o status de um pedido após validação da transição."
  def transition(server \\ __MODULE__, id, new_status) do
    GenServer.call(server, {:transition, id, new_status})
  end

  @doc "Consulta um pedido pelo id."
  def get_order(server \\ __MODULE__, id) do
    GenServer.call(server, {:get, id})
  end

  @doc "Assina notificações. O processo é monitorado e removido se encerrar."
  def subscribe(server \\ __MODULE__, pid \\ self()) do
    GenServer.call(server, {:subscribe, pid})
  end

  @doc false
  # Captura padrão (simulada): sempre aprova o pagamento.
  def default_capture(_order), do: :ok

  # -- Callbacks --------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :data_path)

    case Storage.load(path) do
      {:ok, orders} ->
        state =
          opts
          |> Map.new()
          |> Map.merge(%{orders: orders, workers: %{}, subscribers: %{}, retries: %{}})

        # Recuperação: retoma capturas de pagamento que estavam em andamento.
        state =
          orders
          |> Map.values()
          |> Enum.filter(&(&1.capture == :pending))
          |> Enum.reduce(state, &start_worker(&2, &1))

        {:ok, state}

      {:error, reason} ->
        {:stop, {:storage_load_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, order} <- Validator.validate_new_order(attrs),
         :ok <- ensure_unique(state, order.id) do
      state =
        state
        |> put_order(order)
        |> notify(:order_created, %{order_id: order.id, total: order.total})
        |> maybe_notify_high_value(order)

      {:reply, {:ok, order}, persist(state)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:transition, id, new_status}, _from, state) do
    with {:ok, order} <- fetch_order(state, id),
         :ok <- Validator.validate_transition(order, new_status) do
      state = change_status(state, order, new_status)
      {:reply, {:ok, Map.fetch!(state.orders, id)}, persist(state)}
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch_order(state, id), state}
  end

  def handle_call({:subscribe, pid}, _from, state) when is_pid(pid) do
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  def handle_call({:subscribe, _invalid}, _from, state) do
    {:reply, {:error, :invalid_pid}, state}
  end

  @impl true
  # Resultado enviado por um worker de captura de pagamento.
  def handle_info({:capture_result, id, result}, state) do
    case pop_worker_by_id(state, id) do
      {nil, state} ->
        {:noreply, state}

      {ref, state} ->
        # Remove o monitor antes que o DOWN normal seja processado.
        Process.demonitor(ref, [:flush])

        state =
          case result do
            :ok -> mark_captured(state, id)
            {:error, reason} -> handle_capture_failure(state, id, reason)
            other -> handle_capture_failure(state, id, {:invalid_result, other})
          end

        {:noreply, persist(state)}
    end
  end

  # Encerramento de processo monitorado.
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    cond do
      Map.has_key?(state.workers, ref) ->
        # Worker ainda rastreado = encerrou sem enviar resultado: inesperado.
        {{id, _pid}, workers} = Map.pop(state.workers, ref)
        state = %{state | workers: workers}

        state =
          notify(state, :process_exit_unexpected, %{
            role: :capture_worker,
            order_id: id,
            pid: pid,
            reason: reason
          })

        state = handle_capture_failure(state, id, {:worker_exit, reason})
        {:noreply, persist(state)}

      Map.has_key?(state.subscribers, ref) ->
        state = %{state | subscribers: Map.delete(state.subscribers, ref)}

        state =
          if unexpected_exit?(reason) do
            notify(state, :process_exit_unexpected, %{
              role: :subscriber,
              pid: pid,
              reason: reason
            })
          else
            state
          end

        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Lógica interna ---------------------------------------------------------

  defp ensure_unique(state, id) do
    if Map.has_key?(state.orders, id), do: {:error, :duplicate_id}, else: :ok
  end

  defp fetch_order(state, id) do
    case Map.fetch(state.orders, id) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, :not_found}
    end
  end

  defp put_order(state, order) do
    %{state | orders: Map.put(state.orders, order.id, order)}
  end

  defp maybe_notify_high_value(state, order) do
    if order.total >= state.high_value_threshold do
      notify(state, :high_value_order, %{order_id: order.id, total: order.total})
    else
      state
    end
  end

  defp change_status(state, order, new_status) do
    updated = %{order | status: new_status}
    updated = if new_status == :paid, do: %{updated | capture: :pending}, else: updated

    state =
      state
      |> put_order(updated)
      |> notify(:status_changed, %{order_id: order.id, from: order.status, to: new_status})

    if new_status == :paid, do: start_worker(state, updated), else: state
  end

  defp start_worker(state, %{id: id, total: total}) do
    parent = self()
    capture = state.capture_fun

    # O worker não é linkado; o monitor detecta sua saída inesperada.
    {_pid, ref} =
      spawn_monitor(fn ->
        result = capture.(%{id: id, total: total})
        send(parent, {:capture_result, id, result})
      end)

    # Registra o worker pelo ref do monitor; o id é recuperado no DOWN.
    {pid, _} = Process.info(self(), :links) && {nil, nil}
    _ = pid
    %{state | workers: Map.put(state.workers, ref, {id, worker_pid(ref)})}
  end

  defp worker_pid(_ref), do: nil

  defp pop_worker_by_id(state, id) do
    case Enum.find(state.workers, fn {_ref, {wid, _pid}} -> wid == id end) do
      nil ->
        {nil, state}

      {ref, _} ->
        {ref, %{state | workers: Map.delete(state.workers, ref)}}
    end
  end

  defp mark_captured(state, id) do
    order = %{Map.fetch!(state.orders, id) | capture: :done}

    state
    |> put_order(order)
    |> Map.update!(:retries, &Map.delete(&1, id))
    |> notify(:payment_captured, %{order_id: id, total: order.total})
  end

  defp handle_capture_failure(state, id, reason) do
    attempts = Map.get(state.retries, id, 0) + 1
    state = %{state | retries: Map.put(state.retries, id, attempts)}
    order = Map.fetch!(state.orders, id)

    if attempts < state.max_capture_retries do
      start_worker(state, order)
    else
      state =
        put_order(state, %{order | capture: :failed})

      notify(state, :payment_failed, %{order_id: id, attempts: attempts, reason: reason})
    end
  end

  defp unexpected_exit?(:normal), do: false
  defp unexpected_exit?(:shutdown), do: false
  defp unexpected_exit?({:shutdown, _}), do: false
  defp unexpected_exit?(_), do: true

  defp notify(state, type, payload) do
    event = {:order_notification, type, payload}
    Enum.each(state.subscribers, fn {_ref, pid} -> send(pid, event) end)
    state
  end

  defp persist(state) do
    case Storage.save(state.data_path, state.orders) do
      :ok -> state
      {:error, reason} -> notify(state, :persistence_failed, %{reason: reason})
    end
  end
end
