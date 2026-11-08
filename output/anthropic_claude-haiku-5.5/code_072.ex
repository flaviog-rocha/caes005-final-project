# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Permitir comunicação entre diferentes processos.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de pedidos de e-commerce em Elixir.
# Requisitos atendidos:
#   1. Novas tentativas com backoff exponencial para falhas temporárias (Ecommerce.Retry).
#   2. Expiração automática de pedidos pendentes via GenServer periódico (Ecommerce.Expirer).
#   3. Comunicação entre processos por pub/sub baseado em Registry (Ecommerce.Events).
#   4. Validação dos dados antes de qualquer operação (Ecommerce.Order).
#   5. Banco de dados fictício em memória com Agent (Ecommerce.FakeDB).
#      Não utiliza Ecto; a interface de FakeDB pode ser substituída por um repositório Ecto futuramente.
# Supervisão: Ecommerce.Supervisor com estratégia :one_for_one.

defmodule Ecommerce.Order do
  @moduledoc """
  Estrutura e regras puras de pedidos. Não possui efeitos colaterais,
  exceto a leitura do relógio do sistema na criação.
  """

  @enforce_keys [:id, :customer_id, :items, :status, :inserted_at, :expires_at]
  defstruct [:id, :customer_id, :items, :status, :inserted_at, :expires_at]

  # Máquina de estados: transições permitidas a partir de cada status.
  @transitions %{
    pending: [:paid, :cancelled, :expired],
    paid: [],
    cancelled: [],
    expired: []
  }

  @doc "Valida os atributos e cria um novo pedido pendente com expiração."
  def new(attrs, ttl_ms, now \\ now_ms())

  def new(attrs, ttl_ms, now) when is_map(attrs) and is_integer(ttl_ms) and ttl_ms > 0 do
    with {:ok, customer_id} <- fetch_string(attrs, :customer_id),
         {:ok, items} <- fetch_items(attrs) do
      {:ok,
       %__MODULE__{
         id: generate_id(),
         customer_id: customer_id,
         items: items,
         status: :pending,
         inserted_at: now,
         expires_at: now + ttl_ms
       }}
    end
  end

  def new(_attrs, _ttl_ms, _now),
    do: {:error, {:invalid, :attrs, "atributos devem ser um mapa e o TTL um inteiro positivo"}}

  @doc "Calcula o valor total do pedido em centavos."
  def total(%__MODULE__{items: items}) do
    Enum.reduce(items, 0, fn %{qty: qty, unit_price_cents: price}, acc -> acc + qty * price end)
  end

  @doc "Aplica uma transição de status, se permitida pela máquina de estados."
  def transition(%__MODULE__{status: from} = order, to) do
    if to in Map.fetch!(@transitions, from) do
      {:ok, %{order | status: to}}
    else
      {:error, {:invalid_transition, from, to}}
    end
  end

  @doc "Indica se um pedido pendente já passou do instante de expiração."
  def expired?(%__MODULE__{status: :pending, expires_at: expires_at}, now), do: now >= expires_at
  def expired?(%__MODULE__{}, _now), do: false

  @doc "Retorna o tempo atual em milissegundos (relógio de parede, comparável entre processos)."
  def now_ms, do: System.system_time(:millisecond)

  defp fetch_string(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _ -> {:error, {:invalid, key, "deve ser uma string não vazia"}}
    end
  end

  defp fetch_items(attrs) do
    case Map.get(attrs, :items) do
      [_ | _] = items -> validate_items(items)
      _ -> {:error, {:invalid, :items, "deve ser uma lista não vazia"}}
    end
  end

  defp validate_items(items) do
    result =
      Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
        case validate_item(item) do
          {:ok, valid} -> {:cont, {:ok, [valid | acc]}}
          error -> {:halt, error}
        end
      end)

    case result do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp validate_item(%{sku: sku, qty: qty, unit_price_cents: price})
       when is_binary(sku) and byte_size(sku) > 0 and is_integer(qty) and qty > 0 and
              is_integer(price) and price >= 0 do
    {:ok, %{sku: sku, qty: qty, unit_price_cents: price}}
  end

  defp validate_item(_),
    do:
      {:error,
       {:invalid, :item, "sku (string), qty (> 0) e unit_price_cents (>= 0) são obrigatórios"}}

  defp generate_id do
    time = Integer.to_string(System.system_time(:nanosecond), 36)
    uniq = Integer.to_string(System.unique_integer([:positive]), 36)
    "ord_" <> time <> "_" <> uniq
  end
end

defmodule Ecommerce.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com Agent.
  Permite injetar falhas temporárias para simular indisponibilidade.
  Todas as operações de escrita de atualização recebem funções puras,
  o que garante atomicidade (a função executa dentro do processo do Agent).
  """
  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{orders: %{}, fail_next: 0} end, name: __MODULE__)
  end

  @doc "Insere um pedido."
  def insert(%Ecommerce.Order{} = order) do
    run(fn state -> {{:ok, order}, put_order(state, order)} end)
  end

  @doc "Busca um pedido pelo id."
  def fetch(id) do
    run(fn state ->
      reply =
        case Map.fetch(state.orders, id) do
          {:ok, order} -> {:ok, order}
          :error -> {:error, :not_found}
        end

      {reply, state}
    end)
  end

  @doc "Atualiza atomicamente um pedido aplicando uma função pura que retorna {:ok, novo} ou {:error, motivo}."
  def update(id, fun) when is_function(fun, 1) do
    run(fn state ->
      case Map.fetch(state.orders, id) do
        :error ->
          {{:error, :not_found}, state}

        {:ok, order} ->
          case fun.(order) do
            {:ok, new_order} -> {{:ok, new_order}, put_order(state, new_order)}
            {:error, _reason} = error -> {error, state}
          end
      end
    end)
  end

  @doc "Lista todos os pedidos armazenados."
  def list do
    run(fn state -> {{:ok, Map.values(state.orders)}, state} end)
  end

  @doc "Configura as próximas `n` operações para falharem temporariamente (uso em testes e simulações)."
  def inject_failures(n) when is_integer(n) and n >= 0 do
    Agent.update(__MODULE__, &%{&1 | fail_next: n})
  end

  defp run(fun) do
    Agent.get_and_update(__MODULE__, fn state ->
      if state.fail_next > 0 do
        {{:error, :temporary_failure}, %{state | fail_next: state.fail_next - 1}}
      else
        fun.(state)
      end
    end)
  end

  defp put_order(state, order) do
    %{state | orders: Map.put(state.orders, order.id, order)}
  end
end

defmodule Ecommerce.Retry do
  @moduledoc """
  Novas tentativas com backoff exponencial e jitter para falhas temporárias.
  Apenas erros `{:error, :temporary_failure}` são repetidos; demais resultados
  são retornados imediatamente.
  """

  @defaults [max_attempts: 4, base_delay_ms: 50, max_delay_ms: 1_000]

  @doc "Executa `fun` com tentativas. Opções: :max_attempts, :base_delay_ms, :max_delay_ms."
  def run(fun, opts \\ []) when is_function(fun, 0) do
    config = Keyword.merge(@defaults, opts)
    attempt(fun, 1, config)
  end

  defp attempt(fun, n, config) do
    case fun.() do
      {:error, :temporary_failure} = error ->
        if n < config[:max_attempts] do
          Process.sleep(backoff(n, config))
          attempt(fun, n + 1, config)
        else
          error
        end

      result ->
        result
    end
  end

  defp backoff(n, config) do
    exponential = config[:base_delay_ms] * Integer.pow(2, n - 1)
    min(exponential, config[:max_delay_ms]) + :rand.uniform(config[:base_delay_ms])
  end
end

defmodule Ecommerce.Events do
  @moduledoc """
  Comunicação entre processos por pub/sub usando Registry com chaves duplicadas.
  Assinantes recebem mensagens no formato `{:event, topic, message}`.
  """

  @doc "Registra o processo chamador como assinante de um tópico."
  def subscribe(topic) do
    case Registry.register(__MODULE__, topic, nil) do
      {:ok, _owner} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Envia uma mensagem a todos os assinantes de um tópico."
  def broadcast(topic, message) do
    Registry.dispatch(__MODULE__, topic, fn entries ->
      for {pid, _value} <- entries, do: send(pid, {:event, topic, message})
    end)

    :ok
  end
end

defmodule Ecommerce.Orders do
  @moduledoc """
  API pública de pedidos. Valida entradas, aplica novas tentativas em falhas
  temporárias do banco e publica eventos aos assinantes.
  """

  alias Ecommerce.{Order, FakeDB, Retry, Events}

  @default_ttl_ms 15 * 60 * 1_000

  @doc "Cria um pedido pendente com expiração (TTL padrão de 15 minutos)."
  def create(attrs, ttl_ms \\ @default_ttl_ms) do
    with {:ok, order} <- Order.new(attrs, ttl_ms),
         {:ok, saved} <- Retry.run(fn -> FakeDB.insert(order) end) do
      Events.broadcast(:orders, {:created, saved.id})
      {:ok, saved}
    end
  end

  @doc "Busca um pedido pelo id."
  def get(id) do
    with :ok <- validate_id(id) do
      Retry.run(fn -> FakeDB.fetch(id) end)
    end
  end

  @doc "Marca um pedido pendente como pago."
  def pay(id) do
    with :ok <- validate_id(id),
         {:ok, order} <-
           Retry.run(fn -> FakeDB.update(id, &Order.transition(&1, :paid)) end) do
      Events.broadcast(:orders, {:paid, order.id})
      {:ok, order}
    end
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_), do: {:error, {:invalid, :id, "deve ser uma string não vazia"}}
end

defmodule Ecommerce.Expirer do
  @moduledoc """
  Processo periódico que expira pedidos pendentes vencidos.
  Cada expiração é atômica (verificada novamente dentro do FakeDB), evitando
  conflito com um pagamento concorrente.
  """
  use GenServer

  alias Ecommerce.{Order, FakeDB, Retry, Events}

  @default_interval_ms 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Força uma varredura imediata e retorna {:ok, quantidade_expirada} ou erro."
  def sweep_now, do: GenServer.call(__MODULE__, :sweep)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_call(:sweep, _from, state) do
    {:reply, sweep(), state}
  end

  @impl true
  def handle_info(:tick, state) do
    sweep()
    schedule(state.interval)
    {:noreply, state}
  end

  @doc "Varre os pedidos e expira os que venceram."
  def sweep(now \\ Order.now_ms()) do
    case Retry.run(fn -> FakeDB.list() end) do
      {:ok, orders} ->
        count =
          orders
          |> Enum.filter(&Order.expired?(&1, now))
          |> Enum.count(fn order -> expire(order.id, now) == :ok end)

        {:ok, count}

      error ->
        error
    end
  end

  defp expire(id, now) do
    result =
      Retry.run(fn ->
        FakeDB.update(id, fn order ->
          if Order.expired?(order, now) do
            Order.transition(order, :expired)
          else
            {:error, :not_expired}
          end
        end)
      end)

    case result do
      {:ok, order} ->
        Events.broadcast(:orders, {:expired, order.id})
        :ok

      _ ->
        :skip
    end
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end

defmodule Ecommerce.Supervisor do
  @moduledoc """
  Supervisor raiz do sistema de pedidos (estratégia :one_for_one).
  Ordem de inicialização: Registry de eventos, banco fictício e expirador.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: Ecommerce.Events},
      Ecommerce.FakeDB,
      {Ecommerce.Expirer, Keyword.get(opts, :expirer, [])}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
