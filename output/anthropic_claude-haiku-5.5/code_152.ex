# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# delivery_tracker.exs
#
# Gerenciamento de entregas e rastreamento em um único arquivo.
#
# Componentes:
#   - DeliveryTracker.Delivery   : entidade, validações e transições de status (funções puras)
#   - DeliveryTracker.Store      : banco de dados fictício em memória (GenServer), com atomicidade
#                                  por serialização, índice de códigos de rastreio e registro de
#                                  operações idempotentes
#   - DeliveryTracker.Limiter    : semáforo que limita operações simultâneas (GenServer)
#   - DeliveryTracker            : API pública (cadastrar, consultar, atualizar, remover, rastrear)
#   - DeliveryTracker.Migrations : migração Ecto opcional, compilada apenas se Ecto estiver disponível
#
# Execução exemplo:
#   {:ok, _sup} = DeliveryTracker.start_link(max_concurrency: 5)
#   {:ok, d} = DeliveryTracker.create(%{recipient: "Ana", address: "Rua A, 10"}, "op-001")
#   {:ok, d} = DeliveryTracker.update(d.id, %{status: :in_transit}, "op-002")
#   {:ok, d} = DeliveryTracker.track(d.tracking_code)

defmodule DeliveryTracker.Delivery do
  @moduledoc """
  Entidade de entrega e regras de domínio.
  Todas as funções são puras e não dependem de processos.
  """

  @enforce_keys [:id, :tracking_code, :recipient, :address]
  defstruct [
    :id,
    :tracking_code,
    :recipient,
    :address,
    :inserted_at,
    :updated_at,
    status: :pending,
    events: []
  ]

  # Transições de status permitidas: status atual => lista de próximos status válidos
  @transitions %{
    pending: [:in_transit, :cancelled],
    in_transit: [:delivered, :cancelled],
    delivered: [],
    cancelled: []
  }
  @statuses Map.keys(@transitions)

  @doc "Constrói uma nova entrega a partir de atributos validados."
  def build(attrs) when is_map(attrs) do
    with {:ok, recipient} <- fetch_text(attrs, :recipient),
         {:ok, address} <- fetch_text(attrs, :address) do
      now = DateTime.utc_now()

      {:ok,
       %__MODULE__{
         id: System.unique_integer([:positive, :monotonic]),
         tracking_code: tracking_code(),
         recipient: recipient,
         address: address,
         inserted_at: now,
         updated_at: now,
         events: [event(:pending, now)]
       }}
    end
  end

  @doc """
  Atualiza campos (:recipient, :address apenas em :pending) e/ou o status
  (somente transições válidas).
  """
  def update(%__MODULE__{} = delivery, attrs) when is_map(attrs) do
    with {:ok, delivery} <- put_fields(delivery, attrs),
         {:ok, delivery} <- put_status(delivery, Map.get(attrs, :status)) do
      {:ok, %{delivery | updated_at: DateTime.utc_now()}}
    end
  end

  defp put_fields(delivery, attrs) do
    Enum.reduce_while([:recipient, :address], {:ok, delivery}, fn field, {:ok, acc} ->
      case Map.fetch(attrs, field) do
        :error ->
          {:cont, {:ok, acc}}

        {:ok, value} ->
          if acc.status != :pending do
            {:halt, {:error, {:not_editable, acc.status}}}
          else
            case fetch_text(%{field => value}, field) do
              {:ok, text} -> {:cont, {:ok, Map.put(acc, field, text)}}
              error -> {:halt, error}
            end
          end
      end
    end)
  end

  defp put_status(delivery, nil), do: {:ok, delivery}

  defp put_status(delivery, status) when status in @statuses do
    if status in Map.fetch!(@transitions, delivery.status) do
      now = DateTime.utc_now()

      {:ok,
       %{
         delivery
         | status: status,
           events: delivery.events ++ [event(status, now)]
       }}
    else
      {:error, {:invalid_transition, delivery.status, status}}
    end
  end

  defp put_status(_delivery, status), do: {:error, {:invalid_status, status}}

  defp fetch_text(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> {:error, {:invalid, key}}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, {:invalid, key}}
    end
  end

  defp event(status, at), do: %{status: status, at: at}

  # Código de rastreio aleatório (ex.: "BR3F9A0C12D4")
  defp tracking_code do
    "BR" <> Base.encode16(:crypto.strong_rand_bytes(5))
  end
end

defmodule DeliveryTracker.Store do
  @moduledoc """
  Banco de dados fictício em memória.

  Como todas as operações passam por um único processo, cada chamada é atômica
  e consistente, simulando o comportamento de um banco transacional simples.

  Mantém:
    - `deliveries`: entregas indexadas por id
    - `codes`: conjunto de códigos de rastreio (unicidade)
    - `ops`: registro de operações idempotentes (chave => {:in_progress, monitor} | {:done, result})
  """
  use GenServer

  alias DeliveryTracker.Delivery

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: Keyword.get(opts, :name, __MODULE__))
  end

  def insert(%Delivery{} = delivery), do: call({:insert, delivery})
  def get(id), do: call({:get, id})
  def by_code(code), do: call({:by_code, code})
  def list, do: call(:list)
  def update(id, fun) when is_function(fun, 1), do: call({:update, id, fun})
  def delete(id), do: call({:delete, id})

  # Controle de idempotência
  def claim(key), do: call({:claim, key})
  def finish(key, result), do: call({:finish, key, result})
  def release(key), do: call({:release, key})

  defp call(message), do: GenServer.call(__MODULE__, message)

  @impl true
  def init(:ok) do
    {:ok, %{deliveries: %{}, codes: MapSet.new(), ops: %{}}}
  end

  @impl true
  def handle_call({:insert, delivery}, _from, state) do
    if MapSet.member?(state.codes, delivery.tracking_code) do
      {:reply, {:error, :duplicate_tracking_code}, state}
    else
      state = %{
        state
        | deliveries: Map.put(state.deliveries, delivery.id, delivery),
          codes: MapSet.put(state.codes, delivery.tracking_code)
      }

      {:reply, {:ok, delivery}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch(state.deliveries, id), state}
  end

  def handle_call({:by_code, code}, _from, state) do
    found = Enum.find_value(state.deliveries, fn {_id, d} -> d.tracking_code == code && d end)

    reply =
      case found do
        nil -> {:error, :not_found}
        delivery -> {:ok, delivery}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    list = state.deliveries |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, list}, state}
  end

  def handle_call({:update, id, fun}, _from, state) do
    case fetch(state.deliveries, id) do
      {:ok, delivery} ->
        case fun.(delivery) do
          {:ok, updated} ->
            state = %{state | deliveries: Map.put(state.deliveries, id, updated)}
            {:reply, {:ok, updated}, state}

          {:error, _} = error ->
            {:reply, error, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case fetch(state.deliveries, id) do
      {:ok, delivery} ->
        state = %{
          state
          | deliveries: Map.delete(state.deliveries, id),
            codes: MapSet.delete(state.codes, delivery.tracking_code)
        }

        {:reply, {:ok, delivery}, state}

      error ->
        {:reply, error, state}
    end
  end

  # Idempotência: reserva a chave para o processo chamador.
  # Monitora o chamador para liberar a reserva caso ele morra antes de concluir.
  def handle_call({:claim, key}, {pid, _}, state) do
    case Map.get(state.ops, key) do
      nil ->
        mref = Process.monitor(pid)
        {:reply, :ok, put_op(state, key, {:in_progress, mref})}

      {:in_progress, _mref} ->
        {:reply, :in_progress, state}

      {:done, result} ->
        {:reply, {:done, result}, state}
    end
  end

  def handle_call({:finish, key, result}, _from, state) do
    case Map.get(state.ops, key) do
      {:in_progress, mref} -> Process.demonitor(mref, [:flush])
      _ -> :ok
    end

    {:reply, :ok, put_op(state, key, {:done, result})}
  end

  def handle_call({:release, key}, _from, state) do
    case Map.get(state.ops, key) do
      {:in_progress, mref} ->
        Process.demonitor(mref, [:flush])
        {:reply, :ok, %{state | ops: Map.delete(state.ops, key)}}

      _ ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, _reason}, state) do
    # Libera reservas de chaves cujo processo dono terminou sem concluir
    ops =
      state.ops
      |> Enum.reject(fn {_key, value} -> value == {:in_progress, mref} end)
      |> Map.new()

    {:noreply, %{state | ops: ops}}
  end

  defp fetch(map, id) do
    case Map.get(map, id) do
      nil -> {:error, :not_found}
      value -> {:ok, value}
    end
  end

  defp put_op(state, key, value), do: %{state | ops: Map.put(state.ops, key, value)}
end

defmodule DeliveryTracker.Limiter do
  @moduledoc """
  Semáforo que limita o número de operações executadas simultaneamente.

  - Operações excedentes ficam em fila (FIFO) até que um slot seja liberado.
  - Se a espera exceder o `timeout`, retorna `{:error, :limit_timeout}`.
  - Se o processo que detém um slot morrer, o slot é liberado automaticamente.
  """
  use GenServer

  def start_link(opts \\ []) do
    max = Keyword.get(opts, :max, 10)
    GenServer.start_link(__MODULE__, max, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Executa `fun` respeitando o limite de concorrência."
  def run(fun, opts \\ []) when is_function(fun, 0) do
    server = Keyword.get(opts, :server, __MODULE__)
    timeout = Keyword.get(opts, :timeout, 5_000)

    case GenServer.call(server, {:acquire, timeout}, :infinity) do
      {:ok, ticket} ->
        try do
          fun.()
        after
          GenServer.cast(server, {:release, ticket})
        end

      {:error, :timeout} ->
        {:error, :limit_timeout}
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call({:acquire, timeout}, {pid, _} = from, state) do
    ticket = make_ref()

    if map_size(state.active) < state.max do
      {:reply, {:ok, ticket}, grant(state, ticket, pid)}
    else
      timer = Process.send_after(self(), {:wait_timeout, ticket}, timeout)
      waiting = :queue.in({ticket, from, pid, timer}, state.waiting)
      {:noreply, %{state | waiting: waiting}}
    end
  end

  @impl true
  def handle_cast({:release, ticket}, state) do
    case Map.pop(state.active, ticket) do
      {nil, _} ->
        {:noreply, state}

      {mref, active} ->
        Process.demonitor(mref, [:flush])
        {:noreply, dispatch(%{state | active: active})}
    end
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, _reason}, state) do
    case Enum.find(state.active, fn {_ticket, m} -> m == mref end) do
      nil ->
        {:noreply, state}

      {ticket, _} ->
        {:noreply, dispatch(%{state | active: Map.delete(state.active, ticket)})}
    end
  end

  def handle_info({:wait_timeout, ticket}, state) do
    {matching, rest} =
      state.waiting
      |> :queue.to_list()
      |> Enum.split_with(fn {t, _from, _pid, _timer} -> t == ticket end)

    case matching do
      [{_, from, _, _}] ->
        GenServer.reply(from, {:error, :timeout})
        {:noreply, %{state | waiting: :queue.from_list(rest)}}

      [] ->
        {:noreply, state}
    end
  end

  defp grant(state, ticket, pid) do
    mref = Process.monitor(pid)
    %{state | active: Map.put(state.active, ticket, mref)}
  end

  # Concede slots livres aos próximos da fila
  defp dispatch(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, {ticket, from, pid, timer}}, rest} ->
          Process.cancel_timer(timer)
          GenServer.reply(from, {:ok, ticket})
          dispatch(grant(%{state | waiting: rest}, ticket, pid))

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule DeliveryTracker do
  @moduledoc """
  API pública do gerenciamento de entregas.

  - Operações de escrita (`create/2`, `update/3`, `delete/2`) exigem uma
    `idempotency_key`. Uma mesma chave é executada no máximo uma vez com
    sucesso: repetições retornam o resultado já armazenado, e chamadas
    concorrentes com a mesma chave recebem `{:error, :operation_in_progress}`.
    Falhas liberam a chave, permitindo nova tentativa.
  - Todas as operações passam pelo `Limiter`, que limita a concorrência.
  """

  alias DeliveryTracker.{Delivery, Limiter, Store}

  @doc """
  Inicia a árvore de supervisão (Store + Limiter).

  Opções:
    - `:max_concurrency` - número máximo de operações simultâneas (padrão: 10)
  """
  def start_link(opts \\ []) do
    children = [
      {Store, name: Store},
      {Limiter, name: Limiter, max: Keyword.get(opts, :max_concurrency, 10)}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: DeliveryTracker.Supervisor)
  end

  # Cadastro

  @doc "Cadastra uma entrega. Retorna `{:ok, delivery}` ou `{:error, reason}`."
  def create(attrs, idempotency_key) when is_map(attrs) do
    execute(idempotency_key, fn ->
      with {:ok, delivery} <- Delivery.build(attrs) do
        Store.insert(delivery)
      end
    end)
  end

  # Consulta

  @doc "Consulta uma entrega pelo id."
  def get(id), do: Limiter.run(fn -> Store.get(id) end)

  @doc "Consulta uma entrega pelo código de rastreio."
  def track(tracking_code) when is_binary(tracking_code) do
    Limiter.run(fn -> Store.by_code(tracking_code) end)
  end

  @doc "Lista todas as entregas."
  def list, do: Limiter.run(fn -> Store.list() end)

  # Atualização

  @doc """
  Atualiza campos e/ou status de uma entrega.
  Ex.: `update(id, %{status: :in_transit}, "op-42")`
  """
  def update(id, attrs, idempotency_key) when is_map(attrs) do
    execute(idempotency_key, fn ->
      Store.update(id, &Delivery.update(&1, attrs))
    end)
  end

  # Remoção

  @doc "Remove uma entrega."
  def delete(id, idempotency_key) do
    execute(idempotency_key, fn -> Store.delete(id) end)
  end

  # Núcleo de idempotência + limite de concorrência

  defp execute(key, fun) when is_binary(key) and key != "" do
    case Store.claim(key) do
      {:done, result} ->
        result

      :in_progress ->
        {:error, :operation_in_progress}

      :ok ->
        result =
          try do
            Limiter.run(fun)
          catch
            kind, reason ->
              Store.release(key)
              :erlang.raise(kind, reason, __STACKTRACE__)
          end

        case result do
          {:ok, _} = ok ->
            Store.finish(key, ok)
            ok

          other ->
            # Falhas não são registradas, permitindo nova tentativa com a mesma chave
            Store.release(key)
            other
        end
    end
  end

  defp execute(_key, _fun), do: {:error, :idempotency_key_required}
end

# Migração Ecto opcional.
# Compilada somente se o módulo Ecto.Migration estiver disponível no projeto.
if Code.ensure_loaded?(Ecto.Migration) do
  defmodule DeliveryTracker.Migrations.CreateDeliveries do
    use Ecto.Migration

    def change do
      create table(:deliveries) do
        add :tracking_code, :string, null: false, size: 32
        add :recipient, :string, null: false
        add :address, :text, null: false
        add :status, :string, null: false, default: "pending"
        add :events, {:array, :map}, null: false, default: []
        timestamps(type: :utc_datetime_usec)
      end

      create unique_index(:deliveries, [:tracking_code])

      create table(:delivery_operations, primary_key: false) do
        add :idempotency_key, :string, primary_key: true
        add :result, :binary, null: false
        timestamps(type: :utc_datetime_usec)
      end
    end
  end
end
