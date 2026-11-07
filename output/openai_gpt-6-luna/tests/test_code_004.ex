# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, devendo ter os seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Executar determinadas operações periodicamente.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir a execução de operações simultâneas.
#
# O código deve ser funcional e organizado e acompanhado de testes automatizados que verifiquem os requisitos.

A implementação abaixo usa um `GenServer` como proprietário do estado: cada operação é aplicada atomicamente, enquanto vários processos podem fazer chamadas simultâneas. O serviço mantém o histórico em ordem, permite executar a verificação periódica por timer e oferece `run_periodic/1` para testes ou execução manual.

### Estrutura

```text
delivery_tracker/
├── mix.exs
├── lib/
│   └── delivery_tracker.ex
└── test/
    ├── delivery_tracker_test.exs
    └── test_helper.exs
```

### `mix.exs`

```elixir
defmodule DeliveryTracker.MixProject do
  use Mix.Project

  def project do
    [
      app: :delivery_tracker,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end
end
```

### `lib/delivery_tracker.ex`

```elixir
defmodule DeliveryTracker.Delivery do
  @moduledoc "Representa uma entrega."

  defstruct [
    :id,
    :status,
    :expected_at,
    :inserted_at,
    :updated_at
  ]

  @type status :: :pending | :picked_up | :in_transit | :delayed | :delivered

  @type t :: %__MODULE__{
          id: String.t(),
          status: status(),
          expected_at: integer() | nil,
          inserted_at: integer(),
          updated_at: integer()
        }
end

defmodule DeliveryTracker do
  @moduledoc """
  Gerencia entregas, transições de status e o histórico de operações.

  `expected_at` e os timestamps são Unix em milissegundos. Entregas vencidas
  que ainda não foram entregues são marcadas como `:delayed`.
  """

  use GenServer

  alias DeliveryTracker.Delivery

  @statuses [:pending, :picked_up, :in_transit, :delayed, :delivered]

  @transitions %{
    pending: [:picked_up],
    picked_up: [:in_transit],
    in_transit: [:delivered],
    delayed: [:picked_up, :in_transit, :delivered],
    delivered: []
  }

  # API pública

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Cria uma entrega. As opções aceitas são `expected_at`."
  def create(server, id, attrs \\ %{}) do
    GenServer.call(server, {:create, id, attrs})
  end

  @doc "Tenta alterar o status de uma entrega."
  def transition(server, id, new_status) do
    GenServer.call(server, {:transition, id, new_status})
  end

  @doc "Busca uma entrega pelo ID."
  def get(server, id) do
    GenServer.call(server, {:get, id})
  end

  @doc "Retorna todas as entregas, ordenadas pelo ID."
  def deliveries(server) do
    GenServer.call(server, :deliveries)
  end

  @doc "Retorna o histórico completo em ordem cronológica."
  def history(server) do
    GenServer.call(server, :history)
  end

  @doc "Executa imediatamente a verificação de entregas vencidas."
  def run_periodic(server) do
    GenServer.call(server, :run_periodic)
  end

  # Callbacks do GenServer

  @impl true
  def init(opts) do
    state = %{
      deliveries: %{},
      history: [],
      next_sequence: 1,
      interval_ms: Keyword.get(opts, :interval_ms, 60_000)
    }

    schedule_periodic_check(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:create, id, attrs}, _from, state) do
    with :ok <- validate_id(id),
         {:ok, expected_at} <- extract_expected_at(attrs),
         false <- Map.has_key?(state.deliveries, id) do
      now = now_ms()

      delivery = %Delivery{
        id: id,
        status: :pending,
        expected_at: expected_at,
        inserted_at: now,
        updated_at: now
      }

      state =
        state
        |> put_delivery(delivery)
        |> record_history(:created, id, nil, :pending, now)

      {:reply, {:ok, delivery}, state}
    else
      true -> {:reply, {:error, :already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:transition, id, new_status}, _from, state) do
    case Map.fetch(state.deliveries, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, _delivery} when new_status not in @statuses ->
        {:reply, {:error, :invalid_status}, state}

      {:ok, delivery} ->
        if new_status in Map.fetch!(@transitions, delivery.status) do
          now = now_ms()
          updated = %{delivery | status: new_status, updated_at: now}

          state =
            state
            |> put_delivery(updated)
            |> record_history(
              :status_changed,
              id,
              delivery.status,
              new_status,
              now
            )

          {:reply, {:ok, updated}, state}
        else
          {:reply, {:error, :invalid_transition}, state}
        end
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.deliveries, id) do
        {:ok, delivery} -> {:ok, delivery}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:deliveries, _from, state) do
    result =
      state.deliveries
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, result, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call(:run_periodic, _from, state) do
    {count, state} = mark_overdue_deliveries(state, now_ms())
    {:reply, {:ok, count}, state}
  end

  @impl true
  def handle_info(:periodic_check, state) do
    {_count, state} = mark_overdue_deliveries(state, now_ms())
    schedule_periodic_check(state.interval_ms)
    {:noreply, state}
  end

  # Funções privadas

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_id), do: {:error, :invalid_id}

  defp extract_expected_at(attrs) when is_map(attrs) do
    validate_expected_at(Map.get(attrs, :expected_at))
  end

  defp extract_expected_at(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      validate_expected_at(Keyword.get(attrs, :expected_at))
    else
      {:error, :invalid_attributes}
    end
  end

  defp extract_expected_at(_attrs), do: {:error, :invalid_attributes}

  defp validate_expected_at(nil), do: {:ok, nil}
  defp validate_expected_at(value) when is_integer(value), do: {:ok, value}
  defp validate_expected_at(_value), do: {:error, :invalid_expected_at}

  defp mark_overdue_deliveries(state, now) do
    state.deliveries
    |> Map.values()
    |> Enum.filter(fn delivery ->
      delivery.expected_at != nil and
        delivery.expected_at <= now and
        delivery.status not in [:delayed, :delivered]
    end)
    |> Enum.sort_by(& &1.id)
    |> Enum.reduce({0, state}, fn delivery, {count, acc} ->
      updated = %{delivery | status: :delayed, updated_at: now}

      acc =
        acc
        |> put_delivery(updated)
        |> record_history(
          :marked_delayed,
          delivery.id,
          delivery.status,
          :delayed,
          now
        )

      {count + 1, acc}
    end)
  end

  defp put_delivery(state, delivery) do
    %{state | deliveries: Map.put(state.deliveries, delivery.id, delivery)}
  end

  defp record_history(state, action, delivery_id, from_status, to_status, at) do
    entry = %{
      sequence: state.next_sequence,
      action: action,
      delivery_id: delivery_id,
      from_status: from_status,
      to_status: to_status,
      at: at
    }

    %{
      state
      | history: [entry | state.history],
        next_sequence: state.next_sequence + 1
    }
  end

  defp schedule_periodic_check(interval_ms)
       when is_integer(interval_ms) and interval_ms > 0 do
    Process.send_after(self(), :periodic_check, interval_ms)
  end

  defp schedule_periodic_check(_interval_ms), do: :ok

  defp now_ms, do: System.system_time(:millisecond)
end
```

### `test/test_helper.exs`

```elixir
ExUnit.start()
```

### `test/delivery_tracker_test.exs`

```elixir
defmodule DeliveryTrackerTest do
  use ExUnit.Case, async: true

  alias DeliveryTracker.Delivery

  defp start_tracker(opts \\ []) do
    {:ok, pid} = DeliveryTracker.start_link(opts)

    on_exit(fn ->
      if Process.alive?(pid) do
        GenServer.stop(pid)
      end
    end)

    pid
  end

  test "mantém histórico ordenado das operações" do
    tracker = start_tracker(interval_ms: nil)

    assert {:ok, %Delivery{status: :pending}} =
             DeliveryTracker.create(tracker, "delivery-1")

    assert {:ok, %Delivery{status: :picked_up}} =
             DeliveryTracker.transition(tracker, "delivery-1", :picked_up)

    assert {:ok, %Delivery{status: :in_transit}} =
             DeliveryTracker.transition(tracker, "delivery-1", :in_transit)

    history = DeliveryTracker.history(tracker)

    assert Enum.map(history, & &1.sequence) == [1, 2, 3]
    assert Enum.map(history, & &1.action) ==
             [:created, :status_changed, :status_changed]

    assert Enum.map(history, & &1.to_status) ==
             [:pending, :picked_up, :in_transit]
  end

  test "rejeita transições inválidas sem alterar o estado" do
    tracker = start_tracker(interval_ms: nil)

    assert {:ok, _delivery} = DeliveryTracker.create(tracker, "delivery-1")

    assert {:error, :invalid_transition} =
             DeliveryTracker.transition(tracker, "delivery-1", :delivered)

    assert {:ok, %Delivery{status: :pending}} =
             DeliveryTracker.get(tracker, "delivery-1")

    assert length(DeliveryTracker.history(tracker)) == 1
  end

  test "operações simultâneas em entregas diferentes são processadas" do
    tracker = start_tracker(interval_ms: nil)

    results =
      1..50
      |> Task.async_stream(
        fn number ->
          DeliveryTracker.create(tracker, "delivery-#{number}")
        end,
        max_concurrency: 50
      )
      |> Enum.to_list()

    assert Enum.all?(results, fn
             {:ok, {:ok, %Delivery{}}} -> true
             _ -> false
           end)

    assert length(DeliveryTracker.deliveries(tracker)) == 50
    assert length(DeliveryTracker.history(tracker)) == 50
  end

  test "serializa transições concorrentes para a mesma entrega" do
    tracker = start_tracker(interval_ms: nil)
    assert {:ok, _delivery} = DeliveryTracker.create(tracker, "shared")

    results =
      1..30
      |> Task.async_stream(
        fn _ -> DeliveryTracker.transition(tracker, "shared", :picked_up) end,
        max_concurrency: 30
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &match?({:ok, _}, &1)) == 1

    assert Enum.count(results, fn
             {:error, :invalid_transition} -> true
             _ -> false
           end) == 29

    assert {:ok, %Delivery{status: :picked_up}} =
             DeliveryTracker.get(tracker, "shared")

    # Criação e uma única transição bem-sucedida.
    assert length(DeliveryTracker.history(tracker)) == 2
  end

  test "marca entregas vencidas quando a verificação periódica é executada" do
    tracker = start_tracker(interval_ms: nil)
    overdue_time = System.system_time(:millisecond) - 1_000

    assert {:ok, _delivery} =
             DeliveryTracker.create(tracker, "overdue",
               expected_at: overdue_time
             )

    assert {:ok, 1} = DeliveryTracker.run_periodic(tracker)

    assert {:ok, %Delivery{status: :delayed}} =
             DeliveryTracker.get(tracker, "overdue")

    assert [%{action: :created}, %{action: :marked_delayed}] =
             DeliveryTracker.history(tracker)

    # Uma entrega já marcada como atrasada não gera outro evento.
    assert {:ok, 0} = DeliveryTracker.run_periodic(tracker)
    assert length(DeliveryTracker.history(tracker)) == 2
  end

  test "executa automaticamente a verificação por timer" do
    tracker = start_tracker(interval_ms: 10)
    overdue_time = System.system_time(:millisecond) - 1_000

    assert {:ok, _delivery} =
             DeliveryTracker.create(tracker, "timer-overdue",
               expected_at: overdue_time
             )

    assert eventually(fn ->
             match?(
               {:ok, %Delivery{status: :delayed}},
               DeliveryTracker.get(tracker, "timer-overdue")
             )
           end)
  end

  defp eventually(predicate, attempts \\ 100)

  defp eventually(_predicate, 0), do: false

  defp eventually(predicate, attempts) do
    if predicate.() do
      true
    else
      Process.sleep(10)
      eventually(predicate, attempts - 1)
    end
  end
end
```

Execute os testes na raiz do projeto com:

```bash
mix test
```

As operações sobre uma mesma entrega são serializadas pelo `GenServer`, evitando atualizações conflitantes. Chamadas de processos diferentes continuam podendo ocorrer simultaneamente; cada uma recebe uma resposta após sua operação ser aplicada ao estado consistente do serviço.
