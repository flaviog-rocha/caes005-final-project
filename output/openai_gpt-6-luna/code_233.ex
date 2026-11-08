# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Implementar expiração automática de registros.
# 3. Manter um histórico das operações realizadas.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  use GenServer

  @default_ttl_ms 15 * 60 * 1_000
  @default_sweep_interval_ms 1_000

  # Inicia um servidor local. Para iniciar mais de uma instância, passe
  # name: nil ou um nome diferente nas opções.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # attrs deve conter :operation_id e :amount_minor. :currency e :ttl_ms
  # são opcionais; valores monetários são representados em unidades menores.
  def create(attrs), do: create(__MODULE__, attrs)

  def create(server, attrs) do
    GenServer.call(server, {:create, normalize_attrs(attrs)})
  end

  def capture(transaction_id, operation_id),
    do: capture(__MODULE__, transaction_id, operation_id)

  def capture(server, transaction_id, operation_id) do
    GenServer.call(server, {:capture, transaction_id, operation_id})
  end

  def cancel(transaction_id, operation_id),
    do: cancel(__MODULE__, transaction_id, operation_id)

  def cancel(server, transaction_id, operation_id) do
    GenServer.call(server, {:cancel, transaction_id, operation_id})
  end

  def get(transaction_id), do: get(__MODULE__, transaction_id)

  def get(server, transaction_id) do
    GenServer.call(server, {:get, transaction_id})
  end

  def list, do: list(__MODULE__)

  def list(server) do
    GenServer.call(server, :list)
  end

  def history, do: history(__MODULE__)

  def history(server) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(opts) do
    interval =
      case Keyword.get(opts, :sweep_interval_ms, @default_sweep_interval_ms) do
        value when is_integer(value) and value > 0 -> value
        _ -> @default_sweep_interval_ms
      end

    schedule_sweep(interval)

    {:ok,
     %{
       transactions: %{},
       operations: %{},
       history: [],
       sweep_interval_ms: interval
     }}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    state = expire_due(state)

    operation_id = Map.get(attrs, :operation_id)
    amount_minor = Map.get(attrs, :amount_minor)
    currency = Map.get(attrs, :currency, "USD")
    ttl_ms = Map.get(attrs, :ttl_ms, @default_ttl_ms)

    fingerprint = {:create, amount_minor, currency, ttl_ms}

    {reply, state} =
      perform_operation(
        state,
        operation_id,
        fingerprint,
        :create,
        fn state ->
          create_transaction(state, amount_minor, currency, ttl_ms)
        end
      )

    {:reply, reply, state}
  end

  def handle_call({:capture, transaction_id, operation_id}, _from, state) do
    state = expire_due(state)

    {reply, state} =
      perform_operation(
        state,
        operation_id,
        {:capture, transaction_id},
        :capture,
        fn state -> transition_transaction(state, transaction_id, :captured) end
      )

    {:reply, reply, state}
  end

  def handle_call({:cancel, transaction_id, operation_id}, _from, state) do
    state = expire_due(state)

    {reply, state} =
      perform_operation(
        state,
        operation_id,
        {:cancel, transaction_id},
        :cancel,
        fn state -> transition_transaction(state, transaction_id, :cancelled) end
      )

    {:reply, reply, state}
  end

  def handle_call({:get, transaction_id}, _from, state) do
    state = expire_due(state)
    reply = fetch_transaction(state, transaction_id)
    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    state = expire_due(state)

    transactions =
      state.transactions
      |> Map.values()
      |> Enum.sort_by(& &1.inserted_at)

    {:reply, transactions, state}
  end

  def handle_call(:history, _from, state) do
    state = expire_due(state)
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info(:sweep, state) do
    state
    |> expire_due()
    |> then(fn next_state ->
      schedule_sweep(next_state.sweep_interval_ms)
      {:noreply, next_state}
    end)
  end

  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp normalize_attrs(attrs) when is_list(attrs) do
    try do
      Map.new(attrs)
    rescue
      ArgumentError -> %{}
    end
  end

  defp normalize_attrs(_), do: %{}

  defp create_transaction(state, amount_minor, currency, ttl_ms) do
    cond do
      not (is_integer(amount_minor) and amount_minor > 0) ->
        {{:error, :invalid_amount}, state, nil}

      not valid_currency?(currency) ->
        {{:error, :invalid_currency}, state, nil}

      not (is_integer(ttl_ms) and ttl_ms > 0) ->
        {{:error, :invalid_ttl}, state, nil}

      true ->
        now = System.system_time(:millisecond)
        id = "txn-#{System.unique_integer([:positive, :monotonic])}"

        transaction = %{
          id: id,
          amount_minor: amount_minor,
          currency: currency,
          status: :pending,
          inserted_at: now,
          updated_at: now,
          expires_at: now + ttl_ms
        }

        next_state = %{
          state
          | transactions: Map.put(state.transactions, id, transaction)
        }

        {{:ok, transaction}, next_state, id}
    end
  end

  defp transition_transaction(state, transaction_id, new_status) do
    case Map.fetch(state.transactions, transaction_id) do
      {:ok, %{status: :pending} = transaction} ->
        updated = %{
          transaction
          | status: new_status,
            updated_at: System.system_time(:millisecond)
        }

        next_state = %{
          state
          | transactions: Map.put(state.transactions, transaction_id, updated)
        }

        {{:ok, updated}, next_state, transaction_id}

      {:ok, _transaction} ->
        {{:error, :invalid_status}, state, transaction_id}

      :error ->
        {{:error, :not_found}, state, transaction_id}
    end
  end

  defp perform_operation(state, operation_id, fingerprint, type, action) do
    if valid_operation_id?(operation_id) do
      case Map.fetch(state.operations, operation_id) do
        {:ok, %{fingerprint: ^fingerprint, result: result}} ->
          {result, state}

        {:ok, _previous_operation} ->
          {{:error, :idempotency_key_reused}, state}

        :error ->
          {result, next_state, transaction_id} = action.(state)

          operation = %{fingerprint: fingerprint, result: result}
          now = System.system_time(:millisecond)

          entry = %{
            operation_id: operation_id,
            type: type,
            transaction_id: transaction_id,
            outcome: operation_outcome(result),
            details: operation_details(result),
            at: now
          }

          next_state = %{
            next_state
            | operations: Map.put(next_state.operations, operation_id, operation),
              history: [entry | next_state.history]
          }

          {result, next_state}
      end
    else
      {{:error, :invalid_operation_id}, state}
    end
  end

  defp expire_due(state) do
    now = System.system_time(:millisecond)

    Enum.reduce(state.transactions, state, fn {id, transaction}, acc ->
      if transaction.status == :pending and transaction.expires_at <= now do
        entry = %{
          operation_id: "expiration:#{id}",
          type: :expired,
          transaction_id: id,
          outcome: :ok,
          details: transaction,
          at: now
        }

        %{
          acc
          | transactions: Map.delete(acc.transactions, id),
            history: [entry | acc.history]
        }
      else
        acc
      end
    end)
  end

  defp fetch_transaction(state, transaction_id) do
    case Map.fetch(state.transactions, transaction_id) do
      {:ok, transaction} -> {:ok, transaction}
      :error -> {:error, :not_found}
    end
  end

  defp valid_operation_id?(id), do: is_binary(id) and byte_size(String.trim(id)) > 0

  defp valid_currency?(currency) do
    is_binary(currency) and byte_size(currency) == 3 and
      String.match?(currency, ~r/^[A-Z]{3}$/)
  end

  defp operation_outcome({:ok, _}), do: :ok
  defp operation_outcome({:error, reason}), do: {:error, reason}

  defp operation_details({:ok, value}), do: value
  defp operation_details({:error, reason}), do: reason

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep, interval_ms)
  end
end
