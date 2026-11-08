# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  use GenServer

  @moduledoc """
  Gerenciamento em memória de transações financeiras, sem dependências externas.
  Os valores monetários são armazenados em unidades menores inteiras, como centavos.
  """

  @statuses [:pending, :posted, :failed]
  @mutable_fields [:amount_minor, :currency, :description, :status]

  # Inicia o gerenciador. O intervalo pode ser `nil` para desativar a reconciliação periódica.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  def create(server, attrs), do: GenServer.call(server, {:create, attrs})
  def get(server, id), do: GenServer.call(server, {:get, id})
  def list(server), do: GenServer.call(server, :list)
  def update(server, id, attrs), do: GenServer.call(server, {:update, id, attrs})
  def delete(server, id), do: GenServer.call(server, {:delete, id})
  def reconcile(server), do: GenServer.call(server, :reconcile)
  def last_reconciliation(server), do: GenServer.call(server, :last_reconciliation)

  # Executa funções em paralelo, limitando a quantidade de tarefas simultâneas.
  # Cada função recebe um item da enumerable; resultados de falhas são retornados como {:exit, motivo}.
  def run_limited(server, enumerable, fun) when is_function(fun, 1) do
    max_concurrency = GenServer.call(server, :max_concurrency)

    enumerable
    |> Task.async_stream(fun,
      max_concurrency: max_concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.to_list()
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    interval = Keyword.get(opts, :reconciliation_interval, 60_000)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, :invalid_max_concurrency}
    else
      if interval != nil and not (is_integer(interval) and interval > 0) do
        {:stop, :invalid_reconciliation_interval}
      else
        state = %{
          transactions: %{},
          next_id: 1,
          max_concurrency: max_concurrency,
          reconciliation_interval: interval,
          reconciliation_timer: schedule_reconciliation(interval),
          last_reconciliation: nil
        }

        {:ok, state}
      end
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, transaction_attrs} <- validate_transaction(attrs) do
      now = DateTime.utc_now()

      transaction =
        Map.merge(transaction_attrs, %{
          id: state.next_id,
          inserted_at: now,
          updated_at: now
        })

      new_state = %{
        state
        | transactions: Map.put(state.transactions, transaction.id, transaction),
          next_id: state.next_id + 1
      }

      {:reply, {:ok, transaction}, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.transactions, id) do
      {:ok, transaction} -> {:reply, {:ok, transaction}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    transactions =
      state.transactions
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, transactions, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with {:ok, existing} <- fetch_transaction(state, id),
         {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_mutable_fields(attrs),
         {:ok, updated_attrs} <- validate_transaction(Map.merge(existing, attrs)) do
      transaction =
        existing
        |> Map.merge(updated_attrs)
        |> Map.put(:updated_at, DateTime.utc_now())

      new_state = %{state | transactions: Map.put(state.transactions, id, transaction)}
      {:reply, {:ok, transaction}, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.pop(state.transactions, id) do
      {nil, _transactions} ->
        {:reply, {:error, :not_found}, state}

      {transaction, transactions} ->
        {:reply, {:ok, transaction}, %{state | transactions: transactions}}
    end
  end

  def handle_call(:reconcile, _from, state) do
    snapshot = build_reconciliation(state.transactions)
    {:reply, {:ok, snapshot}, %{state | last_reconciliation: snapshot}}
  end

  def handle_call(:last_reconciliation, _from, state) do
    {:reply, state.last_reconciliation, state}
  end

  def handle_call(:max_concurrency, _from, state) do
    {:reply, state.max_concurrency, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    snapshot = build_reconciliation(state.transactions)

    new_state = %{
      state
      | last_reconciliation: snapshot,
        reconciliation_timer: schedule_reconciliation(state.reconciliation_interval)
    }

    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, %{reconciliation_timer: timer}) do
    if is_reference(timer), do: Process.cancel_timer(timer)
    :ok
  end

  defp fetch_transaction(state, id) do
    case Map.fetch(state.transactions, id) do
      {:ok, transaction} -> {:ok, transaction}
      :error -> {:error, :not_found}
    end
  end

  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}
  defp normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}
  defp normalize_attrs(_attrs), do: {:error, :invalid_attributes}

  defp validate_mutable_fields(attrs) do
    if Enum.all?(Map.keys(attrs), &(&1 in @mutable_fields)) do
      :ok
    else
      {:error, :immutable_or_unknown_field}
    end
  end

  defp validate_transaction(attrs) do
    amount = Map.get(attrs, :amount_minor)
    currency = Map.get(attrs, :currency)
    description = Map.get(attrs, :description, "")
    status = Map.get(attrs, :status, :pending)

    cond do
      not (is_integer(amount) and amount > 0) ->
        {:error, :invalid_amount}

      not valid_currency?(currency) ->
        {:error, :invalid_currency}

      not is_binary(description) ->
        {:error, :invalid_description}

      status not in @statuses ->
        {:error, :invalid_status}

      true ->
        {:ok,
         %{
           amount_minor: amount,
           currency: currency,
           description: description,
           status: status
         }}
    end
  end

  defp valid_currency?(currency) when is_binary(currency) do
    Regex.match?(~r/^[A-Z]{3}$/, currency)
  end

  defp valid_currency?(_currency), do: false

  defp build_reconciliation(transactions) do
    {counts, amounts} =
      Enum.reduce(transactions, {%{}, %{}}, fn {_id, transaction}, {counts, amounts} ->
        counts = Map.update(counts, transaction.status, 1, &(&1 + 1))

        amounts =
          Map.update(
            amounts,
            transaction.currency,
            transaction.amount_minor,
            &(&1 + transaction.amount_minor)
          )

        {counts, amounts}
      end)

    %{
      reconciled_at: DateTime.utc_now(),
      transaction_count: map_size(transactions),
      counts_by_status: counts,
      amounts_minor_by_currency: amounts
    }
  end

  defp schedule_reconciliation(nil), do: nil

  defp schedule_reconciliation(interval) do
    Process.send_after(self(), :reconcile, interval)
  end
end
