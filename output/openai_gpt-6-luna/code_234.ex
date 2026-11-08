# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Implementar expiração automática de registros.
# 3. Manter um histórico das operações realizadas.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialLedger do
  use GenServer

  # As transações expiram da tabela operacional; o índice de idempotência e o
  # histórico são mantidos para impedir reexecuções e preservar a auditoria.

  def start_link(opts \\ []) do
    {name, server_opts} = Keyword.pop(opts, :name)

    case name do
      nil -> GenServer.start_link(__MODULE__, server_opts)
      _ -> GenServer.start_link(__MODULE__, server_opts, name: name)
    end
  end

  def open_account(server, account_id, initial_balance \\ 0) do
    GenServer.call(server, {:open_account, account_id, initial_balance})
  end

  def transfer(server, idempotency_key, from_account, to_account, amount, opts \\ []) do
    GenServer.call(
      server,
      {:transfer, idempotency_key, from_account, to_account, amount, opts}
    )
  end

  def balance(server, account_id) do
    GenServer.call(server, {:balance, account_id})
  end

  def transaction(server, transaction_id) do
    GenServer.call(server, {:transaction, transaction_id})
  end

  def history(server, limit \\ :infinity) do
    GenServer.call(server, {:history, limit})
  end

  @impl true
  def init(opts) do
    default_ttl = Keyword.get(opts, :default_ttl, 86_400_000)

    if is_integer(default_ttl) and default_ttl > 0 do
      {:ok,
       %{
         accounts: %{},
         transactions: %{},
         idempotency: %{},
         history: [],
         default_ttl: default_ttl,
         cleanup_timer: nil
       }}
    else
      {:stop, :invalid_default_ttl}
    end
  end

  @impl true
  def handle_call(request, _from, state) do
    state = state |> purge_expired() |> schedule_cleanup()

    case request do
      {:open_account, account_id, initial_balance} ->
        open_account_call(state, account_id, initial_balance)

      {:transfer, key, from_account, to_account, amount, opts} ->
        transfer_call(state, key, from_account, to_account, amount, opts)

      {:balance, account_id} ->
        case Map.fetch(state.accounts, account_id) do
          {:ok, amount} -> {:reply, {:ok, amount}, state}
          :error -> {:reply, {:error, :account_not_found}, state}
        end

      {:transaction, transaction_id} ->
        case Map.fetch(state.transactions, transaction_id) do
          {:ok, transaction} -> {:reply, {:ok, transaction}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end

      {:history, limit} when limit == :infinity or (is_integer(limit) and limit >= 0) ->
        entries =
          case limit do
            :infinity -> state.history
            value -> Enum.take(state.history, value)
          end

        {:reply, {:ok, entries}, state}

      {:history, _limit} ->
        {:reply, {:error, :invalid_limit}, state}

      _ ->
        {:reply, {:error, :unsupported_operation}, state}
    end
  end

  @impl true
  def handle_info(:cleanup, state) do
    state =
      %{state | cleanup_timer: nil}
      |> purge_expired()
      |> schedule_cleanup()

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{cleanup_timer: timer}) when is_reference(timer) do
    Process.cancel_timer(timer)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp open_account_call(state, account_id, initial_balance)
       when not is_nil(account_id) and is_integer(initial_balance) and initial_balance >= 0 do
    if Map.has_key?(state.accounts, account_id) do
      {:reply, {:error, :account_already_exists}, state}
    else
      entry = %{
        id: System.unique_integer([:positive, :monotonic]),
        operation: :account_opened,
        account_id: account_id,
        initial_balance: initial_balance,
        occurred_at: DateTime.utc_now()
      }

      next_state = %{
        state
        | accounts: Map.put(state.accounts, account_id, initial_balance),
          history: [entry | state.history]
      }

      {:reply, {:ok, entry}, next_state}
    end
  end

  defp open_account_call(state, _account_id, _initial_balance) do
    {:reply, {:error, :invalid_account}, state}
  end

  defp transfer_call(state, key, from_account, to_account, amount, opts) do
    cond do
      not is_binary(key) or byte_size(key) == 0 ->
        {:reply, {:error, :invalid_idempotency_key}, state}

      true ->
        signature = {from_account, to_account, amount}

        case Map.fetch(state.idempotency, key) do
          {:ok, %{signature: ^signature}} ->
            {:reply, {:error, :already_processed}, state}

          {:ok, _entry} ->
            {:reply, {:error, :idempotency_key_reused}, state}

          :error ->
            execute_transfer(state, key, signature, from_account, to_account, amount, opts)
        end
    end
  end

  defp execute_transfer(state, key, signature, from_account, to_account, amount, opts) do
    cond do
      not is_integer(amount) or amount <= 0 ->
        {:reply, {:error, :invalid_amount}, state}

      is_nil(from_account) or is_nil(to_account) ->
        {:reply, {:error, :invalid_account}, state}

      from_account == to_account ->
        {:reply, {:error, :accounts_must_differ}, state}

      not is_list(opts) or not Keyword.keyword?(opts) ->
        {:reply, {:error, :invalid_options}, state}

      true ->
        ttl = Keyword.get(opts, :ttl, state.default_ttl)

        if not is_integer(ttl) or ttl <= 0 do
          {:reply, {:error, :invalid_ttl}, state}
        else
          apply_transfer(state, key, signature, from_account, to_account, amount, ttl)
        end
    end
  end

  defp apply_transfer(state, key, signature, from_account, to_account, amount, ttl) do
    with {:ok, from_balance} <- fetch_account(state.accounts, from_account),
         {:ok, to_balance} <- fetch_account(state.accounts, to_account),
         true <- from_balance >= amount do
      now = System.monotonic_time(:millisecond)
      id = System.unique_integer([:positive, :monotonic])

      transaction = %{
        id: id,
        idempotency_key: key,
        from_account: from_account,
        to_account: to_account,
        amount: amount,
        status: :completed,
        occurred_at: DateTime.utc_now(),
        expires_at: now + ttl
      }

      history_entry = Map.delete(transaction, :expires_at)

      next_state = %{
        state
        | accounts:
            state.accounts
            |> Map.put(from_account, from_balance - amount)
            |> Map.put(to_account, to_balance + amount),
          transactions: Map.put(state.transactions, id, transaction),
          idempotency:
            Map.put(state.idempotency, key, %{
              signature: signature,
              transaction_id: id
            }),
          history: [history_entry | state.history]
      }

      {:reply, {:ok, transaction}, schedule_cleanup(next_state)}
    else
      :error -> {:reply, {:error, :account_not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
      false -> {:reply, {:error, :insufficient_funds}, state}
    end
  end

  defp fetch_account(accounts, account_id) do
    case Map.fetch(accounts, account_id) do
      {:ok, balance} -> {:ok, balance}
      :error -> {:error, :account_not_found}
    end
  end

  defp purge_expired(state) do
    now = System.monotonic_time(:millisecond)

    transactions =
      Map.reject(state.transactions, fn {_id, transaction} ->
        transaction.expires_at <= now
      end)

    %{state | transactions: transactions}
  end

  defp schedule_cleanup(state) do
    if is_reference(state.cleanup_timer) do
      Process.cancel_timer(state.cleanup_timer)
    end

    case state.transactions |> Map.values() |> Enum.map(& &1.expires_at) |> Enum.min(fn -> nil end) do
      nil ->
        %{state | cleanup_timer: nil}

      expires_at ->
        delay = max(expires_at - System.monotonic_time(:millisecond), 0)
        timer = Process.send_after(self(), :cleanup, delay)
        %{state | cleanup_timer: timer}
    end
  end
end
