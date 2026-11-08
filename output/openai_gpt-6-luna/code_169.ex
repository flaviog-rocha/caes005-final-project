# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Manter um histórico das operações realizadas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  @moduledoc """
  Gerenciador de transferências financeiras com histórico em journal,
  execução periódica e recuperação supervisionada.
  """

  @default_server FinancialTransactions.Server
  @default_supervisor FinancialTransactions.Supervisor

  defmacro transaction(attrs) do
    quote do
      FinancialTransactions.submit(unquote(attrs))
    end
  end

  def start_link(opts \\ []) do
    supervisor_name = Keyword.get(opts, :supervisor_name, @default_supervisor)

    if supervisor_name do
      Supervisor.start_link(__MODULE__, opts, name: supervisor_name)
    else
      Supervisor.start_link(__MODULE__, opts)
    end
  end

  @impl true
  def init(opts) do
    server_name = Keyword.get(opts, :name, @default_server)

    child_opts = Keyword.put(opts, :name, server_name)

    children = [
      %{
        id: FinancialTransactions.Server,
        start: {FinancialTransactions.Server, :start_link, [child_opts]},
        restart: :permanent,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def submit(attrs, server \\ @default_server),
    do: GenServer.call(server, {:submit, attrs})

  def schedule(id, attrs, interval_ms, server \\ @default_server),
    do: GenServer.call(server, {:schedule, id, attrs, interval_ms})

  def status(server \\ @default_server),
    do: GenServer.call(server, :status)

  def history(server \\ @default_server),
    do: GenServer.call(server, :history)

  def balances(server \\ @default_server),
    do: GenServer.call(server, :balances)
end

defmodule FinancialTransactions.Server do
  use GenServer

  @default_server FinancialTransactions.Server
  @default_journal "financial_transactions.journal"

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_server)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @impl true
  def init(opts) do
    with {:ok, balances} <- validate_initial_balances(Keyword.get(opts, :balances, %{})),
         {:ok, journal} <- prepare_journal(Keyword.get(opts, :journal, @default_journal)) do
      state = %{
        balances: balances,
        operations: %{},
        history: [],
        schedules: %{},
        journal: journal,
        tick_ms: valid_tick(Keyword.get(opts, :tick_ms, 1_000))
      }

      state = recover(state)
      Process.send_after(self(), :tick, state.tick_ms)
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:submit, attrs}, _from, state) do
    with {:ok, tx} <- normalize_transaction(attrs),
         false <- Map.has_key?(state.operations, tx.id) do
      event = {:submitted, tx}

      case persist_and_apply(event, state) do
        {:ok, next_state} -> {:reply, {:ok, tx.id}, next_state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      true -> {:reply, {:error, :duplicate_id}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:schedule, id, attrs, interval_ms}, _from, state) do
    with true <- valid_id?(id),
         true <- is_integer(interval_ms) and interval_ms > 0,
         {:ok, transfer} <- normalize_transfer(attrs),
         false <- Map.has_key?(state.schedules, id) do
      schedule = %{
        id: id,
        transfer: transfer,
        interval_ms: interval_ms,
        next_at: System.system_time(:millisecond) + interval_ms
      }

      case persist_and_apply({:scheduled, schedule}, state) do
        {:ok, next_state} -> {:reply, {:ok, id}, next_state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      false -> {:reply, {:error, :invalid_schedule}, state}
      true -> {:reply, {:error, :duplicate_id}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       operations: state.operations,
       schedules: state.schedules
     }, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call(:balances, _from, state) do
    {:reply, state.balances, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      state
      |> fire_due_schedules()
      |> run_pending_operations()

    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  defp fire_due_schedules(state) do
    now = System.system_time(:millisecond)

    state.schedules
    |> Enum.filter(fn {_id, schedule} -> schedule.next_at <= now end)
    |> Enum.reduce(state, fn {id, schedule}, current_state ->
      tx_id =
        "#{id}/#{now}/#{System.unique_integer([:positive, :monotonic])}"

      tx = Map.put(schedule.transfer, :id, tx_id)

      updated_schedule = %{
        schedule
        | next_at: now + schedule.interval_ms
      }

      event = {:schedule_fired, id, updated_schedule, tx}

      case persist_and_apply(event, current_state) do
        {:ok, next_state} -> next_state
        {:error, _reason} -> current_state
      end
    end)
  end

  defp run_pending_operations(state) do
    state.operations
    |> Enum.filter(fn {_id, operation} -> operation.status == :pending end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.reduce(state, fn id, current_state ->
      case Map.fetch(current_state.operations, id) do
        {:ok, operation} ->
          tx = operation.transaction

          case persist_and_apply({:started, id}, current_state) do
            {:ok, started_state} ->
              {new_balances, status, reason} = execute_transfer(tx, started_state.balances)

              record = %{
                id: id,
                from: tx.from,
                to: tx.to,
                amount: tx.amount,
                status: status,
                reason: reason,
                completed_at: System.system_time(:millisecond)
              }

              case persist_and_apply(
                     {:finished, id, record, new_balances},
                     started_state
                   ) do
                {:ok, finished_state} -> finished_state
                {:error, _reason} ->
                  put_in(started_state.operations[id].status, :pending)
              end

            {:error, _reason} ->
              current_state
          end

        :error ->
          current_state
      end
    end)
  end

  defp execute_transfer(tx, balances) do
    from_balance = Map.get(balances, tx.from, 0)

    if from_balance < tx.amount do
      {balances, :failed, :insufficient_funds}
    else
      updated_balances =
        if tx.from == tx.to do
          balances
        else
          balances
          |> Map.put(tx.from, from_balance - tx.amount)
          |> Map.update(tx.to, tx.amount, &(&1 + tx.amount))
        end

      {updated_balances, :completed, nil}
    end
  end

  defp persist_and_apply(event, state) do
    case append_event(state.journal, event) do
      :ok -> {:ok, apply_event(state, event)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_event(state, {:submitted, tx}) do
    put_in(state.operations[tx.id], %{transaction: tx, status: :pending})
  end

  defp apply_event(state, {:started, id}) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} ->
        put_in(state.operations[id], %{operation | status: :in_progress})

      :error ->
        state
    end
  end

  defp apply_event(state, {:finished, id, record, balances}) do
    %{
      state
      | operations: Map.delete(state.operations, id),
        history: [record | state.history],
        balances: balances
    }
  end

  defp apply_event(state, {:scheduled, schedule}) do
    put_in(state.schedules[schedule.id], schedule)
  end

  defp apply_event(state, {:schedule_fired, id, schedule, tx}) do
    state
    |> put_in([Access.key(:schedules), id], schedule)
    |> put_in([Access.key(:operations), tx.id], %{
      transaction: tx,
      status: :pending
    })
  end

  defp recover(%{journal: :memory} = state), do: state

  defp recover(state) do
    case File.read(state.journal) do
      {:ok, contents} ->
        contents
        |> String.split("\n", trim: true)
        |> Enum.reduce(state, fn line, acc ->
          case decode_event(line) do
            {:ok, event} -> apply_event(acc, event)
            :error -> acc
          end
        end)
        |> reset_interrupted_operations()

      {:error, _reason} ->
        state
    end
  end

  defp reset_interrupted_operations(state) do
    operations =
      Map.new(state.operations, fn {id, operation} ->
        if operation.status == :in_progress do
          {id, %{operation | status: :pending}}
        else
          {id, operation}
        end
      end)

    %{state | operations: operations}
  end

  defp append_event(:memory, _event), do: :ok

  defp append_event(path, event) do
    encoded =
      event
      |> :erlang.term_to_binary()
      |> Base.encode64()

    case File.open(path, [:append, :binary]) do
      {:ok, file} ->
        result =
          with :ok <- IO.binwrite(file, encoded <> "\n"),
               :ok <- :file.sync(file) do
            :ok
          end

        File.close(file)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_event(line) do
    with {:ok, binary} <- Base.decode64(line) do
      try do
        {:ok, :erlang.binary_to_term(binary, [:safe])}
      rescue
        _ -> :error
      end
    else
      _ -> :error
    end
  end

  defp prepare_journal(:memory), do: {:ok, :memory}

  defp prepare_journal(path) when is_binary(path) do
    expanded = Path.expand(path)

    case File.mkdir_p(Path.dirname(expanded)) do
      :ok ->
        case File.touch(expanded) do
          :ok -> {:ok, expanded}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp prepare_journal(_), do: {:error, :invalid_journal_path}

  defp normalize_transaction(attrs) do
    with {:ok, attrs} <- attrs_to_map(attrs),
         id when is_binary(id) <- Map.get(attrs, :id),
         true <- valid_id?(id),
         {:ok, transfer} <- normalize_transfer(attrs) do
      {:ok, Map.put(transfer, :id, id)}
    else
      false -> {:error, :invalid_transaction}
      nil -> {:error, :missing_id}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_transaction}
    end
  end

  defp normalize_transfer(attrs) do
    with {:ok, attrs} <- attrs_to_map(attrs),
         from when is_binary(from) <- Map.get(attrs, :from),
         to when is_binary(to) <- Map.get(attrs, :to),
         amount when is_integer(amount) and amount > 0 <- Map.get(attrs, :amount) do
      {:ok, %{from: from, to: to, amount: amount}}
    else
      _ -> {:error, :invalid_transaction}
    end
  end

  defp attrs_to_map(attrs) when is_map(attrs), do: {:ok, attrs}

  defp attrs_to_map(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs), do: {:ok, Map.new(attrs)}, else: {:error, :invalid_transaction}
  end

  defp attrs_to_map(_), do: {:error, :invalid_transaction}

  defp valid_id?(id), do: is_binary(id) and byte_size(id) > 0

  defp validate_initial_balances(balances) when is_map(balances) do
    if Enum.all?(balances, fn {account, amount} ->
         is_binary(account) and is_integer(amount) and amount >= 0
       end) do
      {:ok, balances}
    else
      {:error, :invalid_initial_balances}
    end
  end

  defp validate_initial_balances(_), do: {:error, :invalid_initial_balances}

  defp valid_tick(value) when is_integer(value) and value > 0, do: value
  defp valid_tick(_), do: 1_000
end\Extension
