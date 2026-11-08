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
  use GenServer

  @moduledoc """
  Gerenciador de transferências com histórico persistente e recuperação após reinício.
  """

  defmacro __using__(_opts) do
    quote do
      import FinancialTransactions, only: [transfer: 3]
    end
  end

  defmacro transfer(from, to, amount) do
    quote do
      FinancialTransactions.submit(unquote(from), unquote(to), unquote(amount))
    end
  end

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def submit(from, to, amount, server \\ __MODULE__) do
    GenServer.call(server, {:submit, from, to, amount})
  end

  def operations(server \\ __MODULE__), do: GenServer.call(server, :operations)
  def history(server \\ __MODULE__), do: GenServer.call(server, :history)
  def balances(server \\ __MODULE__), do: GenServer.call(server, :balances)
  def run_once(server \\ __MODULE__), do: GenServer.call(server, :run_once)

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, Path.expand("financial_transactions.log"))
    interval = Keyword.get(opts, :interval, 1_000)
    initial_balances = Keyword.get(opts, :balances, %{})

    with true <- is_integer(interval) and interval > 0,
         :ok <- validate_balances(initial_balances),
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, events} <- read_events(path) do
      state = %{
        path: path,
        interval: interval,
        operations: %{},
        balances: initial_balances,
        history: [],
        next_id: 1,
        initialized?: false
      }

      state = Enum.reduce(events, state, fn event, acc -> apply_event(event, acc) end)

      with {:ok, state} <- initialize_journal(state, initial_balances),
           {:ok, state} <- recover_in_progress(state) do
        Process.send_after(self(), :tick, interval)
        {:ok, state}
      else
        {:error, reason} -> {:stop, reason}
      end
    else
      false -> {:stop, :invalid_interval}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:submit, from, to, amount}, _from, state) do
    if valid_account?(from) and valid_account?(to) and is_integer(amount) and amount > 0 do
      id = state.next_id

      operation = %{
        id: id,
        from: from,
        to: to,
        amount: amount,
        submitted_at: System.system_time(:millisecond),
        status: :pending
      }

      case append_and_apply({:submitted, operation}, state) do
        {:ok, new_state} ->
          {:reply, {:ok, id}, %{new_state | next_id: id + 1}}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :invalid_transfer}, state}
    end
  end

  def handle_call(:operations, _from, state) do
    operations =
      state.operations
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, operations, state}
  end

  def handle_call(:history, _from, state), do: {:reply, Enum.reverse(state.history), state}
  def handle_call(:balances, _from, state), do: {:reply, state.balances, state}

  def handle_call(:run_once, _from, state) do
    {:reply, :ok, dispatch(state)}
  end

  @impl true
  def handle_info(:tick, state) do
    state = dispatch(state)
    Process.send_after(self(), :tick, state.interval)
    {:noreply, state}
  end

  def handle_info({:execute, id}, state) do
    case Map.get(state.operations, id) do
      %{status: :processing} = operation ->
        {outcome, balances} = execute_transfer(operation, state.balances)
        event = {:completed, id, outcome, balances}

        case append_and_apply(event, state) do
          {:ok, new_state} -> {:noreply, new_state}
          {:error, _reason} -> {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch(state) do
    processing =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status == :processing))
      |> Enum.sort_by(& &1.id)

    case processing do
      [operation | _] ->
        send(self(), {:execute, operation.id})
        state

      [] ->
        pending =
          state.operations
          |> Map.values()
          |> Enum.filter(&(&1.status == :pending))
          |> Enum.sort_by(& &1.id)

        case pending do
          [operation | _] ->
            case append_and_apply({:started, operation.id}, state) do
              {:ok, new_state} ->
                send(self(), {:execute, operation.id})
                new_state

              {:error, _reason} ->
                state
            end

          [] ->
            state
        end
    end
  end

  defp execute_transfer(operation, balances) do
    from_balance = Map.get(balances, operation.from, 0)

    if from_balance >= operation.amount do
      new_balances =
        if operation.from == operation.to do
          balances
        else
          balances
          |> Map.put(operation.from, from_balance - operation.amount)
          |> Map.put(operation.to, Map.get(balances, operation.to, 0) + operation.amount)
        end

      {{:ok, %{from: operation.from, to: operation.to, amount: operation.amount}}, new_balances}
    else
      {{:error, :insufficient_funds}, balances}
    end
  end

  defp initialize_journal(%{initialized?: true} = state, _initial_balances), do: {:ok, state}

  defp initialize_journal(state, initial_balances) do
    append_and_apply({:initialized, initial_balances}, state)
  end

  defp recover_in_progress(state) do
    state.operations
    |> Map.values()
    |> Enum.filter(&(&1.status == :processing))
    |> Enum.sort_by(& &1.id)
    |> Enum.reduce_while({:ok, state}, fn operation, {:ok, acc} ->
      case append_and_apply({:recovered, operation.id}, acc) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp append_and_apply(event, state) do
    encoded = event |> :erlang.term_to_binary() |> Base.encode64()

    case File.write(state.path, encoded <> "\n", [:append, :binary]) do
      :ok -> {:ok, apply_event(event, state)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_event({:initialized, balances}, state) do
    %{state | balances: balances, initialized?: true, history: [{:initialized, balances} | state.history]}
  end

  defp apply_event({:submitted, operation}, state) do
    state = %{state | operations: Map.put(state.operations, operation.id, operation)}
    %{state | history: [{:submitted, operation} | state.history], next_id: max(state.next_id, operation.id + 1)}
  end

  defp apply_event({:started, id}, state) do
    operations = Map.update(state.operations, id, nil, &%{&1 | status: :processing})
    %{state | operations: operations, history: [{:started, id} | state.history]}
  end

  defp apply_event({:completed, id, outcome, balances}, state) do
    operations =
      Map.update(state.operations, id, nil, fn operation ->
        %{operation | status: :completed, result: outcome}
      end)

    %{state | operations: operations, balances: balances, history: [{:completed, id, outcome, balances} | state.history]}
  end

  defp apply_event({:recovered, id}, state) do
    operations = Map.update(state.operations, id, nil, &%{&1 | status: :pending})
    %{state | operations: operations, history: [{:recovered, id} | state.history]}
  end

  defp read_events(path) do
    case File.read(path) do
      {:ok, content} ->
        lines = String.split(content, "\n", trim: false)
        lines = if String.ends_with?(content, "\n"), do: Enum.drop(lines, -1), else: lines

        {events, valid_lines, complete?} =
          Enum.reduce_while(lines, {[], [], true}, fn line, {events, valid_lines, _} ->
            case decode_event(line) do
              {:ok, event} -> {:cont, {[event | events], [line | valid_lines], true}}
              :error -> {:halt, {events, valid_lines, false}}
            end
          end)

        unless complete? do
          repaired =
            case Enum.reverse(valid_lines) do
              [] -> ""
              valid -> Enum.join(valid, "\n") <> "\n"
            end

          case File.write(path, repaired, [:binary]) do
            :ok -> :ok
            {:error, reason} -> throw({:journal_repair_failed, reason})
          end
        end

        {:ok, Enum.reverse(events)}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  catch
    {:journal_repair_failed, reason} -> {:error, reason}
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

  defp validate_balances(balances) when is_map(balances) do
    if Enum.all?(balances, fn {account, amount} ->
         valid_account?(account) and is_integer(amount) and amount >= 0
       end) do
      :ok
    else
      {:error, :invalid_balances}
    end
  end

  defp validate_balances(_), do: {:error, :invalid_balances}

  defp valid_account?(account) do
    is_atom(account) or is_binary(account) or is_integer(account)
  end
end

defmodule FinancialTransactions.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :supervisor_name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    server_opts = Keyword.drop(opts, [:supervisor_name])
    children = [FinancialTransactions.child_spec(server_opts)]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
