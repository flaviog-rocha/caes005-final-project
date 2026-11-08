# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir comunicação entre diferentes processos.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  @moduledoc """
  Gerenciador de transações financeiras com supervisão, comunicação entre
  processos, notificações e persistência em arquivo.

  Os valores monetários são representados como inteiros, por exemplo, em centavos.
  """

  alias FinancialTransactions.Server

  @default_server Server
  @default_supervisor FinancialTransactions.Supervisor

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :supervisor_name, @default_supervisor)
    Supervisor.start_link(FinancialTransactions.Supervisor, opts, name: name)
  end

  def transact(type, amount, server \\ @default_server) do
    GenServer.call(server, {:transact, type, amount})
  end

  def balance(server \\ @default_server) do
    GenServer.call(server, :balance)
  end

  def transactions(server \\ @default_server) do
    GenServer.call(server, :transactions)
  end

  def subscribe(server \\ @default_server) do
    GenServer.call(server, {:subscribe, self()})
  end

  def unsubscribe(server \\ @default_server) do
    GenServer.call(server, {:unsubscribe, self()})
  end
end

defmodule FinancialTransactions.Supervisor do
  use Supervisor

  def start_link(opts) do
    name = Keyword.get(opts, :supervisor_name, FinancialTransactions.Supervisor)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    children = [
      %{
        id: FinancialTransactions.Server,
        start: {FinancialTransactions.Server, :start_link, [opts]},
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule FinancialTransactions.Server do
  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :server_name, __MODULE__)
    genserver_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, genserver_opts)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "financial_transactions.term")
    threshold = Keyword.get(opts, :threshold, 0)

    if is_integer(threshold) do
      case load(path) do
        {:ok, persisted} ->
          {:ok, Map.merge(persisted, %{path: path, threshold: threshold, subscribers: MapSet.new()})}

        {:error, :enoent} ->
          state = initial_state(path, threshold)

          case persist(state) do
            :ok -> {:ok, state}
            {:error, reason} -> {:stop, {:persistence_error, reason}}
          end

        {:error, reason} ->
          {:stop, {:load_error, reason}}
      end
    else
      {:stop, :invalid_threshold}
    end
  end

  @impl true
  def handle_call(:balance, _from, state) do
    {:reply, state.balance, state}
  end

  def handle_call(:transactions, _from, state) do
    {:reply, Enum.reverse(state.transactions), state}
  end

  def handle_call({:subscribe, pid}, _from, state) when is_pid(pid) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) when is_pid(pid) do
    {:reply, :ok, %{state | subscribers: MapSet.delete(state.subscribers, pid)}}
  end

  def handle_call({:transact, type, amount}, _from, state)
      when type in [:credit, :debit] and is_integer(amount) and amount > 0 do
    transact(type, amount, state)
  end

  def handle_call({:transact, _type, _amount}, _from, state) do
    {:reply, {:error, :invalid_transaction}, state}
  end

  defp transact(:debit, amount, %{balance: balance} = state) when amount > balance do
    {:reply, {:error, :insufficient_funds}, state}
  end

  defp transact(type, amount, state) do
    new_balance =
      case type do
        :credit -> state.balance + amount
        :debit -> state.balance - amount
      end

    transaction = %{
      id: state.next_id,
      type: type,
      amount: amount,
      balance: new_balance,
      timestamp: System.system_time(:millisecond)
    }

    new_state = %{
      state
      | balance: new_balance,
        transactions: [transaction | state.transactions],
        next_id: state.next_id + 1
    }

    case persist(new_state) do
      :ok ->
        notify_if_threshold_reached(state, new_state, transaction)
        {:reply, {:ok, transaction}, new_state}

      {:error, reason} ->
        {:reply, {:error, {:persistence_error, reason}}, state}
    end
  end

  defp notify_if_threshold_reached(old_state, new_state, transaction) do
    if old_state.balance > new_state.threshold and new_state.balance <= new_state.threshold do
      notification = %{
        event: :threshold_reached,
        balance: new_state.balance,
        threshold: new_state.threshold,
        transaction: transaction
      }

      Enum.each(new_state.subscribers, fn pid ->
        send(pid, {:financial_notification, notification})
      end)
    end
  end

  defp initial_state(path, threshold) do
    %{
      path: path,
      threshold: threshold,
      balance: 0,
      transactions: [],
      next_id: 1,
      subscribers: MapSet.new()
    }
  end

  defp load(path) do
    with {:ok, binary} <- File.read(path),
         {:ok, data} <- decode(binary),
         true <- valid_snapshot?(data) do
      {:ok, data}
    else
      false -> {:error, :invalid_snapshot}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode(binary) do
    try do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    rescue
      _ -> {:error, :invalid_snapshot}
    catch
      _, _ -> {:error, :invalid_snapshot}
    end
  end

  defp valid_snapshot?(data) when is_map(data) do
    is_integer(Map.get(data, :balance)) and
      is_list(Map.get(data, :transactions)) and
      is_integer(Map.get(data, :next_id)) and
      Map.get(data, :next_id) > 0
  end

  defp valid_snapshot?(_), do: false

  defp persist(state) do
    snapshot = %{
      balance: state.balance,
      transactions: state.transactions,
      next_id: state.next_id
    }

    path = state.path
    temporary_path = path <> ".tmp"
    binary = :erlang.term_to_binary(snapshot)

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary_path, binary, [:binary]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    end
  end
end
