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

defmodule TransacoesFinanceiras do
  @moduledoc """
  API para gerenciamento de transações financeiras.

  As notificações de saldo baixo são enviadas aos processos inscritos como:
  `{:saldo_baixo, conta, saldo, limite}`.
  """

  def start_link(opts \\ []) do
    TransacoesFinanceiras.Supervisor.start_link(opts)
  end

  def start_server(opts \\ []) do
    TransacoesFinanceiras.Server.start_link(opts)
  end

  def server(supervisor) do
    case Supervisor.which_children(supervisor) do
      [{TransacoesFinanceiras.Server, pid, :worker, _modules}] -> pid
      _ -> nil
    end
  end

  def deposit(server, account, amount), do: GenServer.call(server, {:deposit, account, amount})
  def withdraw(server, account, amount), do: GenServer.call(server, {:withdraw, account, amount})

  def transfer(server, from_account, to_account, amount) do
    GenServer.call(server, {:transfer, from_account, to_account, amount})
  end

  def balance(server, account), do: GenServer.call(server, {:balance, account})
  def transactions(server), do: GenServer.call(server, :transactions)
  def subscribe(server), do: GenServer.call(server, {:subscribe, self()})
  def unsubscribe(server), do: GenServer.call(server, {:unsubscribe, self()})

  def set_threshold(server, threshold) do
    GenServer.call(server, {:set_threshold, threshold})
  end
end

defmodule TransacoesFinanceiras.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    server_opts =
      opts
      |> Keyword.take([:path, :threshold])
      |> maybe_add_server_name(Keyword.get(opts, :server_name))

    children = [
      %{
        id: TransacoesFinanceiras.Server,
        start: {TransacoesFinanceiras.Server, :start_link, [server_opts]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp maybe_add_server_name(opts, nil), do: opts
  defp maybe_add_server_name(opts, name), do: Keyword.put(opts, :name, name)
end

defmodule TransacoesFinanceiras.Server do
  use GenServer

  @default_path "transacoes_financeiras.db"

  def start_link(opts \\ []) do
    server_opts =
      case Keyword.fetch(opts, :name) do
        {:ok, name} -> [name: name]
        :error -> []
      end

    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, @default_path) |> Path.expand()
    initial_threshold = Keyword.get(opts, :threshold)

    with :ok <- validate_threshold(initial_threshold),
         {:ok, persisted} <- load_or_initialize(path, initial_threshold) do
      {:ok,
       %{
         path: path,
         balances: persisted.balances,
         transactions: persisted.transactions,
         threshold: persisted.threshold,
         subscribers: %{}
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:deposit, account, amount}, _from, state) do
    with :ok <- validate_account(account),
         :ok <- validate_amount(amount) do
      balance = Map.get(state.balances, account, 0)
      balances = Map.put(state.balances, account, balance + amount)

      transaction = %{
        type: "deposit",
        account: account,
        amount: amount,
        at: System.system_time(:millisecond)
      }

      commit_transaction(state, balances, transaction, [account])
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:withdraw, account, amount}, _from, state) do
    with :ok <- validate_account(account),
         :ok <- validate_amount(amount),
         balance = Map.get(state.balances, account, 0),
         true <- balance >= amount do
      balances = Map.put(state.balances, account, balance - amount)

      transaction = %{
        type: "withdraw",
        account: account,
        amount: amount,
        at: System.system_time(:millisecond)
      }

      commit_transaction(state, balances, transaction, [account])
    else
      false -> {:reply, {:error, :insufficient_funds}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:transfer, from_account, to_account, amount}, _from, state) do
    with :ok <- validate_account(from_account),
         :ok <- validate_account(to_account),
         false <- from_account == to_account,
         :ok <- validate_amount(amount),
         from_balance = Map.get(state.balances, from_account, 0),
         true <- from_balance >= amount do
      to_balance = Map.get(state.balances, to_account, 0)

      balances =
        state.balances
        |> Map.put(from_account, from_balance - amount)
        |> Map.put(to_account, to_balance + amount)

      transaction = %{
        type: "transfer",
        from: from_account,
        to: to_account,
        amount: amount,
        at: System.system_time(:millisecond)
      }

      commit_transaction(state, balances, transaction, [from_account, to_account])
    else
      true -> {:reply, {:error, :same_account}, state}
      false -> {:reply, {:error, :insufficient_funds}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:balance, account}, _from, state) do
    case validate_account(account) do
      :ok -> {:reply, {:ok, Map.get(state.balances, account, 0)}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:transactions, _from, state) do
    {:reply, {:ok, Enum.reverse(state.transactions)}, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    subscribers =
      if Map.has_key?(state.subscribers, pid) do
        state.subscribers
      else
        Map.put(state.subscribers, pid, Process.monitor(pid))
      end

    {:reply, :ok, %{state | subscribers: subscribers}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, subscribers} ->
        {:reply, :ok, %{state | subscribers: subscribers}}

      {monitor, subscribers} ->
        Process.demonitor(monitor, [:flush])
        {:reply, :ok, %{state | subscribers: subscribers}}
    end
  end

  def handle_call({:set_threshold, threshold}, _from, state) do
    with :ok <- validate_threshold(threshold) do
      new_state = %{state | threshold: threshold}

      case persist(new_state) do
        :ok -> {:reply, :ok, new_state}
        {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    subscribers =
      case Map.get(state.subscribers, pid) do
        ^monitor -> Map.delete(state.subscribers, pid)
        _ -> state.subscribers
      end

    {:noreply, %{state | subscribers: subscribers}}
  end

  defp commit_transaction(state, balances, transaction, affected_accounts) do
    new_state = %{
      state
      | balances: balances,
        transactions: [transaction | state.transactions]
    }

    case persist(new_state) do
      :ok ->
        notify_low_balances(state, new_state, affected_accounts)
        {:reply, {:ok, transaction}, new_state}

      {:error, reason} ->
        {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  defp notify_low_balances(%{threshold: nil}, _new_state, _accounts), do: :ok

  defp notify_low_balances(old_state, new_state, accounts) do
    threshold = new_state.threshold

    Enum.each(accounts, fn account ->
      old_balance = Map.get(old_state.balances, account, 0)
      new_balance = Map.get(new_state.balances, account, 0)

      if old_balance > threshold and new_balance <= threshold do
        Enum.each(new_state.subscribers, fn {pid, _monitor} ->
          send(pid, {:saldo_baixo, account, new_balance, threshold})
        end)
      end
    end)
  end

  defp load_or_initialize(path, threshold) do
    case File.read(path) do
      {:ok, contents} ->
        decode_persisted(contents)

      {:error, :enoent} ->
        persisted = %{balances: %{}, transactions: [], threshold: threshold}

        case persist_to_path(path, persisted) do
          :ok -> {:ok, persisted}
          {:error, reason} -> {:error, {:persistence_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:persistence_failed, reason}}
    end
  end

  defp decode_persisted(contents) do
    try do
      case :erlang.binary_to_term(contents, [:safe]) do
        %{version: 1, balances: balances, transactions: transactions, threshold: threshold}
        when is_map(balances) and is_list(transactions) ->
          if valid_persisted_data?(balances, threshold) do
            {:ok, %{balances: balances, transactions: transactions, threshold: threshold}}
          else
            {:error, :invalid_persisted_data}
          end

        _ ->
          {:error, :invalid_persisted_data}
      end
    rescue
      _ -> {:error, :invalid_persisted_data}
    end
  end

  defp valid_persisted_data?(balances, threshold) do
    Enum.all?(balances, fn
      {account, amount} -> is_binary(account) and is_integer(amount) and amount >= 0
      _ -> false
    end) and valid_threshold?(threshold)
  end

  defp persist(state) do
    persisted = %{
      version: 1,
      balances: state.balances,
      transactions: state.transactions,
      threshold: state.threshold
    }

    persist_to_path(state.path, persisted)
  end

  defp persist_to_path(path, persisted) do
    temporary_path = path <> ".tmp"
    contents = :erlang.term_to_binary(persisted, [:compressed])

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary_path, contents, [:binary]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end

  defp validate_account(account) when is_binary(account) and byte_size(account) > 0, do: :ok
  defp validate_account(_account), do: {:error, :invalid_account}

  defp validate_amount(amount) when is_integer(amount) and amount > 0, do: :ok
  defp validate_amount(_amount), do: {:error, :invalid_amount}

  defp validate_threshold(threshold) do
    if valid_threshold?(threshold), do: :ok, else: {:error, :invalid_threshold}
  end

  defp valid_threshold?(nil), do: true
  defp valid_threshold?(threshold), do: is_integer(threshold) and threshold >= 0
end
