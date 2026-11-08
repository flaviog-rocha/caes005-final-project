# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir a execução de operações simultâneas.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  use Supervisor

  alias FinancialTransactions.{Database, Transaction}

  @default_database FinancialTransactions.Database
  @default_transaction_supervisor FinancialTransactions.TransactionSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(
      __MODULE__,
      opts,
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  @impl true
  def init(opts) do
    accounts = Keyword.get(opts, :accounts, %{})
    database_name = Keyword.get(opts, :database, @default_database)
    transaction_supervisor_name =
      Keyword.get(opts, :transaction_supervisor, @default_transaction_supervisor)

    max_concurrency = Keyword.get(opts, :max_concurrency, 10)

    children = [
      Agent.child_spec({fn -> %{accounts: accounts, transactions: %{}} end, name: database_name}),
      DynamicSupervisor.child_spec(
        strategy: :one_for_one,
        name: transaction_supervisor_name,
        max_children: max_concurrency
      )
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Inicia uma transferência assíncrona. O resultado chega como {ref, resultado}
  # ao processo que chamou esta função.
  def transfer(from, to, amount, opts \\ []) do
    ref = make_ref()

    args = %{
      ref: ref,
      owner: self(),
      database: Keyword.get(opts, :database, @default_database),
      from: from,
      to: to,
      amount: amount
    }

    supervisor =
      Keyword.get(opts, :supervisor, @default_transaction_supervisor)

    case DynamicSupervisor.start_child(supervisor, {Transaction, args}) do
      {:ok, _pid} -> {:ok, ref}
      {:ok, _pid, _info} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  def await(ref, timeout \\ 5_000) do
    receive do
      {^ref, result} -> result
    after
      timeout -> {:error, :timeout}
    end
  end

  def balance(account, opts \\ []) do
    opts
    |> Keyword.get(:database, @default_database)
    |> Database.balance(account)
  end

  def accounts(opts \\ []) do
    opts
    |> Keyword.get(:database, @default_database)
    |> Database.accounts()
  end

  def transactions(opts \\ []) do
    opts
    |> Keyword.get(:database, @default_database)
    |> Database.transactions()
  end
end

defmodule FinancialTransactions.Database do
  @moduledoc false

  def balance(database, account) do
    Agent.get(database, fn state ->
      case Map.fetch(state.accounts, account) do
        {:ok, amount} -> {:ok, amount}
        :error -> {:error, :account_not_found}
      end
    end)
  end

  def accounts(database) do
    Agent.get(database, & &1.accounts)
  end

  def transactions(database) do
    Agent.get(database, fn state ->
      state.transactions
      |> Map.values()
      |> Enum.sort_by(& &1.created_at, DateTime)
    end)
  end

  def transfer(_database, _id, _from, _to, amount)
      when not is_integer(amount) or amount <= 0 do
    {:error, :invalid_amount}
  end

  def transfer(_database, _id, from, to, _amount) when from == to do
    {:error, :same_account}
  end

  def transfer(database, id, from, to, amount) do
    Agent.get_and_update(database, fn state ->
      {result, updated_accounts, status} =
        apply_transfer(state.accounts, from, to, amount)

      transaction = %{
        id: id,
        from: from,
        to: to,
        amount: amount,
        status: status,
        result: result,
        created_at: DateTime.utc_now()
      }

      updated_state = %{
        state
        | accounts: updated_accounts,
          transactions: Map.put(state.transactions, id, transaction)
      }

      {result, updated_state}
    end)
  end

  defp apply_transfer(accounts, from, to, amount) do
    case {Map.fetch(accounts, from), Map.fetch(accounts, to)} do
      {{:ok, source_balance}, {:ok, destination_balance}}
      when source_balance >= amount ->
        updated_accounts =
          accounts
          |> Map.put(from, source_balance - amount)
          |> Map.put(to, destination_balance + amount)

        result =
          {:ok,
           %{
             from: from,
             to: to,
             amount: amount,
             status: :completed
           }}

        {result, updated_accounts, :completed}

      {{:ok, _source_balance}, {:ok, _destination_balance}} ->
        {{:error, :insufficient_funds}, accounts, :rejected}

      {:error, _} ->
        {{:error, :account_not_found}, accounts, :rejected}

      {_, :error} ->
        {{:error, :account_not_found}, accounts, :rejected}
    end
  end
end

defmodule FinancialTransactions.Transaction do
  use GenServer

  alias FinancialTransactions.Database

  @moduledoc false

  def child_spec(args) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, args) do
    result =
      try do
        Database.transfer(
          args.database,
          args.ref,
          args.from,
          args.to,
          args.amount
        )
      rescue
        error -> {:error, {:database_error, error.__struct__}}
      catch
        kind, reason -> {:error, {:database_error, kind, reason}}
      end

    send(args.owner, {args.ref, result})
    {:stop, :normal, args}
  end
end
