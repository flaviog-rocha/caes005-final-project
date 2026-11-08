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
  @moduledoc """
  Gerenciamento simples de transações financeiras com workers temporários.

  Os valores são expressos em unidades monetárias mínimas inteiras, como centavos.
  """

  use Supervisor

  alias FinancialTransactions.{Store, TransactionWorker}

  @default_max_concurrent 10

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> Supervisor.start_link(__MODULE__, opts, name: name)
      :error -> Supervisor.start_link(__MODULE__, opts)
    end
  end

  @impl true
  def init(opts) do
    max_concurrent = Keyword.get(opts, :max_concurrent, @default_max_concurrent)
    initial_balances = Keyword.get(opts, :initial_balances, %{})

    unless is_integer(max_concurrent) and max_concurrent > 0 do
      raise ArgumentError, ":max_concurrent deve ser um inteiro positivo"
    end

    children = [
      %{
        id: :store,
        start: {Store, :start_link, [[initial_balances: initial_balances]]},
        restart: :permanent,
        type: :worker
      },
      %{
        id: :transactions,
        start:
          {DynamicSupervisor, :start_link,
           [[strategy: :one_for_one, max_children: max_concurrent]]},
        restart: :permanent,
        type: :supervisor
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def transfer(server, from, to, amount) do
    run_transaction(server, {:transfer, from, to, amount})
  end

  def deposit(server, account, amount) do
    run_transaction(server, {:deposit, account, amount})
  end

  def withdraw(server, account, amount) do
    run_transaction(server, {:withdraw, account, amount})
  end

  def balance(server, account) do
    with {:ok, store} <- child_pid(server, :store) do
      Store.balance(store, account)
    end
  end

  def ledger(server) do
    with {:ok, store} <- child_pid(server, :store) do
      Store.ledger(store)
    end
  end

  defp run_transaction(server, operation) do
    with {:ok, store} <- child_pid(server, :store),
         {:ok, dynamic_supervisor} <- child_pid(server, :transactions),
         {:ok, worker} <-
           DynamicSupervisor.start_child(
             dynamic_supervisor,
             %{
               id: make_ref(),
               start: {TransactionWorker, :start_link, [[store: store, operation: operation]]},
               restart: :temporary,
               type: :worker
             }
           ) do
      GenServer.call(worker, :execute, :infinity)
    end
  end

  defp child_pid(server, id) do
    case Enum.find(Supervisor.which_children(server), fn {child_id, _, _, _} ->
           child_id == id
         end) do
      {^id, pid, _, _} when is_pid(pid) -> {:ok, pid}
      _ -> {:error, :not_started}
    end
  end
end

defmodule FinancialTransactions.TransactionWorker do
  use GenServer

  alias FinancialTransactions.Store

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    {:ok, %{store: Keyword.fetch!(opts, :store), operation: Keyword.fetch!(opts, :operation)}}
  end

  @impl true
  def handle_call(:execute, _from, state) do
    result = Store.execute(state.store, state.operation)
    {:stop, :normal, result, state}
  end
end

defmodule FinancialTransactions.Store do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def balance(store, account) do
    GenServer.call(store, {:balance, account})
  end

  def ledger(store) do
    GenServer.call(store, :ledger)
  end

  def execute(store, operation) do
    GenServer.call(store, {:execute, operation})
  end

  @impl true
  def init(opts) do
    balances = Keyword.get(opts, :initial_balances, %{})

    unless is_map(balances) and
             Enum.all?(balances, fn {_account, amount} ->
               is_integer(amount) and amount >= 0
             end) do
      raise ArgumentError, "initial_balances deve mapear contas para saldos inteiros não negativos"
    end

    {:ok, %{balances: balances, ledger: [], next_id: 1}}
  end

  @impl true
  def handle_call({:balance, account}, _from, state) do
    reply =
      case Map.fetch(state.balances, account) do
        {:ok, amount} -> {:ok, amount}
        :error -> {:error, :account_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:ledger, _from, state) do
    {:reply, Enum.reverse(state.ledger), state}
  end

  def handle_call({:execute, operation}, _from, state) do
    {reply, new_state} = apply_operation(operation, state)
    {:reply, reply, new_state}
  end

  defp apply_operation({:transfer, from, to, amount}, state)
       when from != to and is_integer(amount) and amount > 0 do
    with {:ok, from_balance} <- fetch_balance(state.balances, from),
         {:ok, to_balance} <- fetch_balance(state.balances, to),
         true <- from_balance >= amount do
      balances =
        state.balances
        |> Map.put(from, from_balance - amount)
        |> Map.put(to, to_balance + amount)

      commit(state, balances, :transfer, amount, %{from: from, to: to})
    else
      false -> {{:error, :insufficient_funds}, state}
      {:error, _} = error -> {error, state}
    end
  end

  defp apply_operation({:transfer, from, to, _amount}, _state) when from == to do
    {{:error, :same_account}, _state}
  end

  defp apply_operation({:deposit, account, amount}, state)
       when is_integer(amount) and amount > 0 do
    with {:ok, balance} <- fetch_balance(state.balances, account) do
      balances = Map.put(state.balances, account, balance + amount)
      commit(state, balances, :deposit, amount, %{account: account})
    else
      {:error, _} = error -> {error, state}
    end
  end

  defp apply_operation({:withdraw, account, amount}, state)
       when is_integer(amount) and amount > 0 do
    with {:ok, balance} <- fetch_balance(state.balances, account),
         true <- balance >= amount do
      balances = Map.put(state.balances, account, balance - amount)
      commit(state, balances, :withdrawal, amount, %{account: account})
    else
      false -> {{:error, :insufficient_funds}, state}
      {:error, _} = error -> {error, state}
    end
  end

  defp apply_operation(_operation, state) do
    {{:error, :invalid_operation_or_amount}, state}
  end

  defp fetch_balance(balances, account) do
    case Map.fetch(balances, account) do
      {:ok, balance} -> {:ok, balance}
      :error -> {:error, :account_not_found}
    end
  end

  defp commit(state, balances, type, amount, details) do
    entry =
      Map.merge(details, %{
        id: state.next_id,
        type: type,
        amount: amount,
        timestamp: System.system_time(:millisecond)
      })

    new_state = %{
      state
      | balances: balances,
        ledger: [entry | state.ledger],
        next_id: state.next_id + 1
    }

    {{:ok, entry}, new_state}
  end
end
