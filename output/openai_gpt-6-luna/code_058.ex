# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir consultar o estado atual das operações em andamento.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialTransactions do
  @moduledoc """
  Gerenciador local de contas e transferências.

  As transferências ficam em estado `:in_progress` até `execute/2` ser chamada.
  O estado completo é persistido em um único registro DETS antes de atualizar
  o cache ETS. Assim, a alteração dos saldos e a conclusão da operação são
  recuperadas juntas após uma reinicialização.
  """

  use GenServer

  @dets_name :financial_transactions_dets
  @snapshot_key :snapshot

  @type operation_id :: term()
  @type account_id :: term()

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def stop(server \\ __MODULE__), do: GenServer.stop(server)

  @doc "Cria uma conta com saldo inicial em unidades monetárias mínimas."
  def open_account(account_id, initial_balance, server \\ __MODULE__) do
    GenServer.call(server, {:open_account, account_id, initial_balance})
  end

  @doc "Registra uma transferência, inicialmente em estado `:in_progress`."
  def create_transfer(operation_id, from_account, to_account, amount, server \\ __MODULE__) do
    GenServer.call(
      server,
      {:create_transfer, operation_id, from_account, to_account, amount}
    )
  end

  @doc "Executa uma transferência no máximo uma vez."
  def execute(operation_id, server \\ __MODULE__) do
    GenServer.call(server, {:execute, operation_id})
  end

  @doc "Consulta uma operação pelo identificador."
  def status(operation_id, server \\ __MODULE__) do
    GenServer.call(server, {:status, operation_id})
  end

  @doc "Lista as operações que ainda estão em andamento."
  def in_progress(server \\ __MODULE__) do
    GenServer.call(server, :in_progress)
  end

  @doc "Consulta o saldo de uma conta."
  def balance(account_id, server \\ __MODULE__) do
    GenServer.call(server, {:balance, account_id})
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "financial_transactions.dets")

    case :dets.open_file(@dets_name, file: path, type: :set) do
      {:ok, dets} ->
        data =
          case :dets.lookup(dets, @snapshot_key) do
            [{@snapshot_key, stored_data}] ->
              stored_data

            [] ->
              %{accounts: %{}, operations: %{}}
          end

        ets = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
        true = :ets.insert(ets, {@snapshot_key, data})

        {:ok, %{dets: dets, ets: ets}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:open_account, account_id, initial_balance}, _from, server)
      when is_integer(initial_balance) and initial_balance >= 0 do
    data = read_data(server)

    if Map.has_key?(data.accounts, account_id) do
      {:reply, {:error, :account_exists}, server}
    else
      new_data = put_in(data, [:accounts, account_id], initial_balance)
      commit_reply(server, new_data, {:ok, initial_balance})
    end
  end

  def handle_call({:open_account, _account_id, _initial_balance}, _from, server) do
    {:reply, {:error, :invalid_initial_balance}, server}
  end

  @impl true
  def handle_call(
        {:create_transfer, operation_id, from_account, to_account, amount},
        _from,
        server
      )
      when is_integer(amount) and amount > 0 and from_account != to_account do
    data = read_data(server)

    operation = %{
      id: operation_id,
      from: from_account,
      to: to_account,
      amount: amount,
      status: :in_progress
    }

    case Map.fetch(data.operations, operation_id) do
      {:ok, ^operation} ->
        {:reply, {:ok, operation}, server}

      {:ok, _existing} ->
        {:reply, {:error, :operation_id_conflict}, server}

      :error ->
        new_data = put_in(data, [:operations, operation_id], operation)
        commit_reply(server, new_data, {:ok, operation})
    end
  end

  def handle_call(
        {:create_transfer, _operation_id, _from_account, _to_account, _amount},
        _from,
        server
      ) do
    {:reply, {:error, :invalid_transfer}, server}
  end

  @impl true
  def handle_call({:execute, operation_id}, _from, server) do
    data = read_data(server)

    case Map.fetch(data.operations, operation_id) do
      :error ->
        {:reply, {:error, :operation_not_found}, server}

      {:ok, %{status: :completed} = operation} ->
        {:reply, {:ok, operation}, server}

      {:ok, operation} ->
        execute_operation(server, data, operation)
    end
  end

  @impl true
  def handle_call({:status, operation_id}, _from, server) do
    data = read_data(server)

    case Map.fetch(data.operations, operation_id) do
      {:ok, operation} -> {:reply, {:ok, operation}, server}
      :error -> {:reply, {:error, :operation_not_found}, server}
    end
  end

  @impl true
  def handle_call(:in_progress, _from, server) do
    data = read_data(server)

    operations =
      data.operations
      |> Map.values()
      |> Enum.filter(&(&1.status == :in_progress))

    {:reply, {:ok, operations}, server}
  end

  @impl true
  def handle_call({:balance, account_id}, _from, server) do
    data = read_data(server)

    case Map.fetch(data.accounts, account_id) do
      {:ok, amount} -> {:reply, {:ok, amount}, server}
      :error -> {:reply, {:error, :account_not_found}, server}
    end
  end

  defp execute_operation(server, data, operation) do
    with {:ok, from_balance} <- Map.fetch(data.accounts, operation.from),
         {:ok, to_balance} <- Map.fetch(data.accounts, operation.to),
         true <- from_balance >= operation.amount do
      completed = %{operation | status: :completed}

      new_data =
        data
        |> put_in([:accounts, operation.from], from_balance - operation.amount)
        |> put_in([:accounts, operation.to], to_balance + operation.amount)
        |> put_in([:operations, operation.id], completed)

      commit_reply(server, new_data, {:ok, completed})
    else
      :error ->
        {:reply, {:error, :account_not_found}, server}

      false ->
        {:reply, {:error, :insufficient_funds}, server}
    end
  end

  defp read_data(server) do
    [{@snapshot_key, data}] = :ets.lookup(server.ets, @snapshot_key)
    data
  end

  defp commit_reply(server, new_data, reply) do
    case :dets.insert(server.dets, {@snapshot_key, new_data}) do
      :ok ->
        case :dets.sync(server.dets) do
          :ok ->
            true = :ets.insert(server.ets, {@snapshot_key, new_data})
            {:reply, reply, server}

          {:error, reason} ->
            {:stop, {:persistence_failed, reason}, {:error, {:persistence, reason}}, server}
        end

      {:error, reason} ->
        {:stop, {:persistence_failed, reason}, {:error, {:persistence, reason}}, server}
    end
  end

  @impl true
  def terminate(_reason, server) do
    :dets.close(server.dets)
    :ok
  end
end
