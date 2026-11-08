# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Implementar expiração automática de registros.
# 3. Manter um histórico das operações realizadas.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de transações financeiras com expiração automática,
# histórico de operações e idempotência. Usa apenas a biblioteca padrão
# (GenServer, Process, System). Não depende de banco de dados.
#
# Uso básico:
#   {:ok, _} = Finance.Ledger.start_link(name: :ledger, ttl_ms: 60_000)
#   :ok = Finance.Ledger.open_account(:ledger, "conta_a", 10_000)
#   :ok = Finance.Ledger.open_account(:ledger, "conta_b", 0)
#   {:ok, txn} = Finance.Ledger.request(:ledger, %{type: :transfer, from: "conta_a", to: "conta_b", amount: 2_500}, "op-123")
#   {:error, {:duplicate, _}} = Finance.Ledger.request(:ledger, %{type: :transfer, from: "conta_a", to: "conta_b", amount: 2_500}, "op-123")
#   {:ok, _} = Finance.Ledger.confirm(:ledger, txn.id)
#   Finance.Ledger.history(:ledger, transaction_id: txn.id)

defmodule Finance.Transaction do
  @moduledoc """
  Regras puras para criação e aplicação de transações financeiras.

  Valores monetários são inteiros (centavos) para evitar erros de ponto flutuante.
  Uma transação nasce `:pending` (autorizada, mas ainda não aplicada) e possui
  um prazo de validade. Só pode ser concluída enquanto estiver válida.
  """

  @types [:deposit, :withdrawal, :transfer]

  @enforce_keys [:id, :type, :amount, :idempotency_key, :status, :created_at, :expires_at]
  defstruct [
    :id,
    :type,
    :from,
    :to,
    :amount,
    :idempotency_key,
    :status,
    :created_at,
    :expires_at,
    :settled_at
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          type: :deposit | :withdrawal | :transfer,
          from: String.t() | nil,
          to: String.t() | nil,
          amount: pos_integer(),
          idempotency_key: String.t(),
          status: :pending | :completed | :failed | :cancelled | :expired,
          created_at: integer(),
          expires_at: integer(),
          settled_at: integer() | nil
        }

  @doc "Valida os atributos e cria uma transação pendente."
  @spec new(map(), String.t(), integer(), pos_integer()) :: {:ok, t()} | {:error, atom()}
  def new(attrs, id, now, ttl_ms) when is_map(attrs) do
    with {:ok, type} <- validate_type(Map.get(attrs, :type)),
         {:ok, amount} <- validate_amount(Map.get(attrs, :amount)),
         {:ok, from, to} <- validate_accounts(type, attrs),
         {:ok, key} <- validate_key(Map.get(attrs, :idempotency_key)) do
      {:ok,
       %__MODULE__{
         id: id,
         type: type,
         from: from,
         to: to,
         amount: amount,
         idempotency_key: key,
         status: :pending,
         created_at: now,
         expires_at: now + ttl_ms
       }}
    end
  end

  @doc "Indica se uma transação pendente já ultrapassou seu prazo de validade."
  @spec expired?(t(), integer()) :: boolean()
  def expired?(%__MODULE__{status: :pending, expires_at: exp}, now), do: now >= exp
  def expired?(_txn, _now), do: false

  @doc "Aplica os efeitos da transação sobre os saldos, retornando os novos saldos."
  @spec apply_effects(map(), t()) :: {:ok, map()} | {:error, :insufficient_funds}
  def apply_effects(balances, %__MODULE__{type: :deposit, to: to, amount: amt}) do
    {:ok, Map.update!(balances, to, &(&1 + amt))}
  end

  def apply_effects(balances, %__MODULE__{type: :withdrawal, from: from, amount: amt}) do
    debit(balances, from, amt)
  end

  def apply_effects(balances, %__MODULE__{type: :transfer, from: from, to: to, amount: amt}) do
    with {:ok, debited} <- debit(balances, from, amt) do
      {:ok, Map.update!(debited, to, &(&1 + amt))}
    end
  end

  defp debit(balances, account, amt) do
    case Map.fetch!(balances, account) do
      bal when bal >= amt -> {:ok, Map.put(balances, account, bal - amt)}
      _ -> {:error, :insufficient_funds}
    end
  end

  defp validate_type(type) when type in @types, do: {:ok, type}
  defp validate_type(_), do: {:error, :invalid_type}

  defp validate_amount(amount) when is_integer(amount) and amount > 0, do: {:ok, amount}
  defp validate_amount(_), do: {:error, :invalid_amount}

  defp validate_accounts(:deposit, %{to: to}) when is_binary(to), do: {:ok, nil, to}

  defp validate_accounts(:withdrawal, %{from: from}) when is_binary(from),
    do: {:ok, from, nil}

  defp validate_accounts(:transfer, %{from: from, to: to})
       when is_binary(from) and is_binary(to) and from != to,
       do: {:ok, from, to}

  defp validate_accounts(_, _), do: {:error, :invalid_accounts}

  defp validate_key(key) when is_binary(key) and byte_size(key) > 0, do: {:ok, key}
  defp validate_key(_), do: {:error, :invalid_idempotency_key}
end

defmodule Finance.Ledger do
  @moduledoc """
  Armazenamento em memória (simulando um banco de dados) com:

    * Expiração automática: transações pendentes expiram após `ttl_ms`, e as
      chaves de idempotência expiram junto com elas. Um varredor periódico
      (`sweep_interval_ms`) limpa registros vencidos; além disso, a validade é
      verificada sob demanda em cada operação, então nunca se usa um registro vencido.
    * Histórico: todas as mudanças de estado são registradas em um log append-only.
    * Idempotência: cada requisição exige uma `idempotency_key`. Repetir a mesma
      chave enquanto o registro estiver válido retorna `{:error, {:duplicate, txn_id}}`
      e a operação não é executada novamente.

  Persistência com Ecto é opcional; veja o bloco de comentários ao final do arquivo.
  """

  use GenServer

  alias Finance.Transaction

  @default_ttl_ms :timer.minutes(15)
  @default_sweep_ms :timer.seconds(30)

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o ledger. Opções: :name, :ttl_ms, :sweep_interval_ms."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Abre uma conta com saldo inicial em centavos."
  def open_account(server, account_id, initial_balance \\ 0) do
    GenServer.call(server, {:open_account, account_id, initial_balance})
  end

  @doc "Consulta o saldo de uma conta."
  def balance(server, account_id) do
    GenServer.call(server, {:balance, account_id})
  end

  @doc """
  Cria uma transação pendente (autorização) de forma idempotente.

  `attrs` deve conter `:type` (`:deposit | :withdrawal | :transfer`), `:amount`
  e as contas envolvidas (`:from` e/ou `:to`).
  """
  def request(server, attrs, idempotency_key) do
    GenServer.call(server, {:request, attrs, idempotency_key})
  end

  @doc "Confirma (aplica) uma transação pendente e válida."
  def confirm(server, txn_id) do
    GenServer.call(server, {:confirm, txn_id})
  end

  @doc "Cancela uma transação pendente e válida."
  def cancel(server, txn_id) do
    GenServer.call(server, {:cancel, txn_id})
  end

  @doc """
  Retorna o histórico em ordem cronológica.
  Opções: `:transaction_id` para filtrar por transação.
  """
  def history(server, opts \\ []) do
    GenServer.call(server, {:history, opts})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      accounts: %{},
      transactions: %{},
      # chave_idempotência => {txn_id, expira_em}
      idempotency: %{},
      # entradas em ordem inversa (inserção O(1)); invertidas na leitura
      history: [],
      ttl_ms: Keyword.get(opts, :ttl_ms, @default_ttl_ms),
      sweep_ms: Keyword.get(opts, :sweep_interval_ms, @default_sweep_ms)
    }

    schedule_sweep(state.sweep_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:open_account, id, initial}, _from, state)
      when is_binary(id) and is_integer(initial) and initial >= 0 do
    if Map.has_key?(state.accounts, id) do
      {:reply, {:error, :account_already_exists}, state}
    else
      {:reply, :ok, %{state | accounts: Map.put(state.accounts, id, initial)}}
    end
  end

  def handle_call({:open_account, _, _}, _from, state) do
    {:reply, {:error, :invalid_account}, state}
  end

  def handle_call({:balance, id}, _from, state) do
    case Map.fetch(state.accounts, id) do
      {:ok, balance} -> {:reply, {:ok, balance}, state}
      :error -> {:reply, {:error, :unknown_account}, state}
    end
  end

  def handle_call({:request, attrs, key}, _from, state) when is_map(attrs) do
    now = now()

    case lookup_idempotency(state, key, now) do
      {:duplicate, txn_id} ->
        state = log(state, :duplicate_rejected, txn_id, %{idempotency_key: key}, now)
        {:reply, {:error, {:duplicate, txn_id}}, state}

      :new ->
        id = new_id()
        attrs = Map.put(attrs, :idempotency_key, key)

        with {:ok, txn} <- Transaction.new(attrs, id, now, state.ttl_ms),
             :ok <- check_accounts(state, txn) do
          state =
            state
            |> put_txn(txn)
            |> put_idempotency(key, id, txn.expires_at)
            |> log(:created, id, %{type: txn.type, amount: txn.amount}, now)

          {:reply, {:ok, txn}, state}
        else
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:request, _attrs, _key}, _from, state) do
    {:reply, {:error, :invalid_request}, state}
  end

  def handle_call({:confirm, id}, _from, state) do
    now = now()

    with_pending(state, id, now, fn txn, st -> settle(st, txn, now) end)
  end

  def handle_call({:cancel, id}, _from, state) do
    now = now()

    with_pending(state, id, now, fn txn, st ->
      cancelled = %{txn | status: :cancelled, settled_at: now}

      st =
        st
        |> put_txn(cancelled)
        |> log(:cancelled, id, %{}, now)

      {:reply, {:ok, cancelled}, st}
    end)
  end

  def handle_call({:history, opts}, _from, state) do
    txn_id = Keyword.get(opts, :transaction_id)

    entries =
      state.history
      |> Enum.reverse()
      |> Enum.filter(fn entry -> is_nil(txn_id) or entry.transaction_id == txn_id end)

    {:reply, entries, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = now()

    # Expira transações pendentes vencidas, registrando no histórico.
    state =
      state.transactions
      |> Map.values()
      |> Enum.filter(&Transaction.expired?(&1, now))
      |> Enum.reduce(state, fn txn, acc -> expire(acc, txn, now) end)

    # Remove chaves de idempotência vencidas.
    idempotency =
      state.idempotency
      |> Enum.filter(fn {_key, {_txn_id, expires_at}} -> expires_at > now end)
      |> Map.new()

    schedule_sweep(state.sweep_ms)
    {:noreply, %{state | idempotency: idempotency}}
  end

  # ---------------------------------------------------------------------------
  # Funções auxiliares privadas
  # ---------------------------------------------------------------------------

  # Executa `fun.(txn, state)` somente se a transação existir, estiver pendente
  # e ainda estiver válida. Caso contrário, retorna o erro apropriado.
  defp with_pending(state, id, now, fun) do
    case Map.fetch(state.transactions, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %Transaction{status: :pending} = txn} ->
        if Transaction.expired?(txn, now) do
          {:reply, {:error, :expired}, expire(state, txn, now)}
        else
          fun.(txn, state)
        end

      {:ok, %Transaction{status: status}} ->
        {:reply, {:error, {:invalid_status, status}}, state}
    end
  end

  defp settle(state, txn, now) do
    case Transaction.apply_effects(state.accounts, txn) do
      {:ok, accounts} ->
        completed = %{txn | status: :completed, settled_at: now}

        state =
          %{state | accounts: accounts}
          |> put_txn(completed)
          |> log(:completed, txn.id, %{}, now)

        {:reply, {:ok, completed}, state}

      {:error, reason} ->
        failed = %{txn | status: :failed, settled_at: now}

        state =
          state
          |> put_txn(failed)
          |> log(:failed, txn.id, %{reason: reason}, now)

        {:reply, {:error, reason}, state}
    end
  end

  defp expire(state, txn, now) do
    expired = %{txn | status: :expired, settled_at: now}

    state
    |> put_txn(expired)
    |> log(:expired, txn.id, %{}, now)
  end

  defp lookup_idempotency(state, key, now) do
    case Map.get(state.idempotency, key) do
      {txn_id, expires_at} when expires_at > now -> {:duplicate, txn_id}
      _ -> :new
    end
  end

  defp check_accounts(state, %Transaction{from: from, to: to}) do
    missing =
      [from, to]
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(&Map.has_key?(state.accounts, &1))

    if missing == [], do: :ok, else: {:error, :unknown_account}
  end

  defp put_txn(state, %Transaction{id: id} = txn) do
    %{state | transactions: Map.put(state.transactions, id, txn)}
  end

  defp put_idempotency(state, key, txn_id, expires_at) do
    %{state | idempotency: Map.put(state.idempotency, key, {txn_id, expires_at})}
  end

  defp log(state, event, txn_id, details, now) do
    entry = %{
      at: now,
      event: event,
      transaction_id: txn_id,
      details: details
    }

    %{state | history: [entry | state.history]}
  end

  defp new_id do
    "txn_" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  defp schedule_sweep(interval_ms) do
    Process.send_after(self(), :sweep, interval_ms)
  end

  defp now, do: System.system_time(:millisecond)
end

# ---------------------------------------------------------------------------
# Persistência opcional com Ecto (não obrigatória; não incluída como dependência).
#
# Se for migrar o armazenamento para um banco de dados, uma migração equivalente
# ao estado acima poderia ser:
#
#   defmodule Finance.Repo.Migrations.CreateFinanceTables do
#     use Ecto.Migration
#
#     def change do
#       create table(:accounts, primary_key: false) do
#         add :id, :string, primary_key: true
#         add :balance, :bigint, null: false, default: 0
#       end
#
#       create table(:transactions, primary_key: false) do
#         add :id, :string, primary_key: true
#         add :type, :string, null: false
#         add :from, references(:accounts, type: :string)
#         add :to, references(:accounts, type: :string)
#         add :amount, :bigint, null: false
#         add :status, :string, null: false
#         add :idempotency_key, :string, null: false
#         add :expires_at, :bigint, null: false
#         add :created_at, :bigint, null: false
#         add :settled_at, :bigint
#       end
#
#       # A restrição única garante idempotência também em nível de banco.
#       create unique_index(:transactions, [:idempotency_key])
#       create index(:transactions, [:status, :expires_at])
#
#       create table(:transaction_history) do
#         add :transaction_id, references(:transactions, type: :string), null: false
#         add :event, :string, null: false
#         add :details, :map, default: %{}
#         add :at, :bigint, null: false
#       end
#
#       create index(:transaction_history, [:transaction_id])
#     end
#   end
#
# Com Ecto, a expiração pode ser feita por um job periódico (ex.: UPDATE ...
# WHERE status = 'pending' AND expires_at <= now) e a idempotência pela
# restrição única combinada com a verificação em transação.
# ---------------------------------------------------------------------------
