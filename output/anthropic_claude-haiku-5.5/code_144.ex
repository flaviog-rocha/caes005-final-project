# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de transações financeiras em Elixir (biblioteca padrão, arquivo único).
#
# Uso básico:
#   {:ok, _sup} = Finance.start_link(max_concurrency: 4, process_interval_ms: 2_000)
#   {:ok, tx} = Finance.create_transaction(%{type: :deposit, to: "conta-1", amount_cents: 10_000})
#   {:ok, tx} = Finance.update_transaction(tx.id, %{description: "Depósito inicial"})
#   Finance.list_transactions(status: :pending)
#   :ok = Finance.delete_transaction(tx.id)

# ---------------------------------------------------------------------------
# Domínio: estrutura e validações puras
# ---------------------------------------------------------------------------
defmodule Finance.Transaction do
  @moduledoc """
  Estrutura de transação financeira com validações puras (sem efeitos colaterais).
  Valores monetários são armazenados em centavos (inteiros) para evitar erros de ponto flutuante.
  """

  @types [:deposit, :withdrawal, :transfer]
  @statuses [:pending, :completed, :failed, :cancelled]
  @updatable [:description, :amount_cents, :status]

  defstruct [
    :id,
    :type,
    :from,
    :to,
    :amount_cents,
    :description,
    status: :pending,
    inserted_at: nil,
    updated_at: nil
  ]

  @type t :: %__MODULE__{
          id: pos_integer() | nil,
          type: :deposit | :withdrawal | :transfer | nil,
          from: String.t() | nil,
          to: String.t() | nil,
          amount_cents: pos_integer() | nil,
          description: String.t() | nil,
          status: :pending | :completed | :failed | :cancelled,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  @doc "Constrói e valida uma nova transação a partir de um mapa de atributos."
  @spec build(map()) :: {:ok, t()} | {:error, term()}
  def build(attrs) when is_map(attrs) do
    now = DateTime.utc_now()

    %__MODULE__{
      # Id gerado localmente; em um banco real seria responsabilidade do storage.
      id: :erlang.unique_integer([:positive, :monotonic]),
      type: Map.get(attrs, :type),
      from: Map.get(attrs, :from),
      to: Map.get(attrs, :to),
      amount_cents: Map.get(attrs, :amount_cents),
      description: Map.get(attrs, :description),
      status: Map.get(attrs, :status, :pending),
      inserted_at: now,
      updated_at: now
    }
    |> validate()
  end

  @doc """
  Aplica atualizações parciais. Transações concluídas são imutáveis.
  Retorna uma nova struct (não há mutação).
  """
  @spec update(t(), map()) :: {:ok, t()} | {:error, term()}
  def update(%__MODULE__{status: :completed}, _attrs), do: {:error, :immutable}

  def update(%__MODULE__{} = tx, attrs) when is_map(attrs) do
    tx
    |> Map.merge(Map.take(attrs, @updatable))
    |> Map.put(:updated_at, DateTime.utc_now())
    |> validate()
  end

  # Pipeline de validações encadeadas com `with`
  defp validate(%__MODULE__{} = tx) do
    with :ok <- check(tx.type in @types, {:invalid, :type}),
         :ok <- check(is_integer(tx.amount_cents) and tx.amount_cents > 0, {:invalid, :amount_cents}),
         :ok <- check(tx.status in @statuses, {:invalid, :status}),
         :ok <- check(accounts_valid?(tx), {:invalid, :accounts}) do
      {:ok, tx}
    end
  end

  defp check(true, _error), do: :ok
  defp check(false, error), do: {:error, error}

  # Regras de contas por tipo de operação
  defp accounts_valid?(%{type: :deposit, to: to}), do: not is_nil(to)
  defp accounts_valid?(%{type: :withdrawal, from: from}), do: not is_nil(from)

  defp accounts_valid?(%{type: :transfer, from: from, to: to}) do
    not is_nil(from) and not is_nil(to) and from != to
  end

  defp accounts_valid?(_), do: false
end

# ---------------------------------------------------------------------------
# Contrato de persistência (permite trocar o banco sem alterar o domínio)
# ---------------------------------------------------------------------------
defmodule Finance.Storage do
  @moduledoc "Behaviour para adaptadores de persistência (banco fictício, Ecto, etc.)."

  alias Finance.Transaction

  @callback insert(Transaction.t()) :: {:ok, Transaction.t()} | {:error, term()}
  @callback get(id :: pos_integer()) :: {:ok, Transaction.t()} | {:error, :not_found}
  @callback all() :: [Transaction.t()]
  @callback update(Transaction.t()) :: {:ok, Transaction.t()} | {:error, :not_found}
  @callback delete(id :: pos_integer()) :: :ok | {:error, :not_found}
end

# ---------------------------------------------------------------------------
# Banco fictício em memória
# ---------------------------------------------------------------------------
defmodule Finance.Storage.Memory do
  @moduledoc "Implementação fictícia de banco de dados usando um GenServer com mapa."

  use GenServer
  @behaviour Finance.Storage

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl Finance.Storage
  def insert(tx), do: GenServer.call(__MODULE__, {:insert, tx})

  @impl Finance.Storage
  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  @impl Finance.Storage
  def all, do: GenServer.call(__MODULE__, :all)

  @impl Finance.Storage
  def update(tx), do: GenServer.call(__MODULE__, {:update, tx})

  @impl Finance.Storage
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl GenServer
  def handle_call({:insert, tx}, _from, state) do
    if Map.has_key?(state, tx.id) do
      {:reply, {:error, :already_exists}, state}
    else
      {:reply, {:ok, tx}, Map.put(state, tx.id, tx)}
    end
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch(state, id), state}
  end

  def handle_call(:all, _from, state) do
    {:reply, state |> Map.values() |> Enum.sort_by(& &1.id), state}
  end

  def handle_call({:update, tx}, _from, state) do
    if Map.has_key?(state, tx.id) do
      {:reply, {:ok, tx}, Map.put(state, tx.id, tx)}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.pop(state, id) do
      {nil, state} -> {:reply, {:error, :not_found}, state}
      {_tx, new_state} -> {:reply, :ok, new_state}
    end
  end

  defp fetch(state, id) do
    case Map.fetch(state, id) do
      {:ok, tx} -> {:ok, tx}
      :error -> {:error, :not_found}
    end
  end
end

# ---------------------------------------------------------------------------
# Limitador de operações simultâneas (semáforo com fila de espera)
# ---------------------------------------------------------------------------
defmodule Finance.Limiter do
  @moduledoc """
  Limita o número de operações executadas simultaneamente.
  Chamadas excedentes ficam em fila (FIFO) até que haja uma vaga.
  Processos que morrem enquanto ocupam uma vaga liberam-na automaticamente.
  """

  use GenServer

  def start_link(opts) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc "Executa `fun` assim que houver uma vaga disponível e libera a vaga ao final."
  @spec run((-> result), timeout()) :: result when result: term()
  def run(fun, timeout \\ :infinity) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :acquire, timeout)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, self()})
    end
  end

  @impl GenServer
  def init(max), do: {:ok, %{max: max, holders: %{}, waiting: :queue.new()}}

  @impl GenServer
  def handle_call(:acquire, {pid, _tag} = from, state) do
    if map_size(state.holders) < state.max do
      {:reply, :ok, grant(state, pid)}
    else
      {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
    end
  end

  @impl GenServer
  def handle_cast({:release, pid}, state) do
    case Enum.find(state.holders, fn {_ref, holder} -> holder == pid end) do
      {ref, _pid} ->
        Process.demonitor(ref, [:flush])
        {:noreply, state |> drop(ref) |> next()}

      nil ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    if Map.has_key?(state.holders, ref) do
      {:noreply, state |> drop(ref) |> next()}
    else
      {:noreply, state}
    end
  end

  defp grant(state, pid) do
    ref = Process.monitor(pid)
    %{state | holders: Map.put(state.holders, ref, pid)}
  end

  defp drop(state, ref), do: %{state | holders: Map.delete(state.holders, ref)}

  # Concede vagas livres aos próximos da fila
  defp next(state) do
    with true <- map_size(state.holders) < state.max,
         {{:value, {pid, _tag} = from}, rest} <- :queue.out(state.waiting) do
      GenServer.reply(from, :ok)
      next(grant(%{state | waiting: rest}, pid))
    else
      _ -> state
    end
  end
end

# ---------------------------------------------------------------------------
# Agendador de tarefas periódicas
# ---------------------------------------------------------------------------
defmodule Finance.Scheduler do
  @moduledoc """
  Executa funções periodicamente. Cada job é `{nome, intervalo_ms, fun}`.
  Um job não é iniciado novamente enquanto a execução anterior ainda estiver ativa.
  """

  use GenServer

  def start_link(jobs), do: GenServer.start_link(__MODULE__, jobs, name: __MODULE__)

  @impl GenServer
  def init(jobs) do
    state =
      Map.new(jobs, fn {name, interval, fun} ->
        schedule(name, interval)
        {name, %{interval: interval, fun: fun, running: nil}}
      end)

    {:ok, state}
  end

  @impl GenServer
  def handle_info({:tick, name}, state) do
    job = Map.fetch!(state, name)
    schedule(name, job.interval)

    if job.running do
      {:noreply, state}
    else
      {_pid, ref} = spawn_monitor(job.fun)
      {:noreply, Map.put(state, name, %{job | running: ref})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    state =
      Map.new(state, fn {name, job} ->
        if job.running == ref, do: {name, %{job | running: nil}}, else: {name, job}
      end)

    {:noreply, state}
  end

  defp schedule(name, interval), do: Process.send_after(self(), {:tick, name}, interval)
end

# ---------------------------------------------------------------------------
# Fachada pública: supervisão, CRUD e processamento periódico
# ---------------------------------------------------------------------------
defmodule Finance do
  @moduledoc """
  API pública do gerenciamento de transações.

  Opções de `start_link/1`:
    * `:max_concurrency` - máximo de operações simultâneas (padrão: 4)
    * `:process_interval_ms` - intervalo do job que liquida transações pendentes (padrão: 5000)
    * `:storage` (via `Application.put_env(:finance, :storage, mod)`) - adaptador de persistência
  """

  alias Finance.{Limiter, Scheduler, Transaction}

  # -- Supervisão -------------------------------------------------------------

  @doc "Inicia a árvore de supervisão (armazenamento, limitador e agendador)."
  def start_link(opts \\ []) do
    children = [
      {Finance.Storage.Memory, []},
      {Limiter, max_concurrency: Keyword.get(opts, :max_concurrency, 4)},
      {Scheduler, jobs(opts)}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Finance.Supervisor)
  end

  defp jobs(opts) do
    interval = Keyword.get(opts, :process_interval_ms, 5_000)
    [{:process_pending, interval, &__MODULE__.process_pending/0}]
  end

  defp storage, do: Application.get_env(:finance, :storage, Finance.Storage.Memory)

  # -- CRUD -------------------------------------------------------------------

  @doc "Cadastra uma nova transação."
  @spec create_transaction(map()) :: {:ok, Transaction.t()} | {:error, term()}
  def create_transaction(attrs) do
    with {:ok, tx} <- Transaction.build(attrs) do
      storage().insert(tx)
    end
  end

  @doc "Consulta uma transação pelo id."
  @spec get_transaction(pos_integer()) :: {:ok, Transaction.t()} | {:error, :not_found}
  def get_transaction(id), do: storage().get(id)

  @doc "Lista transações, opcionalmente filtradas por campos (ex.: `status: :pending`)."
  @spec list_transactions(keyword()) :: [Transaction.t()]
  def list_transactions(filters \\ []) do
    storage().all()
    |> Enum.filter(fn tx -> Enum.all?(filters, fn {k, v} -> Map.get(tx, k) == v end) end)
  end

  @doc "Atualiza parcialmente uma transação existente."
  @spec update_transaction(pos_integer(), map()) :: {:ok, Transaction.t()} | {:error, term()}
  def update_transaction(id, attrs) do
    with {:ok, tx} <- storage().get(id),
         {:ok, updated} <- Transaction.update(tx, attrs) do
      storage().update(updated)
    end
  end

  @doc "Remove uma transação."
  @spec delete_transaction(pos_integer()) :: :ok | {:error, :not_found}
  def delete_transaction(id), do: storage().delete(id)

  # -- Processamento periódico -----------------------------------------------

  @doc """
  Liquida todas as transações pendentes.
  Cada liquidação passa pelo `Limiter`, respeitando o limite de concorrência.
  """
  @spec process_pending() :: :ok
  def process_pending do
    list_transactions(status: :pending)
    |> Task.async_stream(
      fn tx -> Limiter.run(fn -> settle(tx) end) end,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  # Simulação de liquidação: aqui entraria a lógica real (checagem de saldo, integração etc.)
  defp settle(%Transaction{id: id}) do
    case update_transaction(id, %{status: :completed}) do
      {:ok, _tx} -> :ok
      {:error, _reason} -> :skipped
    end
  end
end

# ---------------------------------------------------------------------------
# Migration Ecto opcional (compilada somente se Ecto estiver disponível)
# ---------------------------------------------------------------------------
if Code.ensure_loaded?(Ecto.Migration) do
  defmodule Finance.Repo.Migrations.CreateTransactions do
    @moduledoc "Esquema relacional sugerido para um adaptador Ecto de `Finance.Storage`."
    use Ecto.Migration

    def change do
      create table(:transactions) do
        add :type, :string, null: false
        add :from, :string
        add :to, :string
        add :amount_cents, :integer, null: false
        add :description, :string
        add :status, :string, null: false, default: "pending"
        timestamps()
      end

      create index(:transactions, [:status])
    end
  end
end
