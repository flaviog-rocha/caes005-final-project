# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciador de transações financeiras em um único arquivo.
#
# Requisitos atendidos:
#   1. Persistência em um banco de dados fictício (Store, baseado em ETS dentro de
#      um GenServer). A camada pode ser trocada por um Ecto.Repo sem alterar o domínio.
#      Exemplo de migration opcional (requer ecto_sql):
#
#        defmodule FinanceManager.Migrations.CreateTransactions do
#          use Ecto.Migration
#
#          def change do
#            create table(:transactions) do
#              add :type, :string, null: false
#              add :amount, :integer, null: false
#              add :description, :string, null: false
#              add :status, :string, null: false, default: "pending"
#              timestamps()
#            end
#          end
#        end
#
#   2. Limite de operações simultâneas (Limiter, um semáforo com fila de espera e
#      monitoramento dos processos que detêm permissões).
#   3. Operações periódicas (Scheduler, que liquida transações pendentes e calcula
#      o saldo a cada intervalo configurado).
#   4. CRUD de transações (Transactions.create/1, get/1, list/0, update/2, delete/1).
#
# Uso:
#
#   {:ok, _pid} = FinanceManager.start_link(max_concurrency: 4, settle_interval_ms: 5_000)
#   {:ok, tx} = FinanceManager.Transactions.create(%{type: :income, amount: 10_000, description: "Salário"})
#   {:ok, _} = FinanceManager.Transactions.update(tx.id, %{description: "Salário de janeiro"})
#   {:ok, _} = FinanceManager.Transactions.get(tx.id)
#   FinanceManager.Transactions.list()
#   FinanceManager.Transactions.balance()
#   FinanceManager.Scheduler.last_report()
#   FinanceManager.Transactions.delete(tx.id)
#
# Valores monetários são armazenados como inteiros em centavos.

defmodule FinanceManager do
  @moduledoc """
  Supervisor principal do sistema de gerenciamento de transações financeiras.

  Opções:
    * `:max_concurrency` - número máximo de operações simultâneas sobre o banco (padrão: 4)
    * `:settle_interval_ms` - intervalo da tarefa periódica em milissegundos (padrão: 5000)
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {FinanceManager.Store, []},
      {FinanceManager.Limiter, Keyword.get(opts, :max_concurrency, 4)},
      {FinanceManager.Scheduler, Keyword.get(opts, :settle_interval_ms, 5_000)}
    ]

    # A ordem importa: o Store e o Limiter precisam existir antes do Scheduler.
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule FinanceManager.Limiter do
  @moduledoc """
  Semáforo de contagem: garante no máximo `max` operações executando ao mesmo tempo.
  Chamadas excedentes ficam em uma fila FIFO até que uma permissão seja liberada.
  Permissões de processos que morrem são recuperadas via monitor.
  """
  use GenServer

  @doc "Executa `fun` respeitando o limite de concorrência."
  def run(fun) when is_function(fun, 0) do
    {:ok, ref} = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      GenServer.call(__MODULE__, {:release, ref})
    end
  end

  def start_link(max) when is_integer(max) and max > 0 do
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    if map_size(state.active) < state.max do
      ref = Process.monitor(pid)
      {:reply, {:ok, ref}, put_active(state, ref, pid)}
    else
      {:noreply, %{state | waiting: :queue.in({from, pid}, state.waiting)}}
    end
  end

  def handle_call({:release, ref}, _from, state) do
    {:reply, :ok, state |> free(ref) |> drain()}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, state |> free(ref) |> drain()}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp put_active(state, ref, pid) do
    %{state | active: Map.put(state.active, ref, pid)}
  end

  defp free(state, ref) do
    Process.demonitor(ref, [:flush])
    %{state | active: Map.delete(state.active, ref)}
  end

  # Concede permissões aos próximos da fila, ignorando processos que já morreram.
  defp drain(state) do
    with true <- map_size(state.active) < state.max,
         {{:value, {from, pid}}, rest} <- :queue.out(state.waiting) do
      state = %{state | waiting: rest}

      if Process.alive?(pid) do
        ref = Process.monitor(pid)
        GenServer.reply(from, {:ok, ref})
        state |> put_active(ref, pid) |> drain()
      else
        drain(state)
      end
    else
      _ -> state
    end
  end
end

defmodule FinanceManager.Store do
  @moduledoc """
  Banco de dados fictício em memória (ETS privada, dentro de um GenServer).
  Armazena registros genéricos indexados por {tabela, id}.

  Pode ser substituído por um Ecto.Repo mantendo a mesma interface pública.
  """
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def insert(table, attrs), do: call({:insert, table, attrs})
  def fetch(table, id), do: call({:fetch, table, id})
  def all(table), do: call({:all, table})
  def update(table, id, changes), do: call({:update, table, id, changes})
  def delete(table, id), do: call({:delete, table, id})

  defp call(msg), do: GenServer.call(__MODULE__, msg)

  @impl true
  def init(_) do
    tab = :ets.new(:fake_db, [:set, :private])
    {:ok, %{tab: tab, next_id: 1}}
  end

  @impl true
  def handle_call({:insert, table, attrs}, _from, %{tab: tab, next_id: id} = state) do
    record = Map.put(attrs, :id, id)
    :ets.insert(tab, {{table, id}, record})
    {:reply, {:ok, record}, %{state | next_id: id + 1}}
  end

  def handle_call({:fetch, table, id}, _from, %{tab: tab} = state) do
    {:reply, lookup(tab, table, id), state}
  end

  def handle_call({:all, table}, _from, %{tab: tab} = state) do
    records =
      tab
      |> :ets.match_object({{table, :_}, :_})
      |> Enum.map(fn {_key, record} -> record end)
      |> Enum.sort_by(& &1.id)

    {:reply, records, state}
  end

  def handle_call({:update, table, id, changes}, _from, %{tab: tab} = state) do
    reply =
      case lookup(tab, table, id) do
        {:ok, record} ->
          updated = Map.merge(record, Map.delete(changes, :id))
          :ets.insert(tab, {{table, id}, updated})
          {:ok, updated}

        error ->
          error
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, table, id}, _from, %{tab: tab} = state) do
    reply =
      case lookup(tab, table, id) do
        {:ok, record} ->
          :ets.delete(tab, {table, id})
          {:ok, record}

        error ->
          error
      end

    {:reply, reply, state}
  end

  defp lookup(tab, table, id) do
    case :ets.lookup(tab, {table, id}) do
      [{_key, record}] -> {:ok, record}
      [] -> {:error, :not_found}
    end
  end
end

defmodule FinanceManager.Transactions do
  @moduledoc """
  API de domínio para transações financeiras.

  Cada operação sobre o banco passa pelo `Limiter`, limitando a concorrência.
  Os campos são validados antes de qualquer escrita.

  Campos:
    * `:type` - `:income` ou `:expense`
    * `:amount` - inteiro positivo, em centavos
    * `:description` - string não vazia
    * `:status` - `:pending` ou `:settled`
    * `:created_at` - `DateTime` de criação (definido automaticamente)
  """
  alias FinanceManager.{Limiter, Store}

  @table :transactions
  @required [:type, :amount, :description]
  @types [:income, :expense]
  @statuses [:pending, :settled]

  @doc "Cadastra uma nova transação com status `:pending`."
  def create(attrs) when is_map(attrs) do
    case Enum.find(@required, &(not Map.has_key?(attrs, &1))) do
      nil ->
        with {:ok, data} <- validate_changes(Map.take(attrs, @required)) do
          record = Map.merge(data, %{status: :pending, created_at: DateTime.utc_now()})
          Limiter.run(fn -> Store.insert(@table, record) end)
        end

      field ->
        {:error, {:missing_field, field}}
    end
  end

  @doc "Consulta uma transação pelo id."
  def get(id), do: Limiter.run(fn -> Store.fetch(@table, id) end)

  @doc "Lista todas as transações ordenadas pelo id."
  def list, do: Limiter.run(fn -> Store.all(@table) end)

  @doc "Atualiza campos de uma transação existente."
  def update(id, changes) when is_map(changes) do
    with {:ok, clean} <- validate_changes(changes) do
      Limiter.run(fn -> Store.update(@table, id, clean) end)
    end
  end

  @doc "Remove uma transação pelo id."
  def delete(id), do: Limiter.run(fn -> Store.delete(@table, id) end)

  @doc "Saldo em centavos considerando apenas transações liquidadas."
  def balance do
    list()
    |> Enum.filter(&(&1.status == :settled))
    |> Enum.reduce(0, fn
      %{type: :income, amount: amount}, acc -> acc + amount
      %{type: :expense, amount: amount}, acc -> acc - amount
    end)
  end

  defp validate_changes(changes) do
    Enum.reduce_while(changes, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case validate_field(key, value) do
        {:ok, valid} -> {:cont, {:ok, Map.put(acc, key, valid)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_field(:type, value) when value in @types, do: {:ok, value}
  defp validate_field(:status, value) when value in @statuses, do: {:ok, value}
  defp validate_field(:amount, value) when is_integer(value) and value > 0, do: {:ok, value}
  defp validate_field(:description, value) when is_binary(value) and value != "", do: {:ok, value}
  defp validate_field(field, value), do: {:error, {:invalid_field, field, value}}
end

defmodule FinanceManager.Scheduler do
  @moduledoc """
  Executa periodicamente a liquidação de transações pendentes e calcula um
  relatório com a quantidade liquidada e o saldo atual.

  A liquidação é feita em paralelo via `Task.async_stream`; a concorrência real
  sobre o banco é controlada pelo `Limiter`.
  """
  use GenServer

  alias FinanceManager.Transactions

  @max_task_concurrency 16

  def start_link(interval_ms) when is_integer(interval_ms) and interval_ms > 0 do
    GenServer.start_link(__MODULE__, interval_ms, name: __MODULE__)
  end

  @doc "Executa a tarefa periódica imediatamente e retorna o relatório."
  def run_now, do: GenServer.call(__MODULE__, :run_now, :infinity)

  @doc "Retorna o último relatório gerado (ou `nil`)."
  def last_report, do: GenServer.call(__MODULE__, :last_report)

  @impl true
  def init(interval_ms) do
    schedule(interval_ms)
    {:ok, %{interval: interval_ms, report: nil}}
  end

  @impl true
  def handle_info(:tick, state) do
    report = execute_job()
    schedule(state.interval)
    {:noreply, %{state | report: report}}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    report = execute_job()
    {:reply, report, %{state | report: report}}
  end

  def handle_call(:last_report, _from, state) do
    {:reply, state.report, state}
  end

  defp schedule(interval_ms) do
    Process.send_after(self(), :tick, interval_ms)
  end

  defp execute_job do
    settled_count =
      Transactions.list()
      |> Enum.filter(&(&1.status == :pending))
      |> Task.async_stream(
        &Transactions.update(&1.id, %{status: :settled}),
        max_concurrency: @max_task_concurrency,
        timeout: :infinity
      )
      |> Enum.count(fn
        {:ok, {:ok, _}} -> true
        _ -> false
      end)

    %{
      settled: settled_count,
      balance: Transactions.balance(),
      at: DateTime.utc_now()
    }
  end
end
