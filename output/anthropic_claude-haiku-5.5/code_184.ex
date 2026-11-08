# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# notifications.exs
# Sistema de gerenciamento de notificações (somente biblioteca padrão do Elixir).
# Execute com: elixir notifications.exs

defmodule Notifications.Store do
  @moduledoc """
  Núcleo funcional puro sobre o estado do banco fictício.
  Não possui processos: recebe um estado e devolve `{:ok, resultado, novo_estado}`
  ou `{:error, motivo}`. Isso permite compor operações dentro de transações atômicas.
  """

  def new, do: %{rows: %{}, keys: %{}, next_id: 1}

  @doc "Insere uma notificação de forma idempotente, usando `idempotency_key` como chave única."
  def insert(state, %{user_id: uid, message: msg, idempotency_key: key})
      when is_binary(msg) and is_binary(key) do
    case Map.fetch(state.keys, key) do
      {:ok, id} ->
        # Já existe: não cria duplicata (evita reprocessamento em retentativas).
        {:ok, {:duplicate, Map.fetch!(state.rows, id)}, state}

      :error ->
        id = state.next_id

        row = %{
          id: id,
          user_id: uid,
          message: msg,
          status: :unread,
          version: 1,
          idempotency_key: key
        }

        new_state = %{
          state
          | rows: Map.put(state.rows, id, row),
            keys: Map.put(state.keys, key, id),
            next_id: id + 1
        }

        {:ok, {:created, row}, new_state}
    end
  end

  def insert(_state, _attrs), do: {:error, :invalid_attrs}

  def fetch(state, id) do
    case Map.fetch(state.rows, id) do
      {:ok, row} -> {:ok, row}
      :error -> {:error, :not_found}
    end
  end

  @doc """
  Atualização otimista (compare-and-swap): só aplica a mudança se a versão
  esperada ainda for a versão atual do registro. Caso contrário, `:stale_version`.
  """
  def update_if_version(state, id, expected_version, changes) do
    with {:ok, row} <- fetch(state, id) do
      if row.version == expected_version do
        row = %{row | status: Map.get(changes, :status, row.status), version: row.version + 1}
        {:ok, row, put_row(state, row)}
      else
        {:error, :stale_version}
      end
    end
  end

  def list_by_user(state, user_id) do
    state.rows
    |> Map.values()
    |> Enum.filter(&(&1.user_id == user_id))
    |> Enum.sort_by(& &1.id)
  end

  defp put_row(state, row), do: %{state | rows: Map.put(state.rows, row.id, row)}
end

defmodule Notifications.FakeDB do
  @moduledoc """
  Banco de dados fictício em um GenServer.

  Toda transação é executada dentro do processo do banco, ou seja, é serializada:
  nenhuma outra escrita se intercala no meio dela (atomicidade e isolamento).
  Se a função lançar exceção ou retornar erro, o estado anterior é preservado (rollback).

  Equivalente opcional com Ecto (não obrigatório):

      defmodule Notifications.Repo.Migrations.CreateNotifications do
        use Ecto.Migration

        def change do
          create table(:notifications) do
            add :user_id, :integer, null: false
            add :message, :string, null: false
            add :status, :string, null: false, default: "unread"
            add :version, :integer, null: false, default: 1
            add :idempotency_key, :string, null: false
            timestamps()
          end

          create unique_index(:notifications, [:idempotency_key])
        end
      end
  """
  use GenServer

  alias Notifications.Store

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, :ok, opts)

  @doc """
  Executa `fun.(state)` atomicamente. A função deve retornar
  `{:ok, resultado, novo_estado}` ou `{:error, motivo}`.
  """
  def transaction(db, fun) when is_function(fun, 1), do: GenServer.call(db, {:tx, fun})

  @impl true
  def init(:ok), do: {:ok, Store.new()}

  @impl true
  def handle_call({:tx, fun}, _from, state) do
    case safe_run(fun, state) do
      {:ok, result, new_state} -> {:reply, {:ok, result}, new_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp safe_run(fun, state) do
    case fun.(state) do
      {:ok, _, _} = ok -> ok
      {:error, _} = err -> err
      other -> {:error, {:bad_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end
end

defmodule Notifications.Manager do
  @moduledoc """
  Serviço de notificações.

  - Comunicação entre processos: assinantes recebem `{:notification, row}` por
    troca de mensagens. Assinaturas são monitoradas e removidas automaticamente
    quando o processo assinante termina.
  - Consistência: criações e atualizações passam por transações do FakeDB;
    a publicação só ocorre após o commit da escrita.
  """
  use GenServer

  alias Notifications.{FakeDB, Store}

  def start_link(db, opts \\ []), do: GenServer.start_link(__MODULE__, db, opts)

  def create(mgr, user_id, message, key),
    do: GenServer.call(mgr, {:create, user_id, message, key})

  def mark_read(mgr, id), do: GenServer.call(mgr, {:mark_read, id})

  def update_status(mgr, id, expected_version, status),
    do: GenServer.call(mgr, {:update_status, id, expected_version, status})

  def list(mgr, user_id), do: GenServer.call(mgr, {:list, user_id})

  def subscribe(mgr, user_id, pid \\ self()),
    do: GenServer.call(mgr, {:subscribe, user_id, pid})

  def unsubscribe(mgr, user_id, pid \\ self()),
    do: GenServer.call(mgr, {:unsubscribe, user_id, pid})

  @impl true
  def init(db), do: {:ok, %{db: db, subs: %{}, monitors: %{}}}

  @impl true
  def handle_call({:create, uid, msg, key}, _from, s) do
    attrs = %{user_id: uid, message: msg, idempotency_key: key}

    case FakeDB.transaction(s.db, &Store.insert(&1, attrs)) do
      {:ok, {:created, row}} ->
        # Publica somente após o commit e somente para criações reais.
        publish(s, uid, row)
        {:reply, {:ok, row}, s}

      {:ok, {:duplicate, row}} ->
        {:reply, {:ok, row}, s}

      {:error, _} = err ->
        {:reply, err, s}
    end
  end

  def handle_call({:mark_read, id}, _from, s) do
    # Leitura e escrita ocorrem na mesma transação: sem lost update.
    tx = fn state ->
      with {:ok, row} <- Store.fetch(state, id) do
        if row.status == :read do
          {:ok, row, state}
        else
          Store.update_if_version(state, id, row.version, %{status: :read})
        end
      end
    end

    {:reply, FakeDB.transaction(s.db, tx), s}
  end

  def handle_call({:update_status, id, version, status}, _from, s) do
    tx = &Store.update_if_version(&1, id, version, %{status: status})
    {:reply, FakeDB.transaction(s.db, tx), s}
  end

  def handle_call({:list, uid}, _from, s) do
    tx = &{:ok, Store.list_by_user(&1, uid), &1}
    {:reply, FakeDB.transaction(s.db, tx), s}
  end

  def handle_call({:subscribe, uid, pid}, _from, s) do
    users = Map.get(s.subs, uid, %{})

    if Map.has_key?(users, pid) do
      {:reply, :ok, s}
    else
      ref = Process.monitor(pid)

      s = %{
        s
        | subs: Map.put(s.subs, uid, Map.put(users, pid, ref)),
          monitors: Map.put(s.monitors, ref, {uid, pid})
      }

      {:reply, :ok, s}
    end
  end

  def handle_call({:unsubscribe, uid, pid}, _from, s) do
    case get_in(s.subs, [uid, pid]) do
      nil ->
        {:reply, :ok, s}

      ref ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, drop(s, ref, uid, pid)}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, s) do
    case Map.fetch(s.monitors, ref) do
      {:ok, {uid, pid}} -> {:noreply, drop(s, ref, uid, pid)}
      :error -> {:noreply, s}
    end
  end

  defp publish(s, uid, row) do
    for {pid, _ref} <- Map.get(s.subs, uid, %{}), do: send(pid, {:notification, row})
    :ok
  end

  defp drop(s, ref, uid, pid) do
    users = Map.delete(Map.get(s.subs, uid, %{}), pid)
    subs = if users == %{}, do: Map.delete(s.subs, uid), else: Map.put(s.subs, uid, users)
    %{s | subs: subs, monitors: Map.delete(s.monitors, ref)}
  end
end

defmodule Notifications.Demo do
  @moduledoc "Demonstração de concorrência, idempotência, pub/sub e controle de versão."

  alias Notifications.{FakeDB, Manager}

  def run do
    {:ok, db} = FakeDB.start_link()
    {:ok, mgr} = Manager.start_link(db)
    :ok = Manager.subscribe(mgr, 42)

    # 100 criações concorrentes; as de índice múltiplo de 10 usam a mesma chave.
    created_ids =
      1..100
      |> Task.async_stream(
        fn i ->
          key = if rem(i, 10) == 0, do: "dup", else: "k-#{i}"
          {:ok, row} = Manager.create(mgr, 42, "mensagem #{i}", key)
          row.id
        end,
        max_concurrency: 20
      )
      |> Enum.map(fn {:ok, id} -> id end)
      |> Enum.uniq()

    # Esperado: 91 (90 chaves únicas + 1 chave duplicada reaproveitada)
    IO.puts("Notificações distintas criadas: #{length(created_ids)}")

    # Esperado: 91 (somente criações reais são publicadas)
    IO.puts("Mensagens recebidas pelo assinante: #{count_received(0)}")

    # Marcações concorrentes como lidas
    results =
      created_ids
      |> Task.async_stream(&Manager.mark_read(mgr, &1), max_concurrency: 20)
      |> Enum.map(fn {:ok, r} -> r end)

    IO.puts("Marcações sem erro: #{Enum.count(results, &match?({:ok, _}, &1))}")

    {:ok, rows} = Manager.list(mgr, 42)
    # Esperado: 0
    IO.puts("Não lidas após marcações concorrentes: #{Enum.count(rows, &(&1.status == :unread))}")

    # Controle otimista de versão
    [first | _] = rows
    IO.inspect(Manager.update_status(mgr, first.id, first.version, :unread), label: "CAS com versão atual")
    IO.inspect(Manager.update_status(mgr, first.id, first.version, :unread), label: "CAS com versão antiga")

    :ok = Manager.unsubscribe(mgr, 42)
  end

  defp count_received(n) do
    receive do
      {:notification, _row} -> count_received(n + 1)
    after
      50 -> n
    end
  end
end

Notifications.Demo.run()
