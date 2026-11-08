# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações
# Requisitos atendidos:
#   1. Comunicação entre processos: Registry (pub/sub) + envio de mensagens.
#   2. Consistência em concorrência: escritas serializadas no GenServer do
#      FakeDB, controle de concorrência otimista (:version) e idempotência
#      (idempotency_key único).
#   3. Banco fictício com equivalente de migration descrito em comentário (opcional).
#
# Execução: `elixir notificacoes.exs`

defmodule NotificationSystem do
  @moduledoc """
  Sistema de notificações com:

    * Comunicação entre processos via `Registry` (pub/sub) e envio de mensagens.
    * Consistência durante concorrência: todas as escritas passam pelo processo
      do `FakeDB`, que as serializa; atualizações usam controle otimista por
      `:version`; criações são idempotentes por `:idempotency_key`.
  """

  @registry __MODULE__.Registry
  @task_sup __MODULE__.TaskSup

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc "Inicia a árvore de supervisão do sistema."
  def start do
    children = [
      {Registry, keys: :duplicate, name: @registry},
      {Task.Supervisor, name: @task_sup},
      NotificationSystem.FakeDB
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__.Supervisor)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Registra o processo chamador para receber notificações do usuário."
  def subscribe(user_id), do: Registry.register(@registry, {:user, user_id}, nil)

  @doc """
  Cria uma notificação de forma idempotente. Chamadas repetidas com a mesma
  `idempotency_key` retornam a notificação já existente, sem duplicar dados.
  """
  def create(user_id, message, idempotency_key) do
    attrs = %{user_id: user_id, message: message, idempotency_key: idempotency_key}

    case FakeDB.insert(attrs) do
      {:created, row} ->
        publish({:user, user_id}, {:notification_created, row})
        {:ok, row}

      {:duplicate, row} ->
        {:ok, row}
    end
  end

  @doc """
  Marca a notificação como lida usando controle de concorrência otimista.
  Em caso de conflito de versão, relê o registro e tenta novamente.
  """
  def mark_read(id, attempts \\ 5)
  def mark_read(_id, 0), do: {:error, :too_many_conflicts}

  def mark_read(id, attempts) do
    with {:ok, row} <- FakeDB.get(id) do
      if row.status == :read do
        {:ok, row}
      else
        case FakeDB.update(id, row.version, %{status: :read}) do
          {:ok, updated} ->
            publish({:user, updated.user_id}, {:notification_read, updated})
            {:ok, updated}

          {:error, :stale_version} ->
            mark_read(id, attempts - 1)

          error ->
            error
        end
      end
    end
  end

  @doc "Lista as notificações de um usuário."
  def list(user_id), do: FakeDB.list_by_user(user_id)

  # ---------------------------------------------------------------------------
  # Comunicação assíncrona entre processos
  # ---------------------------------------------------------------------------

  # Entrega a mensagem a todos os processos inscritos no tópico, em uma task
  # supervisionada, para não bloquear o chamador.
  defp publish(topic, message) do
    Task.Supervisor.start_child(@task_sup, fn ->
      Registry.dispatch(@registry, topic, fn entries ->
        for {pid, _} <- entries, do: send(pid, message)
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # Demonstração
  # ---------------------------------------------------------------------------

  @doc "Executa uma demonstração de concorrência, idempotência e pub/sub."
  def demo do
    {:ok, _sup} = start()
    parent = self()

    listener =
      spawn(fn ->
        {:ok, _} = subscribe(1)
        send(parent, :subscribed)
        forward(parent)
      end)

    receive do
      :subscribed -> :ok
    end

    # 1) Idempotência sob concorrência: 20 criações com a mesma chave.
    created =
      1..20
      |> Task.async_stream(fn _ -> create(1, "Pedido 42 enviado", "order-42") end,
        max_concurrency: 20
      )
      |> Enum.map(fn {:ok, res} -> res end)

    {:ok, row} = hd(created)

    ids = created |> Enum.map(fn {:ok, r} -> r.id end) |> Enum.uniq()
    IO.inspect(ids, label: "IDs distintos criados (esperado: 1 ID)")

    # 2) Concorrência otimista: 10 marcações de leitura simultâneas.
    results =
      1..10
      |> Task.async_stream(fn _ -> mark_read(row.id) end, max_concurrency: 10)
      |> Enum.map(fn {:ok, r} -> elem(r, 0) end)
      |> Enum.frequencies()

    IO.inspect(results, label: "Resultados de mark_read")
    IO.inspect(list(1), label: "Estado final no banco")

    # 3) Mensagens recebidas pelo processo inscrito.
    received = collect([])
    IO.inspect(Enum.map(received, fn {tag, _} -> tag end), label: "Eventos recebidos")

    Process.exit(listener, :kill)
    :ok
  end

  defp forward(parent) do
    receive do
      msg ->
        send(parent, {:seen, msg})
        forward(parent)
    end
  end

  defp collect(acc) do
    receive do
      {:seen, msg} -> collect([msg | acc])
    after
      300 -> Enum.reverse(acc)
    end
  end
end

defmodule NotificationSystem.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória.

  Todas as operações passam por este GenServer, o que serializa as escritas
  e garante consistência. Cada registro tem `:version` para controle de
  concorrência otimista, e `:idempotency_key` é único.

  Equivalente conceitual a uma migration Ecto (opcional, não utilizado em tempo de execução):

      create table(:notifications) do
        add :user_id, :integer, null: false
        add :message, :string, null: false
        add :status, :string, null: false, default: "unread"
        add :version, :integer, null: false, default: 1
        add :idempotency_key, :string, null: false
        timestamps()
      end

      create unique_index(:notifications, [:idempotency_key])
  """
  use GenServer

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def insert(attrs), do: GenServer.call(__MODULE__, {:insert, attrs})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list_by_user(user_id), do: GenServer.call(__MODULE__, {:list_by_user, user_id})

  def update(id, expected_version, changes),
    do: GenServer.call(__MODULE__, {:update, id, expected_version, changes})

  @impl true
  def init(_) do
    {:ok, %{rows: %{}, keys: %{}, next_id: 1}}
  end

  @impl true
  def handle_call({:insert, attrs}, _from, state) do
    case Map.fetch(state.keys, attrs.idempotency_key) do
      {:ok, id} ->
        {:reply, {:duplicate, state.rows[id]}, state}

      :error ->
        id = state.next_id

        row =
          Map.merge(attrs, %{
            id: id,
            status: :unread,
            version: 1,
            inserted_at: DateTime.utc_now()
          })

        state = %{
          state
          | rows: Map.put(state.rows, id, row),
            keys: Map.put(state.keys, attrs.idempotency_key, id),
            next_id: id + 1
        }

        {:reply, {:created, row}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.rows, id) do
        {:ok, row} -> {:ok, row}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, id, expected_version, changes}, _from, state) do
    case Map.fetch(state.rows, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{version: ^expected_version} = row} ->
        updated = row |> Map.merge(changes) |> Map.update!(:version, &(&1 + 1))
        state = %{state | rows: Map.put(state.rows, id, updated)}
        {:reply, {:ok, updated}, state}

      {:ok, _row} ->
        {:reply, {:error, :stale_version}, state}
    end
  end

  def handle_call({:list_by_user, user_id}, _from, state) do
    rows =
      state.rows
      |> Map.values()
      |> Enum.filter(&(&1.user_id == user_id))
      |> Enum.sort_by(& &1.id)

    {:reply, rows, state}
  end
end

# Executa a demonstração ao rodar o arquivo como script.
NotificationSystem.demo()
