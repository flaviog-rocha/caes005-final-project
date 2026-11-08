# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Fazer uso de macros.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de troca de mensagens entre usuários.
# Requisitos atendidos:
#   1. Atualizações atômicas (sem atualizações parcialmente concluídas).
#   2. Consulta do estado atual das operações (queued/running/done/failed).
#   3. Uso de macros (macro `operation/2`).
#   4. Idempotência: um mesmo id de operação nunca é executado duas vezes.
#   5. CRUD de registros (create, get, list, update, delete).
# Recursos: Supervisor (one_for_one) e GenServer/Task da biblioteca padrão.

defmodule MessagingSystem.Records do
  # Operações puras sobre o "banco" de registros.
  # O db é um valor imutável: nenhuma função altera dados in-place.
  # Um db alterado só é efetivado quando a operação inteira retorna {:ok, _, novo_db}.

  @moduledoc false

  def empty, do: %{user: %{}, message: %{}, seq: 0}

  # Insere um registro gerando um novo id sequencial.
  def insert(db, kind, attrs) do
    id = db.seq + 1
    record = attrs |> Map.put(:id, id)
    db = %{db | seq: id} |> put(kind, id, record)
    {:ok, record, db}
  end

  # Consulta um registro pelo id.
  def fetch(db, kind, id) do
    case Map.fetch(Map.fetch!(db, kind), id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, {:not_found, kind, id}}
    end
  end

  # Lista todos os registros de um tipo, ordenados pelo id.
  def all(db, kind) do
    db
    |> Map.fetch!(kind)
    |> Map.values()
    |> Enum.sort_by(& &1.id)
  end

  # Atualiza campos de um registro existente (merge).
  def update(db, kind, id, changes) do
    with {:ok, record} <- fetch(db, kind, id) do
      updated = Map.merge(record, Map.delete(changes, :id))
      {:ok, updated, put(db, kind, id, updated)}
    end
  end

  # Remove um registro existente.
  def delete(db, kind, id) do
    with {:ok, record} <- fetch(db, kind, id) do
      {:ok, record, Map.update!(db, kind, &Map.delete(&1, id))}
    end
  end

  defp put(db, kind, id, record) do
    Map.update!(db, kind, &Map.put(&1, id, record))
  end
end

defmodule MessagingSystem.Operation do
  # Macro que transforma um bloco de código em uma operação transacional
  # e idempotente, identificada por `op_id`.
  #
  # Dentro do bloco, a variável `db` contém o snapshot atual dos dados.
  # O bloco deve retornar {:ok, resultado, novo_db} para efetivar,
  # ou {:error, motivo} para descartar todas as alterações.

  @moduledoc false

  defmacro operation(op_id, do: block) do
    quote do
      MessagingSystem.Store.submit(unquote(op_id), fn var!(db) ->
        unquote(block)
      end)
    end
  end
end

defmodule MessagingSystem.Store do
  # Processo dono do estado. Serializa as escritas: apenas uma operação
  # executa por vez, sobre um snapshot consistente, e só seu resultado
  # completo é aplicado ao estado global.

  use GenServer

  alias MessagingSystem.Records

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Submete uma operação. Bloqueia até ela terminar.
  def submit(op_id, fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:submit, op_id, fun}, :infinity)
  end

  # Executa uma leitura sobre o estado committed.
  def read(fun) when is_function(fun, 1) do
    GenServer.call(__MODULE__, {:read, fun})
  end

  # Consulta o status de uma operação pelo id.
  def operation_status(op_id) do
    GenServer.call(__MODULE__, {:status, op_id})
  end

  # Lista todas as operações registradas.
  def operations do
    GenServer.call(__MODULE__, :operations)
  end

  # Lista apenas operações em andamento (enfileiradas ou executando).
  def in_progress do
    GenServer.call(__MODULE__, :in_progress)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    {:ok,
     %{
       db: Records.empty(),
       # op_id => %{status, result, submitted_at, finished_at}
       ops: %{},
       # fila FIFO de {op_id, fun, from}
       queue: :queue.new(),
       # operação atualmente em execução: %{op_id, from, ref} | nil
       running: nil
     }}
  end

  @impl true
  def handle_call({:submit, op_id, fun}, from, state) do
    case Map.get(state.ops, op_id) do
      # Idempotência: qualquer id já submetido é rejeitado.
      %{status: status} ->
        {:reply, {:error, {:already_executed, status}}, state}

      nil ->
        entry = %{status: :queued, result: nil, submitted_at: now(), finished_at: nil}

        state = %{
          state
          | ops: Map.put(state.ops, op_id, entry),
            queue: :queue.in({op_id, fun, from}, state.queue)
        }

        # Resposta é enviada depois, em handle_info/2.
        {:noreply, start_next(state)}
    end
  end

  def handle_call({:read, fun}, _from, state) do
    {:reply, safe_run(fun, state.db), state}
  end

  def handle_call({:status, op_id}, _from, state) do
    {:reply, Map.get(state.ops, op_id), state}
  end

  def handle_call(:operations, _from, state) do
    {:reply, state.ops, state}
  end

  def handle_call(:in_progress, _from, state) do
    ops =
      state.ops
      |> Enum.filter(fn {_id, entry} -> entry.status in [:queued, :running] end)
      |> Map.new()

    {:reply, ops, state}
  end

  @impl true
  def handle_info({ref, result}, %{running: %{ref: ref} = run} = state) do
    Process.demonitor(ref, [:flush])

    # Só aplica o novo db se a operação inteira foi bem-sucedida.
    {status, reply, db} =
      case result do
        {:ok, value, new_db} -> {:done, {:ok, value}, new_db}
        {:error, reason} -> {:failed, {:error, reason}, state.db}
        other -> {:failed, {:error, {:invalid_result, other}}, state.db}
      end

    ops =
      Map.update!(state.ops, run.op_id, fn entry ->
        %{entry | status: status, result: reply, finished_at: now()}
      end)

    GenServer.reply(run.from, reply)

    state = %{state | db: db, ops: ops, running: nil}
    {:noreply, start_next(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  # Inicia a próxima operação da fila, se nenhuma estiver executando.
  defp start_next(%{running: nil} = state) do
    case :queue.out(state.queue) do
      {{:value, {op_id, fun, from}}, rest} ->
        snapshot = state.db
        # A operação roda em Task sobre um snapshot imutável; o GenServer
        # permanece responsivo para consultas de status enquanto isso.
        task = Task.async(fn -> safe_run(fun, snapshot) end)

        ops =
          Map.update!(state.ops, op_id, fn entry ->
            %{entry | status: :running}
          end)

        %{state | queue: rest, ops: ops, running: %{op_id: op_id, from: from, ref: task.ref}}

      {:empty, _} ->
        state
    end
  end

  defp start_next(state), do: state

  # Executa a função isolando exceções, para que nenhuma falha derrube o Store.
  defp safe_run(fun, db) do
    fun.(db)
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp now, do: DateTime.utc_now()
end

defmodule MessagingSystem.Supervisor do
  # Supervisor de topo. Se o Store cair, ele é reiniciado (estado em memória
  # é perdido nesse caso; a estratégia garante que o sistema continue no ar).

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      MessagingSystem.Store
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule MessagingSystem.Application do
  # Ponto de entrada OTP. Pode ser referenciado em mix.exs via `mod:`.

  use Application

  @impl true
  def start(_type, _args) do
    MessagingSystem.Supervisor.start_link()
  end
end

defmodule MessagingSystem do
  # API pública: CRUD de registros e troca de mensagens.
  # Todas as escritas são operações (macro `operation`), logo atômicas e idempotentes.

  import MessagingSystem.Operation

  alias MessagingSystem.{Records, Store}

  @kinds [:user, :message]

  # ---------------------------------------------------------------------------
  # CRUD (escrita via operações)
  # ---------------------------------------------------------------------------

  # Cadastra um registro. `op_id` é a chave de idempotência.
  def create(op_id, kind, attrs) when kind in @kinds and is_map(attrs) do
    operation op_id do
      Records.insert(db, kind, attrs)
    end
  end

  # Atualiza um registro existente.
  def update(op_id, kind, id, changes) when kind in @kinds and is_map(changes) do
    operation op_id do
      Records.update(db, kind, id, changes)
    end
  end

  # Remove um registro existente.
  def delete(op_id, kind, id) when kind in @kinds do
    operation op_id do
      Records.delete(db, kind, id)
    end
  end

  # ---------------------------------------------------------------------------
  # CRUD (leitura)
  # ---------------------------------------------------------------------------

  def get(kind, id) when kind in @kinds do
    Store.read(&Records.fetch(&1, kind, id))
  end

  def list(kind) when kind in @kinds do
    Store.read(&{:ok, Records.all(&1, kind)})
  end

  # ---------------------------------------------------------------------------
  # Troca de mensagens (operações compostas e atômicas)
  # ---------------------------------------------------------------------------

  # Envia uma mensagem: valida remetente e destinatário, insere a mensagem
  # e incrementa o contador de envios do remetente. Ou tudo ocorre, ou nada.
  def send_message(op_id, from_id, to_id, body) when is_binary(body) do
    operation op_id do
      with {:ok, sender} <- Records.fetch(db, :user, from_id),
           {:ok, _recipient} <- Records.fetch(db, :user, to_id),
           {:ok, message, db} <-
             Records.insert(db, :message, %{from: from_id, to: to_id, body: body, read: false}),
           {:ok, _updated, db} <-
             Records.update(db, :user, from_id, %{
               sent_count: Map.get(sender, :sent_count, 0) + 1
             }) do
        {:ok, message, db}
      end
    end
  end

  # Marca uma mensagem como lida. Apenas o destinatário pode fazê-lo.
  def mark_as_read(op_id, message_id, user_id) do
    operation op_id do
      with {:ok, message} <- Records.fetch(db, :message, message_id),
           :ok <- if(message.to == user_id, do: :ok, else: {:error, :not_recipient}),
           {:ok, _updated, db} <- Records.update(db, :message, message_id, %{read: true}) do
        {:ok, :read, db}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Consulta de operações
  # ---------------------------------------------------------------------------

  def operation_status(op_id), do: Store.operation_status(op_id)
  def operations, do: Store.operations()
  def in_progress, do: Store.in_progress()
end
