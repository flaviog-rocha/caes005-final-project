# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Implementar expiração automática de registros.
# 3. Manter um histórico das operações realizadas.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de transações financeiras em Elixir (arquivo único).
#
# Requisitos atendidos:
#   1. Persistência via behaviour (Finance.Storage). Há um adaptador em memória
#      e um adaptador DETS (biblioteca padrão do OTP). Um adaptador Ecto pode
#      implementar o mesmo behaviour (ver comentário no final do arquivo).
#   2. Expiração automática: requisições pendentes expiram, chaves de idempotência
#      expiram e registros antigos são removidos periodicamente.
#   3. Histórico: todo evento relevante é registrado (append-only em memória).
#   4. Idempotência: cada operação exige uma chave única; repetir a chave dentro
#      da janela de validade é rejeitado, e confirmar a mesma transação duas vezes
#      também é rejeitado.
#
# Arquitetura: núcleo funcional puro (Finance.Ledger) + processo de estado
# (Finance.Server) que orquestra persistência e expiração periódica.

defmodule Finance.Storage do
  @moduledoc """
  Contrato de persistência (banco fictício). Qualquer adaptador deve implementar
  `load/1` para recuperar o snapshot do ledger e `save/2` para gravá-lo.
  """

  @callback load(opts :: keyword()) :: {:ok, Finance.Ledger.t()} | {:error, term()}
  @callback save(ledger :: Finance.Ledger.t(), opts :: keyword()) :: :ok | {:error, term()}
end

defmodule Finance.Storage.Memory do
  @moduledoc """
  Adaptador sem persistência em disco: o estado vive apenas no processo do servidor.
  """

  @behaviour Finance.Storage

  @impl true
  def load(_opts), do: {:ok, %Finance.Ledger{}}

  @impl true
  def save(_ledger, _opts), do: :ok
end

defmodule Finance.Storage.DETS do
  @moduledoc """
  Adaptador de banco fictício baseado em DETS (OTP, sem dependências externas).
  Grava o snapshot completo do ledger sob uma chave fixa.

  Opções: `file: "caminho/ledger.dets"`
  """

  @behaviour Finance.Storage

  @table :finance_ledger_dets
  @key :snapshot

  @impl true
  def load(opts) do
    with_table(opts, fn tab ->
      case :dets.lookup(tab, @key) do
        [{@key, ledger}] -> {:ok, ledger}
        [] -> {:ok, %Finance.Ledger{}}
      end
    end)
  end

  @impl true
  def save(ledger, opts) do
    with_table(opts, fn tab -> :dets.insert(tab, {@key, ledger}) end)
  end

  defp with_table(opts, fun) do
    file = opts |> Keyword.fetch!(:file) |> String.to_charlist()

    case :dets.open_file(@table, file: file, type: :set) do
      {:ok, tab} ->
        try do
          fun.(tab)
        after
          :dets.close(tab)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end

defmodule Finance.Ledger do
  @moduledoc """
  Núcleo funcional puro do ledger. Não realiza I/O e não lê o relógio:
  todas as funções recebem `now` em milissegundos, o que facilita testes.

  Ciclo de vida de uma transação:

      request/3  -> :pending  (expira após `pending_ttl`)
      confirm/3  -> :completed (aplica o efeito nos saldos)
      expire/2   -> :expired  (requisições pendentes vencidas)

  Expiração de registros:
    * requisições pendentes  -> `pending_ttl`
    * chaves de idempotência -> `key_ttl` (após isso a chave pode ser reutilizada)
    * transações e histórico -> `record_ttl` (removidos após a retenção)
  """

  defstruct balances: %{},
            # id => transação
            transactions: %{},
            # idempotency_key => %{tx_id: id, expires_at: ms}
            keys: %{},
            # eventos, do mais recente para o mais antigo
            history: [],
            pending_ttl: 5 * 60_000,
            key_ttl: 24 * 3_600_000,
            record_ttl: 30 * 24 * 3_600_000

  @type t :: %__MODULE__{}

  @types [:deposit, :withdraw, :transfer]

  # ---------------------------------------------------------------------------
  # Consultas puras
  # ---------------------------------------------------------------------------

  @doc "Saldo de uma conta (0 se inexistente)."
  def balance(%__MODULE__{balances: b}, account), do: Map.get(b, account, 0)

  @doc "Transação pelo id."
  def transaction(%__MODULE__{transactions: txs}, id), do: Map.get(txs, id)

  @doc "Histórico em ordem cronológica, opcionalmente filtrado por transação."
  def history(%__MODULE__{history: h}, tx_id \\ nil) do
    h
    |> Enum.reverse()
    |> Enum.filter(fn e -> is_nil(tx_id) or e.tx_id == tx_id end)
  end

  # ---------------------------------------------------------------------------
  # Operações
  # ---------------------------------------------------------------------------

  @doc """
  Cria uma requisição pendente.

  `attrs`: `%{type: :deposit | :withdraw | :transfer, from: conta | nil,
             to: conta | nil, amount: inteiro_positivo, key: string_unica}`

  Retorna `{:ok, tx, ledger}` ou `{:error, motivo}`.
  Se a chave já estiver ativa, retorna `{:error, {:duplicate_operation, tx_id}}`.
  """
  def request(%__MODULE__{} = l, attrs, now) do
    with {:ok, op} <- validate(attrs),
         :ok <- check_key(l, op.key, now) do
      id = new_id()

      tx = %{
        id: id,
        type: op.type,
        from: op.from,
        to: op.to,
        amount: op.amount,
        key: op.key,
        status: :pending,
        created_at: now,
        expires_at: now + l.pending_ttl
      }

      l =
        %{
          l
          | transactions: Map.put(l.transactions, id, tx),
            keys: Map.put(l.keys, op.key, %{tx_id: id, expires_at: now + l.key_ttl})
        }
        |> log(:requested, id, now, %{type: op.type, amount: op.amount})

      {:ok, tx, l}
    end
  end

  @doc """
  Confirma uma requisição pendente e aplica o efeito nos saldos.
  Uma transação só pode ser confirmada uma vez (idempotência por estado).
  """
  def confirm(%__MODULE__{} = l, id, now) do
    with {:ok, tx} <- fetch_tx(l, id),
         :ok <- check_pending(tx, now),
         {:ok, balances} <- apply_op(l.balances, tx) do
      tx = %{tx | status: :completed}

      l =
        %{l | balances: balances, transactions: Map.put(l.transactions, id, tx)}
        |> log(:completed, id, now, %{amount: tx.amount})

      {:ok, tx, l}
    end
  end

  @doc """
  Varredura de expiração: marca requisições vencidas, remove chaves de
  idempotência vencidas e descarta registros além do período de retenção.
  """
  def expire(%__MODULE__{} = l, now) do
    newly_expired =
      for {id, %{status: :pending, expires_at: exp}} <- l.transactions, now >= exp,
          do: id

    txs =
      Enum.reduce(l.transactions, %{}, fn {id, tx}, acc ->
        tx = if id in newly_expired, do: %{tx | status: :expired}, else: tx

        if now - tx.created_at >= l.record_ttl,
          do: acc,
          else: Map.put(acc, id, tx)
      end)

    keys = Map.reject(l.keys, fn {_key, %{expires_at: exp}} -> now >= exp end)

    history = Enum.filter(l.history, fn e -> now - e.at < l.record_ttl end)

    l = %{l | transactions: txs, keys: keys, history: history}

    Enum.reduce(newly_expired, l, fn id, acc -> log(acc, :expired, id, now) end)
  end

  # ---------------------------------------------------------------------------
  # Validação e regras internas
  # ---------------------------------------------------------------------------

  defp validate(%{type: type, amount: amount, key: key} = attrs)
       when type in @types and is_integer(amount) and amount > 0 and
              is_binary(key) and byte_size(key) > 0 do
    op = %{
      type: type,
      from: Map.get(attrs, :from),
      to: Map.get(attrs, :to),
      amount: amount,
      key: key
    }

    case {type, op.from, op.to} do
      {:deposit, nil, to} when is_binary(to) -> {:ok, op}
      {:withdraw, from, nil} when is_binary(from) -> {:ok, op}
      {:transfer, from, to} when is_binary(from) and is_binary(to) and from != to -> {:ok, op}
      _ -> {:error, :invalid_accounts}
    end
  end

  defp validate(_attrs), do: {:error, :invalid_params}

  # Chave ativa (não expirada) bloqueia nova execução da mesma operação.
  defp check_key(l, key, now) do
    case Map.get(l.keys, key) do
      nil -> :ok
      %{expires_at: exp, tx_id: id} when exp > now -> {:error, {:duplicate_operation, id}}
      _expired -> :ok
    end
  end

  defp fetch_tx(l, id) do
    case Map.fetch(l.transactions, id) do
      {:ok, tx} -> {:ok, tx}
      :error -> {:error, :not_found}
    end
  end

  defp check_pending(%{status: :pending, expires_at: exp}, now) when now < exp, do: :ok
  defp check_pending(%{status: :pending}, _now), do: {:error, :expired}
  defp check_pending(%{status: status}, _now), do: {:error, {:invalid_status, status}}

  defp apply_op(b, %{type: :deposit, to: to, amount: amount}) do
    {:ok, credit(b, to, amount)}
  end

  defp apply_op(b, %{type: :withdraw, from: from, amount: amount}) do
    if Map.get(b, from, 0) >= amount,
      do: {:ok, debit(b, from, amount)},
      else: {:error, :insufficient_funds}
  end

  defp apply_op(b, %{type: :transfer, from: from, to: to, amount: amount}) do
    if Map.get(b, from, 0) >= amount,
      do: {:ok, b |> debit(from, amount) |> credit(to, amount)},
      else: {:error, :insufficient_funds}
  end

  defp credit(b, account, amount), do: Map.update(b, account, amount, &(&1 + amount))
  defp debit(b, account, amount), do: Map.update!(b, account, &(&1 - amount))

  defp log(l, event, tx_id, now, details \\ %{}) do
    entry = %{at: now, event: event, tx_id: tx_id, details: details}
    %{l | history: [entry | l.history]}
  end

  # Identificador aleatório (crypto faz parte do OTP).
  defp new_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
end

defmodule Finance.Server do
  @moduledoc """
  Processo que mantém o ledger em memória, persiste cada mutação via
  `Finance.Storage` e executa a varredura periódica de expiração.

  Opções de start_link:
    * `:name`           - nome do processo (padrão: `Finance.Server`)
    * `:storage`        - módulo que implementa `Finance.Storage` (padrão: Memory)
    * `:storage_opts`   - opções do adaptador (ex.: `[file: "ledger.dets"]`)
    * `:sweep_interval` - intervalo da varredura em ms (padrão: 1_000)
  """

  use GenServer

  alias Finance.Ledger

  # --- API pública -----------------------------------------------------------

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def request(server \\ __MODULE__, attrs), do: GenServer.call(server, {:request, attrs})

  def confirm(server \\ __MODULE__, id), do: GenServer.call(server, {:confirm, id})

  def balance(server \\ __MODULE__, account), do: GenServer.call(server, {:balance, account})

  def transaction(server \\ __MODULE__, id), do: GenServer.call(server, {:transaction, id})

  def history(server \\ __MODULE__, tx_id \\ nil),
    do: GenServer.call(server, {:history, tx_id})

  # --- Callbacks -------------------------------------------------------------

  @impl true
  def init(opts) do
    storage = Keyword.get(opts, :storage, Finance.Storage.Memory)
    storage_opts = Keyword.get(opts, :storage_opts, [])
    interval = Keyword.get(opts, :sweep_interval, 1_000)

    {:ok, ledger} = storage.load(storage_opts)
    schedule(interval)

    {:ok, %{ledger: ledger, storage: storage, storage_opts: storage_opts, interval: interval}}
  end

  @impl true
  def handle_call({:request, attrs}, _from, s) do
    case Ledger.request(s.ledger, attrs, now_ms()) do
      {:ok, tx, ledger} -> {:reply, {:ok, tx}, commit(s, ledger)}
      {:error, _reason} = error -> {:reply, error, s}
    end
  end

  def handle_call({:confirm, id}, _from, s) do
    case Ledger.confirm(s.ledger, id, now_ms()) do
      {:ok, tx, ledger} -> {:reply, {:ok, tx}, commit(s, ledger)}
      {:error, _reason} = error -> {:reply, error, s}
    end
  end

  def handle_call({:balance, account}, _from, s) do
    {:reply, Ledger.balance(s.ledger, account), s}
  end

  def handle_call({:transaction, id}, _from, s) do
    {:reply, Ledger.transaction(s.ledger, id), s}
  end

  def handle_call({:history, tx_id}, _from, s) do
    {:reply, Ledger.history(s.ledger, tx_id), s}
  end

  @impl true
  def handle_info(:sweep, s) do
    ledger = Ledger.expire(s.ledger, now_ms())

    # Só persiste quando houve alteração, evitando escrita desnecessária.
    s = if ledger == s.ledger, do: s, else: commit(s, ledger)

    schedule(s.interval)
    {:noreply, s}
  end

  # --- Auxiliares ------------------------------------------------------------

  defp commit(%{storage: mod, storage_opts: opts} = s, ledger) do
    # Falha de persistência derruba o processo; o supervisor o reinicia
    # a partir do último snapshot gravado com sucesso.
    :ok = mod.save(ledger, opts)
    %{s | ledger: ledger}
  end

  defp schedule(interval), do: Process.send_after(self(), :sweep, interval)

  defp now_ms, do: System.system_time(:millisecond)
end

# =============================================================================
# Uso de exemplo (referência, não executado automaticamente):
#
#   {:ok, _pid} = Finance.Server.start_link(
#     storage: Finance.Storage.DETS,
#     storage_opts: [file: "ledger.dets"]
#   )
#
#   # Deposit com chave de idempotência
#   {:ok, tx} = Finance.Server.request(%{type: :deposit, to: "alice",
#                                        amount: 10_000, key: "req-001"})
#   {:ok, _}  = Finance.Server.confirm(tx.id)
#
#   # Repetir a mesma chave é rejeitado
#   {:error, {:duplicate_operation, _}} =
#     Finance.Server.request(%{type: :deposit, to: "alice",
#                              amount: 10_000, key: "req-001"})
#
#   # Confirmar novamente é rejeitado
#   {:error, {:invalid_status, :completed}} = Finance.Server.confirm(tx.id)
#
#   Finance.Server.balance("alice")   # => 10000
#   Finance.Server.history()          # => eventos em ordem cronológica
#
# Adaptador Ecto (opcional): um módulo `Finance.Storage.Ecto` poderia implementar
# o mesmo behaviour gravando o snapshot em uma tabela, por exemplo com a migração:
#
#   defmodule MyApp.Repo.Migrations.CreateLedgerSnapshots do
#     use Ecto.Migration
#
#     def change do
#       create table(:ledger_snapshots, primary_key: false) do
#         add :id, :string, primary_key: true
#         add :data, :binary, null: false
#         timestamps()
#       end
#     end
#   end
#
# O snapshot pode ser serializado com :erlang.term_to_binary/1 e recuperado
# com :erlang.binary_to_term/1.
# =============================================================================
