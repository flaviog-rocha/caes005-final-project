# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Permitir comunicação entre diferentes processos.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de transações financeiras com GenServer.
#
# Requisitos atendidos:
# 1. Recuperação de falhas: cada conta é um GenServer supervisionado. Se o processo
#    falhar, o supervisor o reinicia e o estado é recarregado do armazenamento.
# 2. Comunicação entre processos: chamadas síncronas entre contas via GenServer.call,
#    transferências coordenadas e um barramento de eventos (Registry duplicado).
# 3. Notificações: regras de limite (:below / :above) disparam eventos quando a
#    condição é atingida (cruzamento de limite), entregues a processos inscritos.
# 4. Persistência: estado salvo em DETS (biblioteca padrão), com sincronização em disco
#    a cada operação, permitindo recuperação após reinicializações.
#
# Uso (iex -S mix ou iex com este arquivo carregado):
#   {:ok, _} = Finance.start(path: "financeiro.dets")
#   :ok = Finance.subscribe(:all)
#   :ok = Finance.alert(:conta_a, {:below, 100})
#   {:ok, 500} = Finance.deposit(:conta_a, 500, "tx-1")
#   {:ok, 400} = Finance.withdraw(:conta_a, 100, "tx-2")
#   {:ok, 50}  = Finance.withdraw(:conta_a, 350, "tx-3")  # dispara alerta :below 100
#   # Mensagem recebida: {:finance_event, {:rule_triggered, :conta_a, {:below, 100}, 50}}
#   {:ok, :transferred} = Finance.transfer(:conta_a, :conta_b, 20, "tx-4")

defmodule Finance do
  @moduledoc """
  API pública do sistema de transações financeiras.
  Todas as operações usam `tx_id` para idempotência: repetir uma transação já
  aplicada (ex.: retry após falha) não altera o saldo novamente.
  """

  alias Finance.Account

  @doc "Inicia a árvore de supervisão. Opções: `path:` caminho do arquivo DETS."
  def start(opts \\ []) do
    Finance.Root.start_link(opts)
  end

  @doc "Garante que o processo da conta exista (criando-o se necessário)."
  def ensure_account(id) do
    case DynamicSupervisor.start_child(Finance.AccountSup, {Account, id}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      other -> other
    end
  end

  @doc "Reinicia em memória todas as contas persistidas (chamado no boot)."
  def restore_accounts do
    for {id, _state} <- Finance.Store.all_accounts(), do: ensure_account(id)
    :ok
  end

  def deposit(id, amount, tx_id) do
    with :ok <- ensure_account(id), do: Account.apply_tx(id, {:deposit, amount, tx_id})
  end

  def withdraw(id, amount, tx_id) do
    with :ok <- ensure_account(id), do: Account.apply_tx(id, {:withdraw, amount, tx_id})
  end

  def balance(id) do
    with :ok <- ensure_account(id), do: Account.balance(id)
  end

  @doc """
  Transfere valores entre contas em duas etapas (débito e crédito).
  Se o crédito falhar com erro, realiza compensação (estorno) na origem.
  """
  def transfer(from, to, amount, tx_id) do
    with :ok <- ensure_account(from),
         :ok <- ensure_account(to),
         {:ok, _} <- Account.apply_tx(from, {:withdraw, amount, {tx_id, :debit}}) do
      case Account.apply_tx(to, {:deposit, amount, {tx_id, :credit}}) do
        {:ok, _} ->
          {:ok, :transferred}

        {:error, reason} ->
          # Compensação: devolve o valor debitado à conta de origem.
          {:ok, _} = Account.apply_tx(from, {:deposit, amount, {tx_id, :rollback}})
          {:error, reason}
      end
    end
  end

  @doc "Define uma regra de notificação: `{:below, limite}` ou `{:above, limite}`."
  def alert(id, rule) do
    with :ok <- ensure_account(id), do: Account.add_rule(id, rule)
  end

  @doc "Inscreve o processo chamador em um tópico: `:all` ou `{:account, id}`."
  defdelegate subscribe(topic), to: Finance.PubSub

  @doc "Cancela a inscrição do processo chamador em um tópico."
  defdelegate unsubscribe(topic), to: Finance.PubSub
end

defmodule Finance.Store do
  @moduledoc """
  Persistência em DETS (stdlib). Um GenServer de longa duração é o dono da tabela,
  serializando o acesso e garantindo a sincronização em disco.
  """
  use GenServer

  @table :finance_store

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def get(key), do: GenServer.call(__MODULE__, {:get, key})
  def put(key, value), do: GenServer.call(__MODULE__, {:put, key, value})
  def all_accounts, do: GenServer.call(__MODULE__, :all_accounts)

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, "finance_data.dets") |> to_charlist()

    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, @table} -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, _state), do: :dets.close(@table)

  @impl true
  def handle_call({:get, key}, _from, state) do
    reply =
      case :dets.lookup(@table, key) do
        [{^key, value}] -> {:ok, value}
        [] -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    :ok = :dets.insert(@table, {key, value})
    :ok = :dets.sync(@table)
    {:reply, :ok, state}
  end

  def handle_call(:all_accounts, _from, state) do
    accounts =
      for {{:account, id}, saved} <- :dets.match_object(@table, {{:account, :_}, :_}),
          do: {id, saved}

    {:reply, accounts, state}
  end
end

defmodule Finance.PubSub do
  @moduledoc """
  Barramento de eventos baseado em Registry com chaves duplicadas.
  Processos inscritos recebem mensagens `{:finance_event, evento}`.
  """

  @registry :finance_pubsub

  def subscribe(topic), do: Registry.register(@registry, topic, nil)
  def unsubscribe(topic), do: Registry.unregister(@registry, topic)

  def publish(account_id, event) do
    for topic <- [:all, {:account, account_id}] do
      Registry.dispatch(@registry, topic, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:finance_event, event})
      end)
    end

    :ok
  end

  def registry, do: @registry
end

defmodule Finance.Account do
  @moduledoc """
  Um processo GenServer por conta. O estado é carregado do armazenamento no `init/1`,
  permitindo que o supervisor recrie a conta após uma falha sem perda de dados
  confirmados. Operações são persistidas antes da resposta ao chamador.

  Estado:
    * `:id`      - identificador da conta
    * `:balance` - saldo em centavos (inteiro)
    * `:applied` - MapSet de `tx_id` já aplicados (idempotência)
    * `:rules`   - regras de notificação ativas
  """
  use GenServer

  alias Finance.{Store, PubSub}

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))

  def apply_tx(id, tx), do: call(id, {:apply, tx})
  def balance(id), do: call(id, :balance)
  def add_rule(id, rule), do: call(id, {:add_rule, rule})

  defp via(id), do: {:via, Registry, {Finance.Registry, id}}
  defp call(id, msg), do: GenServer.call(via(id), msg)

  @impl true
  def init(id) do
    state =
      case Store.get({:account, id}) do
        {:ok, saved} -> saved
        :not_found -> %{id: id, balance: 0, applied: MapSet.new(), rules: []}
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:apply, {kind, amount, tx_id}}, _from, state)
      when kind in [:deposit, :withdraw] and is_integer(amount) and amount > 0 do
    if MapSet.member?(state.applied, tx_id) do
      # Transação já aplicada: resposta idempotente, sem alterar saldo.
      {:reply, {:ok, state.balance}, state}
    else
      case apply_kind(kind, state.balance, amount) do
        {:error, reason} ->
          {:reply, {:error, reason}, state}

        {:ok, new_balance} ->
          new_state = %{
            state
            | balance: new_balance,
              applied: MapSet.put(state.applied, tx_id)
          }

          # Persiste antes de responder: garante recuperação após falhas.
          :ok = Store.put({:account, state.id}, new_state)

          # Notifica apenas regras cujo limite foi cruzado nesta operação.
          for rule <- state.rules, crossed?(rule, state.balance, new_balance) do
            PubSub.publish(state.id, {:rule_triggered, state.id, rule, new_balance})
          end

          {:reply, {:ok, new_balance}, new_state}
      end
    end
  end

  def handle_call({:apply, _invalid}, _from, state) do
    {:reply, {:error, :invalid_transaction}, state}
  end

  def handle_call(:balance, _from, state) do
    {:reply, {:ok, state.balance}, state}
  end

  def handle_call({:add_rule, {kind, threshold} = rule}, _from, state)
      when kind in [:below, :above] and is_integer(threshold) do
    new_state = %{state | rules: [rule | state.rules]}
    :ok = Store.put({:account, state.id}, new_state)
    {:reply, :ok, new_state}
  end

  def handle_call({:add_rule, _invalid}, _from, state) do
    {:reply, {:error, :invalid_rule}, state}
  end

  defp apply_kind(:deposit, balance, amount), do: {:ok, balance + amount}

  defp apply_kind(:withdraw, balance, amount) when balance >= amount,
    do: {:ok, balance - amount}

  defp apply_kind(:withdraw, _balance, _amount), do: {:error, :insufficient_funds}

  # Detecta cruzamento do limite (transição de um lado para o outro).
  defp crossed?({:below, t}, old, new), do: old >= t and new < t
  defp crossed?({:above, t}, old, new), do: old <= t and new > t
end

defmodule Finance.Root do
  @moduledoc """
  Árvore de supervisão. A ordem dos filhos e a estratégia `:rest_for_one` garantem que,
  se o armazenamento cair, as contas dependentes sejam reiniciadas e recarreguem o estado.
  """
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Finance.Store, opts},
      {Registry, keys: :unique, name: Finance.Registry},
      {Registry, keys: :duplicate, name: Finance.PubSub.registry()},
      {DynamicSupervisor, strategy: :one_for_one, name: Finance.AccountSup},
      # Task transitória que reidrata as contas persistidas no boot.
      Supervisor.child_spec({Task, fn -> Finance.restore_accounts() end}, restart: :transient)
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
