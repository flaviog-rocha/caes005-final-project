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

# Arquivo autocontido: gerenciamento de transações financeiras com concorrência limitada.
# Executar: elixir transacoes.exs

# Banco de dados fictício em memória.
# Em produção, poderia ser substituído por um Ecto.Repo com Ecto.Migration
# (ex.: Repo.transaction/1 com SELECT ... FOR UPDATE nas contas).
defmodule Finance.FakeDB do
  @moduledoc """
  Armazena saldos e um log de transações em memória.
  Cada transferência é executada atomicamente dentro de Agent.get_and_update/2,
  evitando condições de corrida entre operações simultâneas.
  """
  use Agent

  @doc false
  def start_link(opts) do
    accounts = Keyword.get(opts, :accounts, %{})
    Agent.start_link(fn -> %{accounts: accounts, log: []} end, name: __MODULE__)
  end

  @doc "Retorna um snapshot dos saldos atuais."
  def snapshot do
    Agent.get(__MODULE__, & &1.accounts)
  end

  @doc "Retorna o log de transações efetivadas (mais recente primeiro)."
  def log do
    Agent.get(__MODULE__, & &1.log)
  end

  @doc """
  Transfere `amount` (inteiro, em centavos) da conta `from` para `to`.
  Operação atômica: ou ambas as contas são atualizadas, ou nenhuma é.
  """
  def transfer(tx_id, from, to, amount) do
    Agent.get_and_update(__MODULE__, fn %{accounts: accounts} = state ->
      with {:ok, from_balance} <- fetch(accounts, from),
           {:ok, to_balance} <- fetch(accounts, to),
           :ok <- check_funds(from_balance, amount) do
        new_accounts =
          accounts
          |> Map.put(from, from_balance - amount)
          |> Map.put(to, to_balance + amount)

        entry = %{id: tx_id, from: from, to: to, amount: amount, at: DateTime.utc_now()}
        new_state = %{state | accounts: new_accounts, log: [entry | state.log]}

        {{:ok, entry}, new_state}
      else
        {:error, _reason} = error -> {error, state}
      end
    end)
  end

  defp fetch(accounts, id) do
    case Map.fetch(accounts, id) do
      {:ok, balance} -> {:ok, balance}
      :error -> {:error, :account_not_found}
    end
  end

  defp check_funds(balance, amount) when balance >= amount, do: :ok
  defp check_funds(_balance, _amount), do: {:error, :insufficient_funds}
end

# Processo temporário que executa uma única transação.
# Iniciado sob o DynamicSupervisor; ao terminar, notifica o Gateway e
# aguarda ser encerrado por ele (o que libera o slot de concorrência).
defmodule Finance.Transactions.Worker do
  @moduledoc false
  use GenServer, restart: :temporary

  alias Finance.FakeDB

  def start_link({gateway, tx}) do
    GenServer.start_link(__MODULE__, {gateway, tx})
  end

  @impl true
  def init(arg) do
    {:ok, arg, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, {gateway, tx} = state) do
    result = process(tx)
    send(gateway, {:worker_done, self(), result})
    {:noreply, state}
  end

  defp process(%{latency: latency} = tx) do
    # Simula latência de rede/banco de dados.
    Process.sleep(latency)
    FakeDB.transfer(tx.id, tx.from, tx.to, tx.amount)
  end
end

# Gateway: controla quantas transações rodam ao mesmo tempo.
# Requisições excedentes ficam em uma fila (:queue) e são despachadas
# conforme os workers terminam. O DynamicSupervisor também recebe
# `max_children` como uma segunda barreira de segurança.
defmodule Finance.Transactions.Gateway do
  @moduledoc false
  use GenServer

  alias Finance.Transactions.Worker

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Submete uma transação e aguarda seu resultado (permite chamadas simultâneas)."
  def submit(tx) do
    GenServer.call(__MODULE__, {:submit, tx}, :infinity)
  end

  @impl true
  def init(opts) do
    state = %{
      max: Keyword.fetch!(opts, :max_concurrency),
      sup: Keyword.fetch!(opts, :worker_supervisor),
      # pid => {monitor_ref, from}
      running: %{},
      queue: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:submit, tx}, from, state) do
    state = %{state | queue: :queue.in({from, tx}, state.queue)}
    {:noreply, dispatch(state)}
  end

  @impl true
  def handle_info({:worker_done, pid, result}, state) do
    case Map.pop(state.running, pid) do
      {nil, _running} ->
        {:noreply, state}

      {{mref, from}, running} ->
        Process.demonitor(mref, [:flush])
        GenServer.reply(from, result)
        # Encerra o worker, liberando o slot no DynamicSupervisor.
        DynamicSupervisor.terminate_child(state.sup, pid)
        {:noreply, dispatch(%{state | running: running})}
    end
  end

  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_pid, {ref, _from}} -> ref == mref end) do
      nil ->
        {:noreply, state}

      {pid, {_ref, from}} ->
        GenServer.reply(from, {:error, {:worker_crashed, reason}})
        {:noreply, dispatch(%{state | running: Map.delete(state.running, pid)})}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Inicia workers enquanto houver capacidade e itens na fila.
  defp dispatch(state) do
    if map_size(state.running) < state.max and not :queue.is_empty(state.queue) do
      {{:value, {from, tx}}, queue} = :queue.out(state.queue)

      case DynamicSupervisor.start_child(state.sup, {Worker, {self(), tx}}) do
        {:ok, pid} ->
          mref = Process.monitor(pid)
          dispatch(%{state | queue: queue, running: Map.put(state.running, pid, {mref, from})})

        {:error, reason} ->
          GenServer.reply(from, {:error, reason})
          dispatch(%{state | queue: queue})
      end
    else
      state
    end
  end
end

# API pública e árvore de supervisão.
defmodule Finance.Transactions do
  @moduledoc """
  Gerenciamento de transações financeiras.

  Opções de `start_link/1`:
    * `:max_concurrency` - número máximo de transações executando simultaneamente (padrão: 10)
    * `:accounts` - mapa inicial `%{id_conta => saldo_em_centavos}`
  """

  alias Finance.FakeDB
  alias Finance.Transactions.Gateway

  @worker_sup Finance.Transactions.WorkerSupervisor

  @doc "Inicia a árvore de supervisão do sistema."
  def start_link(opts \\ []) do
    max = Keyword.get(opts, :max_concurrency, 10)

    children = [
      {FakeDB, accounts: Keyword.get(opts, :accounts, %{})},
      {DynamicSupervisor, name: @worker_sup, strategy: :one_for_one, max_children: max},
      {Gateway, max_concurrency: max, worker_supervisor: @worker_sup}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: __MODULE__)
  end

  @doc """
  Realiza uma transferência de `amount` centavos de `from` para `to`.
  Pode ser chamada por múltiplos processos simultaneamente.

  Opções:
    * `:latency` - atraso simulado em ms para a operação (padrão: 0)
  """
  def transfer(from, to, amount, opts \\ []) do
    with :ok <- validate(from, to, amount) do
      tx = %{
        id: System.unique_integer([:positive, :monotonic]),
        from: from,
        to: to,
        amount: amount,
        latency: Keyword.get(opts, :latency, 0)
      }

      Gateway.submit(tx)
    end
  end

  @doc "Retorna os saldos atuais."
  def balances, do: FakeDB.snapshot()

  @doc "Retorna o log de transações efetivadas."
  def history, do: FakeDB.log()

  defp validate(from, to, _amount) when from == to, do: {:error, :same_account}
  defp validate(_from, _to, amount) when is_integer(amount) and amount > 0, do: :ok
  defp validate(_from, _to, _amount), do: {:error, :invalid_amount}
end

# Demonstração de uso com concorrência limitada.
defmodule Finance.Demo do
  @moduledoc false
  alias Finance.Transactions

  def run do
    {:ok, sup} =
      Transactions.start_link(
        max_concurrency: 3,
        accounts: %{"A" => 1_000, "B" => 1_000, "C" => 500}
      )

    pairs = [{"A", "B"}, {"B", "C"}, {"C", "A"}, {"A", "C"}, {"B", "A"}, {"C", "B"}]

    # 30 operações disparadas simultaneamente; no máximo 3 executam por vez.
    tasks =
      for i <- 1..30 do
        {from, to} = Enum.at(pairs, rem(i, length(pairs)))

        Task.async(fn ->
          Transactions.transfer(from, to, 10 * i, latency: 100)
        end)
      end

    results = Task.await_many(tasks, :infinity)

    IO.inspect(Enum.frequencies_by(results, &elem(&1, 0)), label: "Resultados")
    IO.inspect(Transactions.balances(), label: "Saldos finais")
    IO.inspect(length(Transactions.history()), label: "Transações efetivadas")

    Supervisor.stop(sup)
  end
end

Finance.Demo.run()
