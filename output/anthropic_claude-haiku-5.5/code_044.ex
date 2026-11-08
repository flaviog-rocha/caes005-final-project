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

# Arquivo único: gerenciamento de transações financeiras com concorrência limitada.
#
# Componentes:
#   * Finance.Bank              - banco de dados fictício em memória (Agent).
#   * Finance.TransactionWorker - processo efêmero que executa uma transação.
#   * Finance.Scheduler         - controla o limite de operações simultâneas e a fila.
#   * Finance                   - API pública e árvore de supervisão.
#
# Uso (exemplo, em IEx):
#   {:ok, _} = Finance.start_link(accounts: %{1 => 1_000, 2 => 500}, max_concurrency: 3)
#   Finance.transfer(1, 2, 100)
#   Finance.transfer(1, 2, 50, latency: 200)   # latência simulada em ms
#   Finance.balance(1)
#   Finance.stats()
#
# Persistência com Ecto (opcional, não obrigatória):
#   Substituir Finance.Bank por um Ecto.Repo, envolvendo cada transferência em
#   Repo.transaction/1 com bloqueio pessimista (lock: "FOR UPDATE") nas contas.
#   Migration equivalente:
#     create table(:accounts) do
#       add :balance, :integer, null: false, default: 0
#     end
#     create table(:transactions) do
#       add :from_id, references(:accounts), null: false
#       add :to_id, references(:accounts), null: false
#       add :amount, :integer, null: false
#       timestamps()
#     end

defmodule Finance.Bank do
  # Banco de dados fictício. O Agent serializa todas as operações, então cada
  # transferência é atômica: validação e atualização acontecem em uma única
  # função executada dentro do processo do Agent.
  use Agent

  @doc false
  def start_link(initial_accounts) do
    Agent.start_link(
      fn -> %{accounts: initial_accounts, log: []} end,
      name: __MODULE__
    )
  end

  @doc "Retorna {:ok, saldo} ou :error se a conta não existir."
  def balance(account_id) do
    Agent.get(__MODULE__, fn %{accounts: accounts} -> Map.fetch(accounts, account_id) end)
  end

  @doc "Executa uma transferência atômica entre duas contas."
  def transfer(from, to, amount) do
    Agent.get_and_update(__MODULE__, fn state -> apply_transfer(state, from, to, amount) end)
  end

  @doc "Retorna o histórico de transações (mais recentes primeiro)."
  def history do
    Agent.get(__MODULE__, fn %{log: log} -> log end)
  end

  # Regras de negócio executadas dentro do Agent (retorno: {resposta, novo_estado}).
  defp apply_transfer(state, from, to, _amount) when from == to do
    {{:error, :same_account}, state}
  end

  defp apply_transfer(%{accounts: accounts} = state, from, to, amount)
       when is_integer(amount) and amount > 0 do
    with {:ok, from_balance} <- fetch_account(accounts, from),
         {:ok, to_balance} <- fetch_account(accounts, to),
         :ok <- check_funds(from_balance, amount) do
      tx = %{
        id: System.unique_integer([:positive, :monotonic]),
        from: from,
        to: to,
        amount: amount,
        at: DateTime.utc_now()
      }

      new_accounts =
        accounts
        |> Map.put(from, from_balance - amount)
        |> Map.put(to, to_balance + amount)

      {{:ok, tx}, %{state | accounts: new_accounts, log: [tx | state.log]}}
    else
      {:error, _reason} = error -> {error, state}
    end
  end

  defp apply_transfer(state, _from, _to, _amount) do
    {{:error, :invalid_amount}, state}
  end

  defp fetch_account(accounts, id) do
    case Map.fetch(accounts, id) do
      {:ok, balance} -> {:ok, balance}
      :error -> {:error, {:account_not_found, id}}
    end
  end

  defp check_funds(balance, amount) when balance >= amount, do: :ok
  defp check_funds(_balance, _amount), do: {:error, :insufficient_funds}
end

defmodule Finance.TransactionWorker do
  # Processo efêmero que executa uma única transação.
  # restart: :temporary -> o DynamicSupervisor não o reinicia após o término,
  # pois o Scheduler é responsável por reagir a falhas e liberar a vaga.
  use GenServer, restart: :temporary

  @doc false
  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    # Trabalho pesado fora do init/1 para não bloquear o supervisor.
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{scheduler: scheduler, from: from, to: to, amount: amount} = state) do
    if state.latency > 0, do: Process.sleep(state.latency)

    result = Finance.Bank.transfer(from, to, amount)

    # Entrega o resultado ao Scheduler antes de encerrar.
    send(scheduler, {:transaction_result, self(), result})

    {:stop, :normal, state}
  end
end

defmodule Finance.Scheduler do
  # Controla a quantidade de operações simultâneas (semáforo com fila FIFO).
  # Quando há vaga, inicia um worker via DynamicSupervisor; caso contrário,
  # enfileira a requisição do chamador, que fica bloqueado até o resultado.
  use GenServer

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    state = %{
      limit: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      supervisor: Keyword.get(opts, :supervisor, Finance.WorkerSupervisor),
      # Mapa: pid do worker => {caller_from, monitor_ref}
      active: %{},
      # Fila de {job, caller_from}
      queue: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:submit, job}, from, state) do
    if map_size(state.active) < state.limit do
      {:noreply, start_job(state, job, from)}
    else
      {:noreply, %{state | queue: :queue.in({job, from}, state.queue)}}
    end
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      limit: state.limit,
      active: map_size(state.active),
      queued: :queue.len(state.queue)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info({:transaction_result, pid, result}, state) do
    case Map.pop(state.active, pid) do
      {{from, ref}, active} ->
        Process.demonitor(ref, [:flush])
        GenServer.reply(from, result)
        {:noreply, dequeue(%{state | active: active})}

      {nil, _} ->
        {:noreply, state}
    end
  end

  # Worker terminou sem enviar resultado (falha inesperada).
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.active, pid) do
      {{from, ^ref}, active} ->
        GenServer.reply(from, {:error, {:worker_crashed, reason}})
        {:noreply, dequeue(%{state | active: active})}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Inicia um worker sob o DynamicSupervisor e registra o chamador.
  defp start_job(state, job, from) do
    args = Map.put(job, :scheduler, self())

    case DynamicSupervisor.start_child(state.supervisor, {Finance.TransactionWorker, args}) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        %{state | active: Map.put(state.active, pid, {from, ref})}

      {:error, reason} ->
        GenServer.reply(from, {:error, {:worker_start_failed, reason}})
        state
    end
  end

  # Preenche vagas livres a partir da fila.
  defp dequeue(state) do
    if map_size(state.active) < state.limit do
      case :queue.out(state.queue) do
        {{:value, {job, from}}, rest} ->
          dequeue(start_job(%{state | queue: rest}, job, from))

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule Finance do
  # API pública e árvore de supervisão do sistema.
  #
  # Estrutura:
  #   Finance.Supervisor (one_for_one)
  #     ├── Finance.Bank              (Agent - banco fictício)
  #     ├── Finance.WorkerSupervisor  (DynamicSupervisor - workers)
  #     └── Finance.Scheduler         (limita concorrência)

  @doc """
  Inicia o sistema.

  Opções:
    * `:accounts`        - mapa %{id => saldo_inicial}
    * `:max_concurrency` - máximo de transações executando simultaneamente
  """
  def start_link(opts \\ []) do
    children = [
      {Finance.Bank, Keyword.get(opts, :accounts, %{})},
      {DynamicSupervisor, name: Finance.WorkerSupervisor, strategy: :one_for_one},
      {Finance.Scheduler,
       max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
       supervisor: Finance.WorkerSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Finance.Supervisor)
  end

  @doc """
  Solicita uma transferência. Bloqueia o chamador até o resultado, mas
  várias chamadas podem ser feitas em paralelo (ex.: Task.async_stream/3).

  Opções:
    * `:latency` - atraso simulado em ms (padrão 0)
  """
  def transfer(from, to, amount, opts \\ []) do
    job = %{
      from: from,
      to: to,
      amount: amount,
      latency: Keyword.get(opts, :latency, 0)
    }

    GenServer.call(Finance.Scheduler, {:submit, job}, :infinity)
  end

  @doc "Consulta o saldo de uma conta."
  def balance(account_id), do: Finance.Bank.balance(account_id)

  @doc "Histórico de transações concluídas."
  def history, do: Finance.Bank.history()

  @doc "Estatísticas de concorrência: limite, ativas e enfileiradas."
  def stats, do: GenServer.call(Finance.Scheduler, :stats)
end
