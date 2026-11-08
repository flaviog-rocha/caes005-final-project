# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Executar determinadas operações periodicamente.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# fin_tx.exs
# Execução: elixir fin_tx.exs
# Gerenciamento de transações financeiras usando apenas a biblioteca padrão do Elixir,
# com Registry para identificação de operações e agendamentos.

defmodule FinTx do
  @moduledoc """
  Ponto de entrada do sistema. Inicia a árvore de supervisão composta por:
    * `Registry` (`FinTx.Registry`): identifica operações e agendamentos por chave.
    * `FinTx.Store`: armazenamento CRUD das transações em ETS.
    * `DynamicSupervisor` (`FinTx.OpSupervisor`): supervisiona operações e agendamentos
      executados concorrentemente.
  """

  @registry FinTx.Registry
  @op_supervisor FinTx.OpSupervisor

  def start do
    children = [
      {Registry, keys: :unique, name: @registry},
      FinTx.Store,
      {DynamicSupervisor, strategy: :one_for_one, name: @op_supervisor}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: FinTx.Supervisor)
  end
end

defmodule FinTx.Transaction do
  @moduledoc """
  Estrutura de uma transação financeira.
  Valores monetários são inteiros representando centavos, evitando erros de ponto flutuante.
  """

  @enforce_keys [:id, :from, :to, :amount, :status]
  defstruct [:id, :from, :to, :amount, :status, :inserted_at, :updated_at]

  @statuses [:pending, :settled, :failed]

  @doc "Constrói e valida uma transação a partir de um mapa de atributos."
  def build(attrs) when is_map(attrs) do
    now = DateTime.utc_now()

    tx = %__MODULE__{
      id: Map.get(attrs, :id) || generate_id(),
      from: Map.get(attrs, :from),
      to: Map.get(attrs, :to),
      amount: Map.get(attrs, :amount),
      status: Map.get(attrs, :status, :pending),
      inserted_at: now,
      updated_at: now
    }

    validate(tx)
  end

  @doc "Valida os campos obrigatórios e o domínio de valores."
  def validate(%__MODULE__{amount: amount, status: status, from: from, to: to} = tx)
      when is_integer(amount) and amount > 0 and status in @statuses and
             not is_nil(from) and not is_nil(to),
      do: {:ok, tx}

  def validate(_), do: {:error, :invalid_transaction}

  defp generate_id, do: "tx_" <> Integer.to_string(System.unique_integer([:positive]))
end

defmodule FinTx.Store do
  @moduledoc """
  Armazenamento de transações em ETS.

  * Escritas são serializadas pelo GenServer (garante consistência, ex.: unicidade no create).
  * Leituras acontecem diretamente na ETS, sem passar pelo processo, permitindo
    acesso concorrente.
  * `stream/2` percorre a tabela em blocos via continuations de `:ets.select/3`,
    sem carregar todos os registros na memória.
  """

  use GenServer
  alias FinTx.Transaction

  @table :fin_tx_transactions
  @default_chunk 500

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  # ---------------------------------------------------------------------------
  # API CRUD
  # ---------------------------------------------------------------------------

  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})

  def update(id, changes), do: GenServer.call(__MODULE__, {:update, id, changes})

  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  @doc "Consulta uma transação pelo id (leitura direta na ETS)."
  def get(id) do
    case :ets.lookup(@table, id) do
      [{_id, _status, tx}] -> {:ok, tx}
      [] -> {:error, :not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Streaming: processamento de grandes volumes sem carregar tudo em memória
  # ---------------------------------------------------------------------------

  @doc """
  Retorna um `Stream` lazy de transações filtradas por status (ou `:any`).
  Cada bloco de `chunk_size` registros é lido sob demanda.
  """
  def stream(status \\ :any, chunk_size \\ @default_chunk) do
    pattern = if status == :any, do: :_, else: status
    spec = [{{:_, pattern, :_}, [], [:"$_"]}]

    Stream.resource(
      fn -> :ets.select(@table, spec, chunk_size) end,
      fn
        :"$end_of_table" ->
          {:halt, nil}

        {rows, continuation} ->
          txs = Enum.map(rows, fn {_id, _status, tx} -> tx end)
          {txs, :ets.select(continuation)}
      end,
      fn _ -> :ok end
    )
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(_) do
    # Tabela protegida: somente este processo escreve; qualquer processo lê.
    :ets.new(@table, [:set, :protected, :named_table, read_concurrency: true])
    {:ok, nil}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, tx} <- Transaction.build(attrs),
         false <- :ets.member(@table, tx.id) do
      :ets.insert(@table, {tx.id, tx.status, tx})
      {:reply, {:ok, tx}, state}
    else
      true -> {:reply, {:error, :already_exists}, state}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:update, id, changes}, _from, state) do
    case :ets.lookup(@table, id) do
      [{^id, _status, tx}] ->
        changes =
          changes
          |> Map.drop([:id, :inserted_at])
          |> Map.put(:updated_at, DateTime.utc_now())

        updated = struct(tx, changes)

        case Transaction.validate(updated) do
          {:ok, updated} ->
            :ets.insert(@table, {id, updated.status, updated})
            {:reply, {:ok, updated}, state}

          error ->
            {:reply, error, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    reply =
      if :ets.member(@table, id) do
        :ets.delete(@table, id)
        :ok
      else
        {:error, :not_found}
      end

    {:reply, reply, state}
  end
end

defmodule FinTx.Operation do
  @moduledoc """
  Processo que executa uma operação em lote sobre as transações.

  * Registrado no `Registry` com a chave `{:operation, id}`.
  * O trabalho roda em uma `Task` separada, para que o GenServer continue
    respondendo consultas de estado enquanto processa.
  * Os registros são lidos em blocos via `FinTx.Store.stream/2` e processados
    concorrentemente com `Task.async_stream/3`.
  """

  use GenServer, restart: :temporary

  @chunk_size 500
  # Tempo que a operação permanece disponível para consulta após finalizar.
  @retention_ms 300_000

  def start_link({id, job}) do
    GenServer.start_link(__MODULE__, {id, job},
      name: {:via, Registry, {FinTx.Registry, {:operation, id}}}
    )
  end

  @doc "Consulta o estado atual da operação."
  def status(pid), do: GenServer.call(pid, :status)

  @impl true
  def init({id, job}) do
    # Permite receber mensagens de saída da Task sem morrer junto.
    Process.flag(:trap_exit, true)

    state = %{
      id: id,
      name: job.name,
      job: job,
      status: :running,
      processed: 0,
      failed: 0,
      started_at: DateTime.utc_now(),
      finished_at: nil,
      task: nil
    }

    {:ok, state, {:continue, :run}}
  end

  @impl true
  def handle_continue(:run, %{job: job} = state) do
    parent = self()
    task = Task.async(fn -> run_pipeline(job, parent) end)
    {:noreply, %{state | task: task}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    reply = %{
      id: state.id,
      name: state.name,
      status: state.status,
      processed: state.processed,
      failed: state.failed,
      started_at: state.started_at,
      finished_at: state.finished_at
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:progress, ok, err}, state) do
    {:noreply, %{state | processed: state.processed + ok, failed: state.failed + err}}
  end

  def handle_info({ref, _result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    finish(state, :completed)
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{task: %Task{ref: ref}} = state) do
    finish(state, :failed)
  end

  def handle_info(:retire, state), do: {:stop, :normal, state}

  # Mensagens de saída de tasks finalizadas e demais mensagens são ignoradas.
  def handle_info(_msg, state), do: {:noreply, state}

  defp finish(state, status) do
    Process.send_after(self(), :retire, @retention_ms)

    {:noreply, %{state | status: status, finished_at: DateTime.utc_now(), task: nil}}
  end

  # ---------------------------------------------------------------------------
  # Pipeline de processamento
  # ---------------------------------------------------------------------------

  defp run_pipeline(job, parent) do
    concurrency = Map.get(job, :concurrency, System.schedulers_online())
    status_filter = Map.get(job, :status, :any)

    FinTx.Store.stream(status_filter, @chunk_size)
    |> Stream.chunk_every(@chunk_size)
    |> Stream.each(fn chunk ->
      {ok, err} =
        chunk
        |> Task.async_stream(&safe_apply(job.fun, &1),
          max_concurrency: concurrency,
          timeout: :infinity,
          ordered: false
        )
        |> Enum.reduce({0, 0}, fn
          {:ok, :ok}, {o, e} -> {o + 1, e}
          _, {o, e} -> {o, e + 1}
        end)

      send(parent, {:progress, ok, err})
    end)
    |> Stream.run()
  end

  defp safe_apply(fun, tx) do
    fun.(tx)
  rescue
    e -> {:error, e}
  end
end

defmodule FinTx.Operations do
  @moduledoc "API pública para iniciar e consultar operações em andamento."

  @registry FinTx.Registry
  @op_supervisor FinTx.OpSupervisor

  @doc """
  Inicia uma operação. O `job` é um mapa com:
    * `:name` - nome da operação (obrigatório)
    * `:fun` - função aplicada a cada transação, deve retornar `:ok` ou `{:error, _}`
    * `:status` - filtra as transações por status (padrão `:any`)
    * `:concurrency` - número de processos concorrentes (padrão: schedulers online)
  """
  def start(job) do
    id = System.unique_integer([:positive, :monotonic])

    case DynamicSupervisor.start_child(@op_supervisor, {FinTx.Operation, {id, job}}) do
      {:ok, _pid} -> {:ok, id}
      error -> error
    end
  end

  @doc "Consulta o estado atual de uma operação pelo id."
  def status(id) do
    with [{pid, _}] <- Registry.lookup(@registry, {:operation, id}) do
      {:ok, FinTx.Operation.status(pid)}
    else
      _ -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  @doc "Lista os ids de todas as operações registradas."
  def registered_ids do
    Registry.select(@registry, [{{{:operation, :"$1"}, :_, :_}, [], [:"$1"]}])
  end

  @doc "Retorna o estado de todas as operações atualmente em execução."
  def in_progress do
    for id <- registered_ids(),
        {:ok, %{status: :running} = state} <- [status(id)],
        do: state
  end
end

defmodule FinTx.Scheduler do
  @moduledoc """
  Executa periodicamente uma operação.
  Registrado no `Registry` com a chave `{:schedule, name}`.
  Se a execução anterior ainda estiver em andamento, o disparo é ignorado
  para evitar sobreposição.
  """

  use GenServer, restart: :temporary

  @registry FinTx.Registry
  @op_supervisor FinTx.OpSupervisor

  def start_link({name, interval, job}) do
    GenServer.start_link(__MODULE__, {interval, job},
      name: {:via, Registry, {@registry, {:schedule, name}}}
    )
  end

  @doc "Cria um agendamento que executa `job` a cada `interval_ms` milissegundos."
  def every(name, interval_ms, job) do
    DynamicSupervisor.start_child(@op_supervisor, {__MODULE__, {name, interval_ms, job}})
  end

  @doc "Cancela um agendamento pelo nome."
  def cancel(name) do
    case Registry.lookup(@registry, {:schedule, name}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(@op_supervisor, pid)
      [] -> {:error, :not_found}
    end
  end

  @impl true
  def init({interval, job}) do
    Process.send_after(self(), :tick, interval)
    {:ok, %{interval: interval, job: job, last_op: nil}}
  end

  @impl true
  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, state.interval)

    state =
      if running?(state.last_op) do
        state
      else
        %{state | last_op: start_run(state.job)}
      end

    {:noreply, state}
  end

  defp start_run(job) do
    case FinTx.Operations.start(job) do
      {:ok, id} -> id
      _ -> nil
    end
  end

  defp running?(nil), do: false
  defp running?(id), do: Registry.lookup(@registry, {:operation, id}) != []
end

defmodule FinTx.Demo do
  @moduledoc "Demonstração de uso do sistema."

  alias FinTx.{Store, Operations, Scheduler}

  def run do
    {:ok, _sup} = FinTx.start()

    # Cadastro em massa
    for i <- 1..20_000 do
      {:ok, _} = Store.create(%{from: "C#{rem(i, 100)}", to: "C#{rem(i + 1, 100)}", amount: i})
    end

    # Operação concorrente: liquida transações pendentes
    settle = %{
      name: "settle",
      status: :pending,
      concurrency: 8,
      fun: fn tx ->
        case Store.update(tx.id, %{status: :settled}) do
          {:ok, _} -> :ok
          error -> error
        end
      end
    }

    {:ok, id} = Operations.start(settle)
    IO.inspect(Operations.status(id), label: "status inicial")

    # Operação periódica: auditoria a cada 500ms
    audit = %{
      name: "audit",
      status: :settled,
      concurrency: 4,
      fun: fn _tx -> :ok end
    }

    {:ok, _} = Scheduler.every("audit", 500, audit)

    IO.inspect(in_progress_count(), label: "operações em andamento")
    IO.inspect(wait_for(id), label: "resultado settle")

    Scheduler.cancel("audit")

    IO.inspect(Store.stream(:settled) |> Enum.count(), label: "transações liquidadas")
    IO.inspect(Store.stream(:pending) |> Enum.count(), label: "transações pendentes")
  end

  defp in_progress_count, do: length(Operations.in_progress())

  defp wait_for(id) do
    case Operations.status(id) do
      {:ok, %{status: :running}} ->
        Process.sleep(100)
        wait_for(id)

      other ->
        other
    end
  end
end

FinTx.Demo.run()
