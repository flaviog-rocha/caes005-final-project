# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Detectar encerramentos inesperados de processos.
# 4. Garantir a consistência dos dados durante operações concorrentes.
# 5. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de votação online com controle de operações, expiração e supervisão de processos.
#
# Uso básico:
#
#     {:ok, _pid} = VotingSystem.start_link(ttl_ms: 60_000, timeout_ms: 5_000)
#
#     VotingSystem.vote("eleicao-1", "joao", "sim")
#     # => {:ok, :registered}
#
#     VotingSystem.vote("eleicao-1", "joao", "nao")
#     # => {:error, {:duplicate, :completed}}
#
#     VotingSystem.tally("eleicao-1")
#     # => %{"sim" => 1}
#
#     VotingSystem.running()
#     # => lista das operações em andamento
#
#     VotingSystem.status({:vote, "eleicao-1", "joao"})
#     # => {:ok, %{op_id: ..., status: :completed, ...}}

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação online.

  Requisitos atendidos:

    1. Idempotência: uma operação com o mesmo `op_id` não é executada novamente
       enquanto estiver em andamento ou enquanto seu registro não expirar.
    2. Consulta: `status/2` e `running/1` expõem o estado das operações.
    3. Detecção de falhas: cada operação é executada em um `Task` monitorado.
       Encerramentos inesperados geram o status `:crashed`.
    4. Consistência: o estado das operações é alterado apenas pelo GenServer,
       serializando as verificações e as inserções. Os votos são contabilizados
       com `:ets.update_counter/4`, que é atômico.
    5. Expiração: operações concluídas expiram após `ttl_ms`. Operações que
       excedem `timeout_ms` são encerradas com status `:timed_out`.
  """

  use GenServer

  @default_timeout_ms 5_000
  @default_ttl_ms 60_000
  @sweep_interval_ms 500
  @default_wait_ms 10_000
  @tally_table :voting_tallies

  ## API pública

  @doc "Inicia o servidor de operações."
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Executa `fun` no máximo uma vez para o `op_id` informado e aguarda o resultado.

  Retorna `{:ok, valor}` ou `{:error, motivo}`. Se a operação já foi executada
  (ou está em execução), retorna `{:error, {:duplicate, status}}` ou
  `{:error, :already_running}`.
  """
  def execute(op_id, fun, opts \\ []) when is_function(fun, 0) do
    server = Keyword.get(opts, :server, __MODULE__)
    wait = Keyword.get(opts, :wait_timeout, @default_wait_ms)

    case GenServer.call(server, {:start, op_id, fun, self()}) do
      :ok ->
        receive do
          {__MODULE__, :result, ^op_id, result} -> result
        after
          wait -> {:error, :wait_timeout}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Consulta o estado atual de uma operação."
  def status(op_id, server \\ __MODULE__) do
    GenServer.call(server, {:status, op_id})
  end

  @doc "Lista as operações em andamento."
  def running(server \\ __MODULE__) do
    GenServer.call(server, :running)
  end

  @doc """
  Registra um voto. A operação é identificada por `{:vote, poll_id, voter_id}`,
  o que garante que cada eleitor vote apenas uma vez por enquete.
  """
  def vote(poll_id, voter_id, option, opts \\ []) do
    op_id = {:vote, poll_id, voter_id}

    execute(
      op_id,
      fn ->
        # Atualização atômica do contador. Se a chave não existir, é criada com 0.
        :ets.update_counter(@tally_table, {poll_id, option}, {2, 1}, {{poll_id, option}, 0})
        :registered
      end,
      opts
    )
  end

  @doc "Retorna a contagem de votos por opção para a enquete informada."
  def tally(poll_id) do
    @tally_table
    |> :ets.match_object({{poll_id, :_}, :_})
    |> Map.new(fn {{_poll, option}, count} -> {option, count} end)
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    # Tabela pública e nomeada para as contagens. Escritas são feitas pelas tasks,
    # e a atomicidade vem de :ets.update_counter/4.
    :ets.new(@tally_table, [:named_table, :public, :set, write_concurrency: true])

    state = %{
      # op_id => entrada com status, pid, ref do monitor, horários, resultado e waiters
      ops: %{},
      # ref do monitor => op_id (para tratar mensagens :DOWN)
      refs: %{},
      timeout_ms: Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      ttl_ms: Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    }

    schedule_sweep()
    {:ok, state}
  end

  @impl true
  def handle_call({:start, op_id, fun, caller}, _from, state) do
    # A verificação e a inserção ocorrem dentro do mesmo callback do GenServer,
    # portanto não há condição de corrida entre duas chamadas concorrentes.
    case Map.fetch(state.ops, op_id) do
      :error ->
        start_operation(state, op_id, fun, caller)

      {:ok, %{status: :running}} ->
        {:reply, {:error, :already_running}, state}

      {:ok, %{status: status}} ->
        {:reply, {:error, {:duplicate, status}}, state}
    end
  end

  def handle_call({:status, op_id}, _from, state) do
    reply =
      case Map.fetch(state.ops, op_id) do
        {:ok, entry} -> {:ok, public_info(op_id, entry)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:running, _from, state) do
    list = for {op_id, %{status: :running} = entry} <- state.ops, do: public_info(op_id, entry)
    {:reply, list, state}
  end

  @impl true
  def handle_info({:operation_done, op_id, pid, result}, state) do
    # Mensagem enviada pela task ao terminar. Só é aceita se a operação ainda estiver
    # em execução e se o remetente for o pid registrado. Isso descarta resultados obsoletos.
    case Map.get(state.ops, op_id) do
      %{status: :running, pid: ^pid} = entry ->
        status = if match?({:ok, _}, result), do: :completed, else: :failed
        {:noreply, finish(state, op_id, entry, status, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # Encerramento inesperado da task. Se o resultado já tiver sido recebido,
    # a operação não está mais em :running e a mensagem é ignorada.
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {op_id, refs} ->
        state = %{state | refs: refs}

        case Map.get(state.ops, op_id) do
          %{status: :running} = entry ->
            {:noreply, finish(state, op_id, entry, :crashed, {:error, {:process_down, reason}})}

          _ ->
            {:noreply, state}
        end
    end
  end

  def handle_info(:sweep, state) do
    now = now()

    state =
      Enum.reduce(state.ops, state, fn {op_id, entry}, acc ->
        cond do
          # Operação excedeu o tempo máximo: encerra e marca como expirada por timeout
          entry.status == :running and now - entry.started_at >= acc.timeout_ms ->
            finish(acc, op_id, entry, :timed_out, {:error, :timeout})

          # Registro concluído há tempo suficiente: remove (expiração automática)
          entry.status != :running and now - entry.finished_at >= acc.ttl_ms ->
            %{acc | ops: Map.delete(acc.ops, op_id)}

          true ->
            acc
        end
      end)

    schedule_sweep()
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Funções auxiliares privadas

  defp start_operation(state, op_id, fun, caller) do
    server = self()

    # Task não vinculada: uma falha no processo da operação não derruba o servidor.
    # O encerramento é detectado pelo monitor.
    {:ok, pid} =
      Task.start(fn ->
        result = run(fun)
        send(server, {:operation_done, op_id, self(), result})
      end)

    ref = Process.monitor(pid)

    entry = %{
      status: :running,
      pid: pid,
      ref: ref,
      started_at: now(),
      finished_at: nil,
      result: nil,
      waiters: [caller]
    }

    state = %{
      state
      | ops: Map.put(state.ops, op_id, entry),
        refs: Map.put(state.refs, ref, op_id)
    }

    {:reply, :ok, state}
  end

  # Executa a função isolando exceções e saídas, para classificar o resultado.
  defp run(fun) do
    {:ok, fun.()}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Finaliza uma operação: remove monitor, encerra a task se necessário,
  # notifica os interessados e atualiza o registro.
  defp finish(state, op_id, entry, status, result) do
    Process.demonitor(entry.ref, [:flush])

    if status == :timed_out, do: Process.exit(entry.pid, :kill)

    Enum.each(entry.waiters, &send(&1, {__MODULE__, :result, op_id, result}))

    done = %{
      entry
      | status: status,
        result: result,
        finished_at: now(),
        pid: nil,
        ref: nil,
        waiters: []
    }

    %{
      state
      | ops: Map.put(state.ops, op_id, done),
        refs: Map.delete(state.refs, entry.ref)
    }
  end

  defp public_info(op_id, entry) do
    %{
      op_id: op_id,
      status: entry.status,
      pid: entry.pid,
      started_at: entry.started_at,
      finished_at: entry.finished_at,
      result: entry.result
    }
  end

  defp schedule_sweep do
    Process.send_after(self(), :sweep, @sweep_interval_ms)
  end

  defp now, do: System.monotonic_time(:millisecond)
end
