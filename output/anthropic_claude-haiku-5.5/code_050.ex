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

defmodule VotingSystem do
  @moduledoc """
  Sistema de votação online idempotente e concorrente.

  Cada voto é uma operação identificada por um `op_id` (chave de idempotência).
  Fluxo:

    * Uma operação com o mesmo `op_id` nunca é executada duas vezes: enquanto
      estiver em andamento, novas submissões recebem `{:error, :in_progress}`;
      após finalizada, recebem o resultado original.
    * A verificação do voto roda em uma `Task` supervisionada (`Task.Supervisor`),
      sem bloquear o servidor.
    * A gravação do voto acontece somente dentro do GenServer, que serializa as
      mutações de estado. Isso garante consistência mesmo com submissões
      concorrentes (ex.: um eleitor não consegue votar duas vezes na mesma
      votação, mesmo com `op_id`s diferentes).
    * Falhas das tasks são detectadas via monitor (`:DOWN`) e reportadas como
      `{:error, {:crashed, reason}}`. Tasks que excedem o tempo limite são
      encerradas e marcadas como `:timed_out`.
    * Registros finalizados expiram após um TTL, removidos por uma limpeza
      periódica (ou sob demanda na consulta).

  ## Exemplo de uso

      {:ok, _pid} = VotingSystem.start_link(ttl_ms: 60_000)

      VotingSystem.vote("op-1", "eleicao-42", "eleitor-7", "opcao-A")
      #=> {:ok, :vote_recorded}

      VotingSystem.vote("op-1", "eleicao-42", "eleitor-7", "opcao-A")
      #=> {:ok, :vote_recorded}   (resultado original, sem reexecutar)

      VotingSystem.vote("op-2", "eleicao-42", "eleitor-7", "opcao-B")
      #=> {:error, :already_voted}

      VotingSystem.status("op-1")
      VotingSystem.running()
      VotingSystem.tally("eleicao-42")
  """

  use GenServer

  # Tempo de vida de registros finalizados (idempotência e consulta).
  @ttl_ms 60_000
  # Intervalo da limpeza periódica de registros expirados.
  @cleanup_interval_ms 5_000
  # Tempo máximo de execução de uma task de verificação.
  @task_timeout_ms 10_000
  # Timeout da chamada síncrona ao GenServer (deve exceder o timeout da task).
  @call_timeout_ms (@task_timeout_ms + 2_000)

  defstruct ops: %{},
            # Mapa %{task_ref => op_id} para correlacionar mensagens das tasks.
            refs: %{},
            # Votos efetivamente gravados: %{{poll_id, voter_id} => option}.
            votes: %{},
            sup: nil,
            ttl_ms: @ttl_ms

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o sistema. Opções: `:name` (padrão `__MODULE__`) e `:ttl_ms`."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Submete um voto idempotente. Bloqueia até o resultado final.

  Retorna `{:ok, :vote_recorded}`, `{:error, :already_voted}`,
  `{:error, :in_progress}`, `{:error, :timeout}` ou `{:error, {:crashed, reason}}`.
  """
  def vote(server \\ __MODULE__, op_id, poll_id, voter_id, option) do
    GenServer.call(server, {:vote, op_id, poll_id, voter_id, option}, @call_timeout_ms)
  end

  @doc "Consulta o estado de uma operação pelo `op_id`."
  def status(server \\ __MODULE__, op_id) do
    GenServer.call(server, {:status, op_id})
  end

  @doc "Lista as operações atualmente em andamento."
  def running(server \\ __MODULE__) do
    GenServer.call(server, :running)
  end

  @doc "Contagem de votos por opção para uma votação."
  def tally(server \\ __MODULE__, poll_id) do
    GenServer.call(server, {:tally, poll_id})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    {:ok, sup} = Task.Supervisor.start_link()

    state = %__MODULE__{
      sup: sup,
      ttl_ms: Keyword.get(opts, :ttl_ms, @ttl_ms)
    }

    schedule(:cleanup, @cleanup_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:vote, op_id, poll_id, voter_id, option}, from, state) do
    request = %{poll_id: poll_id, voter_id: voter_id, option: option}

    case fetch_active(state, op_id) do
      # Operação nova: dispara a verificação em uma Task.
      nil ->
        start_operation(op_id, request, from, state)

      # Mesma operação já em andamento: não reexecuta.
      %{status: :running} ->
        {:reply, {:error, :in_progress}, state}

      # Operação finalizada: devolve o resultado original.
      %{result: result} ->
        {:reply, result, state}
    end
  end

  def handle_call({:status, op_id}, _from, state) do
    case fetch_active(state, op_id) do
      nil -> {:reply, {:error, :not_found}, state}
      op -> {:reply, {:ok, public_view(op)}, state}
    end
  end

  def handle_call(:running, _from, state) do
    running =
      for {op_id, %{status: :running} = op} <- state.ops do
        %{op_id: op_id, started_at: op.started_at}
      end

    {:reply, running, state}
  end

  def handle_call({:tally, poll_id}, _from, state) do
    tally =
      state.votes
      |> Enum.filter(fn {{pid, _voter}, _option} -> pid == poll_id end)
      |> Enum.frequencies_by(fn {_key, option} -> option end)

    {:reply, tally, state}
  end

  @impl true
  # Timeout de uma task em execução: encerra a task e finaliza a operação.
  def handle_info({:task_timeout, ref}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, op_id} ->
        op = Map.fetch!(state.ops, op_id)
        Task.Supervisor.terminate_child(state.sup, op.task_pid)
        {:noreply, finish(state, ref, op_id, :timed_out, {:error, :timeout})}

      :error ->
        {:noreply, state}
    end
  end

  # Resultado de uma task concluída com sucesso: grava o voto de forma atômica.
  def handle_info({ref, task_result}, state) when is_reference(ref) do
    case Map.fetch(state.refs, ref) do
      {:ok, op_id} ->
        {reply, votes} = commit(task_result, state.votes)
        state = %{state | votes: votes}
        {:noreply, finish(state, ref, op_id, :completed, reply)}

      :error ->
        {:noreply, state}
    end
  end

  # Encerramento inesperado de uma task (monitor).
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.refs, ref) do
      {:ok, op_id} ->
        {:noreply, finish(state, ref, op_id, :crashed, {:error, {:crashed, reason}})}

      :error ->
        {:noreply, state}
    end
  end

  # Limpeza periódica de registros finalizados e expirados.
  def handle_info(:cleanup, state) do
    now = now_ms()

    ops =
      for {op_id, op} <- state.ops, not expired_record?(op, now), into: %{} do
        {op_id, op}
      end

    schedule(:cleanup, @cleanup_interval_ms)
    {:noreply, %{state | ops: ops}}
  end

  def handle_info(_unexpected, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções auxiliares privadas
  # ---------------------------------------------------------------------------

  defp start_operation(op_id, request, from, state) do
    # A task apenas verifica o voto (sem efeitos colaterais no estado).
    task = Task.Supervisor.async_nolink(state.sup, fn -> verify(request) end)
    timer = Process.send_after(self(), {:task_timeout, task.ref}, @task_timeout_ms)

    op = %{
      status: :running,
      from: from,
      task_pid: task.pid,
      task_ref: task.ref,
      timer: timer,
      started_at: now_ms(),
      finished_at: nil,
      expires_at: nil,
      result: nil
    }

    state = %{
      state
      | ops: Map.put(state.ops, op_id, op),
        refs: Map.put(state.refs, task.ref, op_id)
    }

    {:noreply, state}
  end

  # Verificação do voto executada dentro da Task (validações e checagens externas).
  defp verify(%{poll_id: poll_id, voter_id: voter_id, option: option}) do
    # Simula latência de uma checagem externa (ex.: elegibilidade).
    Process.sleep(Enum.random(50..200))

    cond do
      not (is_binary(poll_id) and poll_id != "") -> {:error, :invalid_poll}
      not (is_binary(voter_id) and voter_id != "") -> {:error, :invalid_voter}
      not (is_binary(option) and option != "") -> {:error, :invalid_option}
      true -> {:ok, {poll_id, voter_id, option}}
    end
  end

  # Aplicação do resultado da verificação ao estado de votos (somente no GenServer).
  defp commit({:ok, {poll_id, voter_id, option}}, votes) do
    key = {poll_id, voter_id}

    if Map.has_key?(votes, key) do
      {{:error, :already_voted}, votes}
    else
      {{:ok, :vote_recorded}, Map.put(votes, key, option)}
    end
  end

  defp commit({:error, reason}, votes), do: {{:error, reason}, votes}

  # Finaliza uma operação: cancela timer, remove monitor, responde ao chamador
  # e registra o resultado com TTL.
  defp finish(state, ref, op_id, status, reply) do
    Process.demonitor(ref, [:flush])

    op = Map.fetch!(state.ops, op_id)

    if op.timer, do: Process.cancel_timer(op.timer)
    if op.from, do: GenServer.reply(op.from, reply)

    now = now_ms()

    finished = %{
      op
      | status: status,
        result: reply,
        finished_at: now,
        expires_at: now + state.ttl_ms,
        from: nil,
        task_pid: nil,
        task_ref: nil,
        timer: nil
    }

    %{
      state
      | ops: Map.put(state.ops, op_id, finished),
        refs: Map.delete(state.refs, ref)
    }
  end

  # Retorna a operação somente se ainda válida (não expirada).
  defp fetch_active(state, op_id) do
    case Map.get(state.ops, op_id) do
      nil ->
        nil

      op ->
        if expired_record?(op, now_ms()), do: nil, else: op
    end
  end

  defp expired_record?(%{expires_at: expires_at}, now) when is_integer(expires_at),
    do: expires_at <= now

  defp expired_record?(_op, _now), do: false

  defp public_view(op) do
    Map.take(op, [:status, :started_at, :finished_at, :expires_at, :result])
  end

  defp schedule(message, interval) do
    Process.send_after(self(), message, interval)
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
