# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Implementar expiração automática de registros.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Monitor de sensores concorrente e tolerante a falhas, usando apenas a biblioteca padrão.
#
# Recursos:
#   * Execução simultânea de leituras (um processo por tentativa).
#   * Limite de concorrência (max_concurrency); excedentes ficam em fila FIFO.
#   * Novas tentativas com backoff exponencial e jitter para falhas temporárias.
#   * Timeout por tentativa e detecção de crash do processo de leitura.
#   * Consulta do estado de operações individuais e das em andamento.
#   * Expiração automática de registros finalizados (varredura periódica + verificação preguiçosa).

defmodule SensorMonitor do
  use GenServer

  # Configuração padrão. Todos os valores podem ser sobrescritos em start_link/1.
  @defaults [
    # Número máximo de leituras executando ao mesmo tempo.
    max_concurrency: 4,
    # Número máximo de tentativas por operação (inclui a primeira).
    max_attempts: 3,
    # Base do backoff exponencial, em milissegundos.
    base_backoff_ms: 200,
    # Teto do backoff, em milissegundos.
    max_backoff_ms: 5_000,
    # Tempo máximo de uma tentativa antes de ser considerada timeout.
    attempt_timeout_ms: 2_000,
    # Tempo de vida de registros finalizados (:completed / :failed).
    record_ttl_ms: 60_000,
    # Intervalo da varredura de expiração.
    sweep_interval_ms: 5_000,
    # Nome opcional do processo registrado.
    name: nil
  ]

  # Erros considerados temporários e, portanto, elegíveis para nova tentativa.
  @transient_reasons [:timeout, :busy, :unavailable]

  ## API pública

  @doc "Inicia o monitor. Opções: ver @defaults e :sensor_fun (fn sensor_id -> {:ok, map} | {:error, term} end)."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, Keyword.put(opts, :name, name), gen_opts)
  end

  @doc "Enfileira a leitura de um sensor. Retorna {:ok, op_id}."
  def submit(server, sensor_id) do
    GenServer.call(server, {:submit, sensor_id})
  end

  @doc "Consulta o estado atual de uma operação. Registros expirados são tratados como inexistentes."
  def status(server, op_id) do
    GenServer.call(server, {:status, op_id})
  end

  @doc "Lista as operações em andamento (:queued, :running ou :waiting para nova tentativa)."
  def in_progress(server) do
    GenServer.call(server, :in_progress)
  end

  @doc "Leitura simulada de sensor, usada quando nenhuma :sensor_fun é fornecida."
  def default_read(sensor_id) do
    Process.sleep(:rand.uniform(300))

    case :rand.uniform(10) do
      n when n <= 2 ->
        {:error, :timeout}

      _ ->
        {:ok,
         %{
           sensor: sensor_id,
           value: Float.round(:rand.uniform() * 100, 2),
           read_at: System.system_time(:millisecond)
         }}
    end
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    cfg =
      @defaults
      |> Keyword.merge(opts)
      |> Map.new()
      |> Map.put_new(:sensor_fun, &default_read/1)

    schedule_sweep(cfg.sweep_interval_ms)

    {:ok,
     %{
       cfg: cfg,
       # op_id => registro da operação
       ops: %{},
       # fila FIFO de op_ids aguardando um slot de execução
       queue: :queue.new(),
       # op_id => %{pid, mref, attempt, timer} para tentativas em execução
       running: %{}
     }}
  end

  @impl true
  def handle_call({:submit, sensor_id}, _from, state) do
    op_id = System.unique_integer([:positive, :monotonic])

    op = %{
      id: op_id,
      sensor_id: sensor_id,
      status: :queued,
      attempt: 0,
      last_error: nil,
      result: nil,
      timer: nil,
      submitted_at: now_ms(),
      started_at: nil,
      finished_at: nil,
      expires_at: nil
    }

    state =
      %{state | ops: Map.put(state.ops, op_id, op), queue: :queue.in(op_id, state.queue)}
      |> dispatch()

    {:reply, {:ok, op_id}, state}
  end

  def handle_call({:status, op_id}, _from, state) do
    now = now_ms()

    reply =
      case Map.get(state.ops, op_id) do
        nil -> {:error, :not_found}
        op -> if expired?(op, now), do: {:error, :not_found}, else: {:ok, view(op)}
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    views =
      state.ops
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running, :waiting]))
      |> Enum.map(&view/1)

    {:reply, views, state}
  end

  # Resultado enviado por um processo de leitura.
  @impl true
  def handle_info({:attempt_result, op_id, attempt, result}, state) do
    case Map.get(state.running, op_id) do
      %{attempt: ^attempt} ->
        state =
          state
          |> release(op_id)
          |> complete_attempt(op_id, result)
          |> dispatch()

        {:noreply, state}

      # Resultado obsoleto (tentativa já encerrada por timeout ou crash).
      _ ->
        {:noreply, state}
    end
  end

  # Processo de leitura terminou sem enviar resultado (crash).
  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_id, run} -> run.mref == mref end) do
      {op_id, _run} ->
        state =
          state
          |> release(op_id)
          |> complete_attempt(op_id, {:error, {:crash, reason}})
          |> dispatch()

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  # Timeout de uma tentativa: encerra o processo e trata como falha temporária.
  def handle_info({:attempt_timeout, op_id, attempt}, state) do
    case Map.get(state.running, op_id) do
      %{attempt: ^attempt, pid: pid} ->
        Process.exit(pid, :kill)

        state =
          state
          |> release(op_id)
          |> complete_attempt(op_id, {:error, :timeout})
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # Fim do backoff: a operação volta para a fila.
  def handle_info({:retry, op_id}, state) do
    case Map.get(state.ops, op_id) do
      %{status: :waiting} ->
        state =
          state
          |> update_op(op_id, &Map.merge(&1, %{status: :queued, timer: nil}))
          |> Map.update!(:queue, &:queue.in(op_id, &1))
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # Varredura periódica de registros expirados.
  def handle_info(:sweep, state) do
    now = now_ms()

    ops =
      state.ops
      |> Enum.reject(fn {_id, op} -> expired?(op, now) end)
      |> Map.new()

    schedule_sweep(state.cfg.sweep_interval_ms)
    {:noreply, %{state | ops: ops}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Despacho e execução

  # Inicia novas tentativas enquanto houver slots livres e operações na fila.
  defp dispatch(state) do
    with true <- map_size(state.running) < state.cfg.max_concurrency,
         {{:value, op_id}, queue} <- :queue.out(state.queue) do
      state
      |> Map.put(:queue, queue)
      |> start_attempt(op_id)
      |> dispatch()
    else
      _ -> state
    end
  end

  defp start_attempt(state, op_id) do
    server = self()
    cfg = state.cfg
    op = Map.fetch!(state.ops, op_id)
    attempt = op.attempt + 1
    sensor_id = op.sensor_id

    {pid, mref} =
      spawn_monitor(fn ->
        result = safe_read(cfg.sensor_fun, sensor_id)
        send(server, {:attempt_result, op_id, attempt, result})
      end)

    timer = Process.send_after(server, {:attempt_timeout, op_id, attempt}, cfg.attempt_timeout_ms)
    run = %{pid: pid, mref: mref, attempt: attempt, timer: timer}

    %{state | running: Map.put(state.running, op_id, run)}
    |> update_op(op_id, fn o ->
      Map.merge(o, %{status: :running, attempt: attempt, started_at: now_ms()})
    end)
  end

  # Executa a função do sensor isolando exceções, exits e throws.
  defp safe_read(fun, sensor_id) do
    try do
      case fun.(sensor_id) do
        {:ok, _} = ok -> ok
        {:error, _} = error -> error
        other -> {:error, {:unexpected_return, other}}
      end
    rescue
      e -> {:error, {:crash, Exception.message(e)}}
    catch
      kind, reason -> {:error, {:crash, {kind, reason}}}
    end
  end

  # Libera o slot de execução de uma operação.
  defp release(state, op_id) do
    {run, running} = Map.pop(state.running, op_id)
    Process.cancel_timer(run.timer)
    Process.demonitor(run.mref, [:flush])
    %{state | running: running}
  end

  ## Tratamento de resultados e retentativas

  defp complete_attempt(state, op_id, {:ok, value}) do
    finish(state, op_id, :completed, %{result: value, last_error: nil})
  end

  defp complete_attempt(state, op_id, {:error, _} = error) do
    handle_failure(state, op_id, error)
  end

  defp handle_failure(state, op_id, error) do
    op = Map.fetch!(state.ops, op_id)

    if transient?(error) and op.attempt < state.cfg.max_attempts do
      delay = backoff_delay(state.cfg, op.attempt)
      timer = Process.send_after(self(), {:retry, op_id}, delay)

      update_op(state, op_id, &Map.merge(&1, %{status: :waiting, last_error: error, timer: timer}))
    else
      finish(state, op_id, :failed, %{last_error: error})
    end
  end

  defp finish(state, op_id, status, fields) do
    now = now_ms()

    base = %{
      status: status,
      timer: nil,
      finished_at: now,
      expires_at: now + state.cfg.record_ttl_ms
    }

    update_op(state, op_id, &Map.merge(&1, Map.merge(base, fields)))
  end

  defp transient?({:error, {:crash, _}}), do: true
  defp transient?({:error, reason}) when reason in @transient_reasons, do: true
  defp transient?(_), do: false

  # Backoff exponencial limitado, com jitter para evitar rajadas sincronizadas.
  defp backoff_delay(cfg, attempt) do
    exp = cfg.base_backoff_ms * Integer.pow(2, attempt - 1)
    min(exp, cfg.max_backoff_ms) + :rand.uniform(cfg.base_backoff_ms)
  end

  ## Utilitários

  defp update_op(state, op_id, fun) do
    %{state | ops: Map.update!(state.ops, op_id, fun)}
  end

  defp expired?(%{status: status, expires_at: expires_at}, now)
       when status in [:completed, :failed] do
    expires_at <= now
  end

  defp expired?(_op, _now), do: false

  defp view(op) do
    Map.take(op, [
      :id,
      :sensor_id,
      :status,
      :attempt,
      :last_error,
      :result,
      :submitted_at,
      :started_at,
      :finished_at,
      :expires_at
    ])
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp now_ms, do: System.monotonic_time(:millisecond)
end

# Exemplo de uso (como comentário):
#
# {:ok, mon} = SensorMonitor.start_link(max_concurrency: 3, max_attempts: 4)
# {:ok, id} = SensorMonitor.submit(mon, "temp-01")
# SensorMonitor.in_progress(mon)
# SensorMonitor.status(mon, id)
#
# Com leitura customizada:
# {:ok, mon} = SensorMonitor.start_link(
#   sensor_fun: fn id -> if :rand.uniform() < 0.3, do: {:error, :busy}, else: {:ok, %{sensor: id}} end
# )
