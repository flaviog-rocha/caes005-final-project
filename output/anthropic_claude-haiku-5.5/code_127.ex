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

defmodule SensorMonitor do
  @moduledoc """
  Monitor concorrente de sensores, implementado apenas com a biblioteca padrão.

  Recursos:
    * Execução simultânea: cada leitura roda em um processo próprio.
    * Limite de concorrência: operações excedentes aguardam em fila FIFO.
    * Novas tentativas: falhas temporárias e crashes são re-tentados com backoff exponencial.
    * Expiração automática: registros finalizados são removidos após o TTL (varredura periódica).
    * Consulta de estado: operações em andamento podem ser listadas a qualquer momento.

  Contrato de uma operação (função de aridade 0):
    {:ok, valor}                  -> sucesso
    {:error, :transient, motivo}  -> falha temporária (será re-tentada)
    {:error, motivo}              -> falha definitiva
    exceção / throw / exit        -> falha definitiva

  Exemplo:
    {:ok, pid} = SensorMonitor.start_link(max_concurrency: 2, ttl_ms: 30_000)
    {:ok, op_id} = SensorMonitor.submit(pid, :temp_01, fn -> {:ok, 22.5} end)
    SensorMonitor.fetch(pid, op_id)
    SensorMonitor.status(pid)
  """

  use GenServer

  defmodule Op do
    @moduledoc false
    # Registro de uma operação de leitura de sensor.
    defstruct [
      :id,
      :sensor_id,
      :fun,
      :status,
      :attempt,
      :result,
      :error,
      :created_at,
      :updated_at,
      :expires_at
    ]
  end

  @defaults [
    name: nil,
    # Quantidade máxima de operações executando ao mesmo tempo
    max_concurrency: 4,
    # Número de novas tentativas após a tentativa inicial
    max_retries: 3,
    # Base do backoff exponencial (base * 2^(tentativa - 1))
    base_backoff_ms: 200,
    # Tempo de vida de registros finalizados (sucesso ou falha definitiva)
    ttl_ms: 60_000,
    # Intervalo da varredura de expiração
    sweep_interval_ms: 1_000
  ]

  ## API pública

  @doc "Inicia o monitor. Aceita as opções em @defaults."
  def start_link(opts \\ []) do
    opts = Keyword.merge(@defaults, opts)
    gen_opts = if opts[:name], do: [name: opts[:name]], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Submete uma operação de leitura para um sensor. Retorna {:ok, op_id}."
  def submit(server, sensor_id, fun) when is_function(fun, 0) do
    GenServer.call(server, {:submit, sensor_id, fun})
  end

  @doc "Consulta um registro de operação. Retorna {:ok, op} | {:error, :not_found}."
  def fetch(server, op_id) do
    GenServer.call(server, {:fetch, op_id})
  end

  @doc "Retorna o estado atual das operações em andamento e métricas gerais."
  def status(server) do
    GenServer.call(server, :status)
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    cfg = Map.new(opts)

    state = %{
      cfg: cfg,
      seq: 0,
      # op_id => %Op{}
      ops: %{},
      # Fila FIFO de ids aguardando execução
      queue: :queue.new(),
      # monitor_ref => op_id, para operações em execução
      running: %{}
    }

    schedule_sweep(cfg.sweep_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:submit, sensor_id, fun}, _from, state) do
    now = now()
    id = state.seq + 1

    op = %Op{
      id: id,
      sensor_id: sensor_id,
      fun: fun,
      status: :queued,
      attempt: 0,
      created_at: now,
      updated_at: now
    }

    state =
      %{state | seq: id, ops: Map.put(state.ops, id, op), queue: :queue.in(id, state.queue)}
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:fetch, id}, _from, state) do
    reply =
      case Map.fetch(state.ops, id) do
        {:ok, op} -> {:ok, public(op)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:status, _from, state) do
    in_progress =
      state.ops
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running, :retrying]))
      |> Enum.map(&public/1)
      |> Enum.sort_by(& &1.id)

    summary = %{
      running: Enum.count(in_progress, &(&1.status == :running)),
      queued: Enum.count(in_progress, &(&1.status == :queued)),
      retrying: Enum.count(in_progress, &(&1.status == :retrying)),
      max_concurrency: state.cfg.max_concurrency,
      operations: in_progress
    }

    {:reply, summary, state}
  end

  @impl true
  # Resultado enviado pelo processo da operação
  def handle_info({:op_result, id, result}, state) do
    state =
      state
      |> release(id)
      |> finish_attempt(id, result)
      |> dispatch()

    {:noreply, state}
  end

  # Processo da operação terminou sem enviar resultado (crash)
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.running, ref) do
      {nil, _running} ->
        {:noreply, state}

      {id, running} ->
        state =
          %{state | running: running}
          |> finish_attempt(id, {:error, :transient, {:crash, reason}})
          |> dispatch()

        {:noreply, state}
    end
  end

  # Disparo de nova tentativa após o backoff
  def handle_info({:retry, id}, state) do
    state =
      case Map.get(state.ops, id) do
        %Op{status: :retrying} = op ->
          op = %{op | status: :queued, updated_at: now()}
          %{state | ops: Map.put(state.ops, id, op), queue: :queue.in(id, state.queue)}

        _ ->
          state
      end

    {:noreply, dispatch(state)}
  end

  # Varredura de expiração de registros finalizados
  def handle_info(:sweep, state) do
    now = now()

    ops =
      for {id, op} <- state.ops, not expired?(op, now), into: %{}, do: {id, op}

    schedule_sweep(state.cfg.sweep_interval_ms)
    {:noreply, %{state | ops: ops}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Funções privadas

  # Inicia operações da fila enquanto houver capacidade disponível
  defp dispatch(state) do
    cond do
      map_size(state.running) >= state.cfg.max_concurrency ->
        state

      :queue.is_empty(state.queue) ->
        state

      true ->
        {{:value, id}, queue} = :queue.out(state.queue)

        state
        |> Map.put(:queue, queue)
        |> start_op(id)
        |> dispatch()
    end
  end

  # Executa a operação em um processo monitorado
  defp start_op(state, id) do
    op = Map.fetch!(state.ops, id)
    parent = self()
    fun = op.fun

    {_pid, ref} =
      spawn_monitor(fn ->
        result = run(fun)
        send(parent, {:op_result, id, result})
      end)

    op = %{op | status: :running, attempt: op.attempt + 1, updated_at: now()}

    %{
      state
      | ops: Map.put(state.ops, id, op),
        running: Map.put(state.running, ref, id)
    }
  end

  # Executa a função capturando exceções como falhas definitivas
  defp run(fun) do
    fun.()
  rescue
    e -> {:error, Exception.message(e)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # Remove o monitor e o vínculo de execução da operação
  defp release(state, id) do
    case Enum.find(state.running, fn {_ref, op_id} -> op_id == id end) do
      {ref, _} ->
        Process.demonitor(ref, [:flush])
        %{state | running: Map.delete(state.running, ref)}

      nil ->
        state
    end
  end

  # Decide o próximo estado da operação com base no resultado da tentativa
  defp finish_attempt(state, id, result) do
    op = Map.fetch!(state.ops, id)
    %{max_retries: max_retries, base_backoff_ms: base} = state.cfg
    now = now()

    case result do
      {:ok, value} ->
        op = %{finalize(op, :done, now, state.cfg) | result: value, error: nil}
        put_op(state, op)

      {:error, :transient, reason} ->
        if op.attempt <= max_retries do
          delay = base * Integer.pow(2, op.attempt - 1)
          Process.send_after(self(), {:retry, id}, delay)
          put_op(state, %{op | status: :retrying, error: reason, updated_at: now})
        else
          put_op(state, finalize(%{op | error: reason}, :failed, now, state.cfg))
        end

      {:error, reason} ->
        put_op(state, finalize(%{op | error: reason}, :failed, now, state.cfg))

      other ->
        put_op(state, finalize(%{op | error: {:unexpected_result, other}}, :failed, now, state.cfg))
    end
  end

  # Marca o registro como finalizado e define sua expiração
  defp finalize(op, status, now, cfg) do
    %{op | status: status, updated_at: now, expires_at: now + cfg.ttl_ms}
  end

  defp put_op(state, op), do: %{state | ops: Map.put(state.ops, op.id, op)}

  defp expired?(%Op{status: status, expires_at: exp}, now)
       when status in [:done, :failed] and is_integer(exp),
       do: exp <= now

  defp expired?(_op, _now), do: false

  # Representação pública do registro (sem a função interna)
  defp public(op) do
    op
    |> Map.from_struct()
    |> Map.delete(:fun)
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp now, do: System.monotonic_time(:millisecond)
end
