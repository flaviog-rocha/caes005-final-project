# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Detectar encerramentos inesperados de processos.
# 4. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Monitoramento de sensores em Elixir (arquivo único, apenas biblioteca padrão).
# Execute com: elixir sensor_monitor.exs

defmodule SensorMonitor do
  @moduledoc """
  Monitoramento de sensores com processamento em streams, concorrência limitada
  e detecção de encerramentos inesperados de Tasks.

  Pipeline:
    fonte (Stream, lazy) -> lotes (chunk_every) -> janelas de concorrência (chunk_every)
    -> Task.Supervisor.async_nolink por lote -> Task.yield_many (monitoramento de saída)
    -> estatísticas agregadas (memória constante)

  Opcionalmente, a persistência pode ser substituída por um banco real (ex.: Ecto),
  criando-se um Ecto.Migration para a tabela de leituras. Aqui usamos um banco fictício.
  """

  require Logger

  @defaults [
    total_readings: 200_000,
    batch_size: 500,
    max_concurrency: 8,
    task_timeout: 5_000,
    alert_threshold: 90.0,
    fault_every: 50_000
  ]

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Executa o monitoramento.

  Opções:
    * `:source` - Stream de leituras (opcional; por padrão gera leituras sintéticas).
    * `:total_readings` - quantidade de leituras sintéticas.
    * `:batch_size` - tamanho de cada lote enviado a uma Task.
    * `:max_concurrency` - máximo de Tasks simultâneas.
    * `:task_timeout` - tempo limite (ms) por lote.
    * `:alert_threshold` - limiar que gera alerta.
    * `:fault_every` - injeta falhas a cada N leituras (demonstração de crash).
  """
  def run(opts \\ []) do
    cfg = Keyword.merge(@defaults, opts)

    {:ok, db} = SensorMonitor.FakeDB.start_link()
    {:ok, sup} = Task.Supervisor.start_link()

    source = Keyword.get_lazy(cfg, :source, fn -> readings_stream(cfg[:total_readings]) end)

    stats =
      source
      # Lotes lazy: nunca mais de batch_size leituras em memória por Task.
      |> Stream.chunk_every(cfg[:batch_size])
      # Janelas: no máximo max_concurrency Tasks são disparadas por vez.
      |> Stream.chunk_every(cfg[:max_concurrency])
      |> Enum.reduce(new_stats(), fn window, acc ->
        window
        |> Enum.map(fn batch ->
          Task.Supervisor.async_nolink(sup, fn -> process_batch(batch, cfg) end)
        end)
        |> collect(cfg[:task_timeout])
        |> then(&merge_stats(acc, &1))
      end)

    report = Map.put(stats, :db, SensorMonitor.FakeDB.snapshot())

    Supervisor.stop(sup)
    Agent.stop(db)

    IO.inspect(report, label: "Relatório de monitoramento")
    report
  end

  @doc """
  Fonte de leituras lida de arquivo CSV (`seq,sensor_id,value`) de forma lazy.
  Não carrega o arquivo inteiro na memória.
  """
  def file_stream(path) do
    path
    |> File.stream!()
    |> Stream.map(&parse_line/1)
  end

  # ---------------------------------------------------------------------------
  # Fonte sintética (lazy)
  # ---------------------------------------------------------------------------

  defp readings_stream(total) do
    Stream.unfold(0, fn
      i when i >= total -> nil
      i -> {synthetic_reading(i), i + 1}
    end)
  end

  defp synthetic_reading(i) do
    %{seq: i, sensor_id: rem(i, 100), value: :rand.uniform() * 100}
  end

  defp parse_line(line) do
    [seq, sensor_id, value] = line |> String.trim() |> String.split(",")

    %{
      seq: String.to_integer(seq),
      sensor_id: String.to_integer(sensor_id),
      value: String.to_float(value)
    }
  end

  # ---------------------------------------------------------------------------
  # Processamento executado dentro das Tasks
  # ---------------------------------------------------------------------------

  defp process_batch(batch, cfg) do
    evaluated = Enum.map(batch, &evaluate(&1, cfg))
    :ok = SensorMonitor.FakeDB.insert_batch(evaluated)
    {:ok, summarize(evaluated)}
  end

  defp evaluate(%{seq: seq, value: value} = reading, cfg) do
    # Falha simulada para demonstrar detecção de encerramento inesperado.
    if seq > 0 and rem(seq, cfg[:fault_every]) == 0 do
      raise "falha simulada no sensor #{reading.sensor_id} (seq #{seq})"
    end

    Map.put(reading, :alert, value >= cfg[:alert_threshold])
  end

  defp summarize(evaluated) do
    Enum.reduce(evaluated, %{processed: 0, alerts: 0}, fn r, acc ->
      %{acc | processed: acc.processed + 1, alerts: acc.alerts + if(r.alert, do: 1, else: 0)}
    end)
  end

  # ---------------------------------------------------------------------------
  # Coleta e monitoramento de término das Tasks
  # ---------------------------------------------------------------------------

  # Task.yield_many aguarda cada Task; Task.async_nolink garante que a falha
  # de uma Task não derruba o processo coordenador. Erros viram {:crashed, reason}.
  defp collect(tasks, timeout) do
    tasks
    |> Task.yield_many(timeout)
    |> Enum.map(fn
      {_task, {:ok, result}} ->
        {:ok, result}

      {_task, {:exit, reason}} ->
        {:crashed, reason}

      {task, nil} ->
        # Timeout: encerra a Task à força e registra como falha.
        case Task.shutdown(task, :brutal_kill) do
          {:ok, result} -> {:ok, result}
          {:exit, reason} -> {:crashed, reason}
          nil -> {:crashed, :timeout}
        end
    end)
  end

  # ---------------------------------------------------------------------------
  # Estatísticas (memória constante, independente do volume)
  # ---------------------------------------------------------------------------

  defp new_stats do
    %{processed: 0, alerts: 0, batches_ok: 0, batches_failed: 0, failures: %{}}
  end

  defp merge_stats(acc, results) do
    Enum.reduce(results, acc, fn
      {:ok, %{processed: p, alerts: a}}, s ->
        %{s | processed: s.processed + p, alerts: s.alerts + a, batches_ok: s.batches_ok + 1}

      {:crashed, reason}, s ->
        kind = failure_kind(reason)
        Logger.warning("Encerramento inesperado de Task: #{inspect(kind)}")

        %{
          s
          | batches_failed: s.batches_failed + 1,
            failures: Map.update(s.failures, kind, 1, &(&1 + 1))
        }
    end)
  end

  defp failure_kind({%{__struct__: module}, _stacktrace}), do: module
  defp failure_kind(reason), do: reason
end

# ---------------------------------------------------------------------------
# Banco de dados fictício (Agent). Pode ser substituído por Ecto.Repo
# com um Ecto.Migration criando a tabela `sensor_readings`.
# ---------------------------------------------------------------------------

defmodule SensorMonitor.FakeDB do
  @moduledoc false

  def start_link do
    Agent.start_link(fn -> %{rows: 0, alerts: 0} end, name: __MODULE__)
  end

  def insert_batch(batch) do
    # Latência simulada de I/O.
    Process.sleep(2)

    rows = length(batch)
    alerts = Enum.count(batch, & &1.alert)

    Agent.update(__MODULE__, fn state ->
      %{state | rows: state.rows + rows, alerts: state.alerts + alerts}
    end)

    :ok
  end

  def snapshot do
    Agent.get(__MODULE__, & &1)
  end
end

SensorMonitor.run()
