# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitoramento de sensores com GenServer.

  Requisitos atendidos:

    1. Idempotência: cada operação é identificada por `op_id`. Uma operação já
       registrada é rejeitada (contabilizada como duplicada), mesmo após reinício,
       pois o índice de operações é persistido em DETS.

    2. Grandes volumes: o arquivo de entrada é lido em streaming (`File.stream!/1`),
       processado em lotes de tamanho fixo (`Stream.chunk_every/2`) e nunca
       carregado integralmente na memória. Os dados de estado também residem em
       DETS (disco), não em memória do processo.

    3. Histórico: toda operação (processada, duplicada ou alerta) é registrada
       em uma tabela DETS sequencial, consultável via `history/2`.

  Formato de cada linha do CSV de entrada:

      op_id,sensor_id,valor,timestamp_unix

  Uso típico:

      {:ok, _pid} = SensorMonitor.start_link(data_dir: "/tmp/sensores", threshold: 80.0)
      SensorMonitor.process_file("leituras.csv", batch_size: 1_000)
      SensorMonitor.history(limit: 20)
      SensorMonitor.sensor_status("sensor-42")
  """

  use GenServer

  # Chave reservada (átomo) para persistir o contador de sequência do histórico
  @meta_key :__meta__

  @empty_stats %{processed: 0, duplicated: 0, alerts: 0}

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o monitor.

  Opções:
    * `:name` - nome do processo registrado (padrão: `__MODULE__`)
    * `:data_dir` - diretório onde os arquivos DETS são gravados
    * `:threshold` - limiar acima do qual um alerta é gerado (padrão: 100.0)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Processa um arquivo de leituras em streaming. Retorna estatísticas agregadas.
  """
  def process_file(server \\ __MODULE__, path, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 1_000)
    timeout = Keyword.get(opts, :timeout, :infinity)

    path
    |> File.stream!()
    |> Stream.map(&parse_line/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.chunk_every(batch_size)
    |> Stream.map(&GenServer.call(server, {:process_batch, &1}, timeout))
    |> Enum.reduce(@empty_stats, &merge_stats/2)
  end

  @doc """
  Processa uma lista (já em memória) de leituras em lote.
  Cada leitura é um mapa com as chaves `:op_id`, `:sensor_id`, `:value` e `:ts`.
  """
  def process_batch(server \\ __MODULE__, readings) when is_list(readings) do
    GenServer.call(server, {:process_batch, readings}, :infinity)
  end

  @doc """
  Retorna as últimas `limit` entradas do histórico, da mais recente para a mais antiga.
  """
  def history(server \\ __MODULE__, limit \\ 100) do
    GenServer.call(server, {:history, limit})
  end

  @doc """
  Retorna a última leitura conhecida de um sensor, ou `nil`.
  """
  def sensor_status(server \\ __MODULE__, sensor_id) do
    GenServer.call(server, {:sensor_status, sensor_id})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    dir = Keyword.get(opts, :data_dir, System.tmp_dir!())
    threshold = Keyword.get(opts, :threshold, 100.0)
    prefix = name |> inspect() |> String.replace(~r/\W/, "_")

    File.mkdir_p!(dir)

    with {:ok, ops} <- open_table(:"#{prefix}_ops", dir),
         {:ok, sensors} <- open_table(:"#{prefix}_sensors", dir),
         {:ok, history} <- open_table(:"#{prefix}_history", dir) do
      seq =
        case :dets.lookup(ops, @meta_key) do
          [{@meta_key, s}] -> s
          [] -> 0
        end

      {:ok,
       %{
         ops: ops,
         sensors: sensors,
         history: history,
         seq: seq,
         threshold: threshold
       }}
    end
  end

  @impl true
  def handle_call({:process_batch, readings}, _from, state) do
    # Processamento sequencial dentro do GenServer garante que não há
    # condição de corrida entre a verificação e a inserção do op_id.
    {stats, state} = Enum.reduce(readings, {@empty_stats, state}, &process_reading/2)

    :ok = :dets.insert(state.ops, {@meta_key, state.seq})
    :ok = sync(state)

    {:reply, stats, state}
  end

  def handle_call({:history, limit}, _from, state) do
    {:reply, last_entries(state.history, limit), state}
  end

  def handle_call({:sensor_status, sensor_id}, _from, state) do
    reply =
      case :dets.lookup(state.sensors, sensor_id) do
        [{^sensor_id, last}] -> last
        [] -> nil
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each([state.ops, state.sensors, state.history], &:dets.close/1)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Processamento de uma leitura
  # ---------------------------------------------------------------------------

  defp process_reading(
         %{op_id: id, sensor_id: sid, value: value, ts: ts},
         {stats, state}
       ) do
    case :dets.lookup(state.ops, id) do
      # Operação já executada: rejeita e registra no histórico
      [_] ->
        state = append_history(state, {:duplicate, id, sid})
        {Map.update!(stats, :duplicated, &(&1 + 1)), state}

      # Operação nova: executa, persiste e registra
      [] ->
        :ok = :dets.insert(state.ops, {id, ts})
        :ok = :dets.insert(state.sensors, {sid, %{value: value, ts: ts}})

        state = append_history(state, {:processed, id, sid, value})

        alert? = value > state.threshold
        state = if alert?, do: append_history(state, {:alert, id, sid, value}), else: state

        stats = %{
          stats
          | processed: stats.processed + 1,
            alerts: stats.alerts + (if alert?, do: 1, else: 0)
        }

        {stats, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Histórico
  # ---------------------------------------------------------------------------

  defp append_history(state, entry) do
    seq = state.seq + 1
    :ok = :dets.insert(state.history, {seq, System.os_time(:millisecond), entry})
    %{state | seq: seq}
  end

  # Percorre a tabela em streaming (foldl), mantendo apenas uma janela limitada
  # de registros em memória (no máximo 2 * limit).
  defp last_entries(table, limit) do
    :dets.foldl(
      fn record, acc ->
        acc = [record | acc]
        if length(acc) > 2 * limit, do: top(acc, limit), else: acc
      end,
      [],
      table
    )
    |> top(limit)
  end

  defp top(records, limit) do
    records
    |> Enum.sort_by(&elem(&1, 0), :desc)
    |> Enum.take(limit)
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  defp open_table(name, dir) do
    file = dir |> Path.join("#{name}.dets") |> String.to_charlist()
    :dets.open_file(name, file: file, type: :set)
  end

  defp sync(state) do
    with :ok <- :dets.sync(state.ops),
         :ok <- :dets.sync(state.sensors),
         :ok <- :dets.sync(state.history) do
      :ok
    end
  end

  defp merge_stats(batch_stats, acc) do
    Map.merge(acc, batch_stats, fn _key, a, b -> a + b end)
  end

  # Converte uma linha CSV em mapa de leitura. Linhas inválidas retornam nil.
  defp parse_line(line) do
    with [op_id, sensor_id, value, ts] <- line |> String.trim() |> String.split(","),
         {v, ""} <- Float.parse(value),
         {t, ""} <- Integer.parse(ts) do
      %{op_id: op_id, sensor_id: sensor_id, value: v, ts: t}
    else
      _ -> nil
    end
  end
end
