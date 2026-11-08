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

# Monitor de sensores com GenServer, idempotência, processamento em streaming
# e histórico persistido em arquivo (append-only).
#
# Formato de entrada (uma operação por linha):
#   op_id,sensor_id,valor,timestamp
#
# Formato do histórico (uma operação executada por linha, separada por TAB):
#   op_id<TAB>sensor_id<TAB>valor<TAB>timestamp
defmodule SensorMonitor do
  @moduledoc """
  Monitora sensores garantindo que cada operação (identificada por `op_id`)
  seja executada no máximo uma vez, processa grandes volumes via `Stream`
  (sem carregar a fonte inteira na memória) e mantém um histórico em disco.
  """

  use GenServer

  # Tamanho do lote enviado ao GenServer por chamada.
  @batch_size 1_000
  # Caminho padrão do arquivo de histórico.
  @default_history "sensor_history.log"

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o monitor. Opções: `:name` (padrão: módulo) e `:history_path`."
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Processa uma fonte Enumerable (ex.: `File.stream!/1`) de forma preguiçosa.
  As linhas são parseadas e agrupadas em lotes, e cada lote é enviado ao
  GenServer. Retorna um mapa com a quantidade de operações executadas e
  de duplicatas descartadas.
  """
  def process_stream(source, server \\ __MODULE__) do
    source
    |> Stream.map(&parse_line/1)
    |> Stream.reject(&is_nil/1)
    |> Stream.chunk_every(@batch_size)
    |> Enum.reduce(%{executed: 0, duplicates: 0}, fn batch, acc ->
      %{executed: e, duplicates: d} =
        GenServer.call(server, {:process_batch, batch}, :infinity)

      %{executed: acc.executed + e, duplicates: acc.duplicates + d}
    end)
  end

  @doc "Retorna as estatísticas acumuladas de um sensor."
  def stats(sensor_id, server \\ __MODULE__) do
    GenServer.call(server, {:stats, sensor_id})
  end

  @doc """
  Retorna um Stream preguiçoso com as linhas do histórico.
  Nada é carregado na memória até que o Stream seja consumido.
  """
  def history(server \\ __MODULE__) do
    path = GenServer.call(server, :history_path)
    File.stream!(path)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :history_path, @default_history)
    # Garante que o arquivo de histórico exista.
    File.touch!(path)

    state = %{
      path: path,
      # Conjunto de op_ids já executados (idempotência).
      seen: load_seen_ids(path),
      # Estatísticas por sensor, com memória constante por sensor.
      sensors: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:process_batch, readings}, _from, state) do
    # Acumula as entradas aceitas e conta duplicatas, atualizando o estado
    # operação a operação. Como o `seen` é atualizado dentro do reduce,
    # duplicatas dentro do mesmo lote também são detectadas.
    {entries, duplicates, state} =
      Enum.reduce(readings, {[], 0, state}, fn reading, {entries, dups, st} ->
        if MapSet.member?(st.seen, reading.op_id) do
          # Operação já executada: não reexecuta.
          {entries, dups + 1, st}
        else
          st = %{
            st
            | seen: MapSet.put(st.seen, reading.op_id),
              sensors: update_sensor(st.sensors, reading)
          }

          {[format_entry(reading) | entries], dups, st}
        end
      end)

    # Persiste o histórico do lote em um único append.
    if entries != [] do
      File.write!(state.path, Enum.reverse(entries), [:append])
    end

    result = %{executed: length(entries), duplicates: duplicates}
    {:reply, result, state}
  end

  def handle_call({:stats, sensor_id}, _from, state) do
    {:reply, Map.get(state.sensors, sensor_id), state}
  end

  def handle_call(:history_path, _from, state) do
    {:reply, state.path, state}
  end

  # ---------------------------------------------------------------------------
  # Funções auxiliares privadas
  # ---------------------------------------------------------------------------

  # Reconstrói o conjunto de op_ids a partir do histórico em disco,
  # lendo linha a linha (sem carregar o arquivo inteiro).
  defp load_seen_ids(path) do
    path
    |> File.stream!()
    |> Stream.map(fn line -> line |> String.split("\t", parts: 2) |> hd() end)
    |> Enum.into(MapSet.new())
  end

  # Atualiza agregados do sensor: contagem, mínimo, máximo, último valor.
  defp update_sensor(sensors, %{sensor_id: id, value: v, ts: ts}) do
    Map.update(
      sensors,
      id,
      %{count: 1, min: v, max: v, last: v, last_ts: ts},
      fn s ->
        %{
          s
          | count: s.count + 1,
            min: min(s.min, v),
            max: max(s.max, v),
            last: v,
            last_ts: ts
        }
      end
    )
  end

  defp format_entry(%{op_id: op, sensor_id: sensor, value: v, ts: ts}) do
    "#{op}\t#{sensor}\t#{v}\t#{ts}\n"
  end

  # Converte uma linha CSV em um mapa de leitura. Retorna nil se inválida.
  defp parse_line(line) do
    with [op, sensor, val, ts] <- String.split(String.trim(line), ",", parts: 4),
         {v, ""} <- Float.parse(val),
         {t, ""} <- Integer.parse(ts) do
      %{op_id: op, sensor_id: sensor, value: v, ts: t}
    else
      _ -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Demonstração
  # ---------------------------------------------------------------------------

  @doc """
  Gera um arquivo grande de leituras (com operações repetidas), processa-o
  em streaming e exibe os resultados.
  """
  def demo do
    tmp = System.tmp_dir!()
    suffix = System.unique_integer([:positive])
    input = Path.join(tmp, "sensores_#{suffix}.csv")
    history = Path.join(tmp, "historico_#{suffix}.log")

    # Gera 200_000 linhas; os op_ids se repetem para testar idempotência.
    File.open!(input, [:write], fn io ->
      for i <- 1..200_000 do
        IO.write(
          io,
          "op-#{rem(i, 150_000)},sensor-#{rem(i, 10)},#{:rand.uniform() * 100},#{i}\n"
        )
      end
    end)

    {:ok, pid} = start_link(history_path: history)

    # Processamento em streaming: o arquivo é lido sob demanda.
    result = input |> File.stream!() |> process_stream(pid)
    IO.inspect(result, label: "resultado")

    IO.inspect(stats("sensor-3", pid), label: "estatísticas sensor-3")

    history_count = pid |> history() |> Enum.count()
    IO.inspect(history_count, label: "entradas no histórico")

    GenServer.stop(pid)
    File.rm!(input)
    File.rm!(history)

    result
  end
end
