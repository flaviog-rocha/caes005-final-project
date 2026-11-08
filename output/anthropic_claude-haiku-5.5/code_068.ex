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

# Monitoramento de sensores com processamento em lotes, detecção de falhas e concorrência limitada.
# Arquivo único e autocontido. Execute com: elixir sensor_monitor.exs

defmodule SensorMonitor do
  @moduledoc """
  Pipeline de monitoramento de sensores:

  * Leitura preguiçosa (lazy) de um arquivo CSV, linha a linha, via `File.stream!/1` e `Stream`.
    Nunca se carrega o arquivo inteiro na memória.
  * Agrupamento em lotes com `Stream.chunk_every/2`.
  * Processamento concorrente com `Task.Supervisor.async_stream_nolink/4`, que:
    - limita o número de operações simultâneas (`:max_concurrency`);
    - isola falhas: um lote que falha não derruba o pipeline;
    - reporta encerramentos inesperados como `{:exit, reason}`.
  * Persistência em um banco fictício (`SensorMonitor.FakeDB`).

  Equivalente opcional em Ecto.Migration (não obrigatório, não usado aqui):

      # defmodule SensorMonitor.Repo.Migrations.CreateSensorReadings do
      #   use Ecto.Migration
      #
      #   def change do
      #     create table(:sensor_readings) do
      #       add :sensor, :string, null: false
      #       add :ts, :bigint, null: false
      #       add :value, :float, null: false
      #       add :status, :string, null: false
      #     end
      #
      #     create index(:sensor_readings, [:sensor, :ts])
      #   end
      # end
  """

  # Quantidade de leituras por lote enviado ao banco.
  @batch_size 500

  # Limite padrão de tarefas simultâneas.
  @default_max_concurrency 4

  # Tempo máximo (ms) para um lote individual.
  @batch_timeout 5_000

  # Leituras acima deste valor são tratadas como anômalas e provocam falha no lote.
  @anomaly_threshold 5_000.0

  @doc """
  Executa o pipeline.

  Opções:
    * `:total` - número de linhas do arquivo gerado (padrão: 100_000)
    * `:max_concurrency` - limite de operações simultâneas (padrão: #{@default_max_concurrency})
    * `:path` - caminho do arquivo CSV de entrada (padrão: arquivo temporário gerado)
  """
  def run(opts \\ []) do
    total = Keyword.get(opts, :total, 100_000)
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)

    path =
      Keyword.get(
        opts,
        :path,
        Path.join(System.tmp_dir!(), "sensors_#{System.unique_integer([:positive])}.csv")
      )

    # Inicia o banco fictício e o supervisor de tarefas (ignora caso já estejam iniciados).
    _ = SensorMonitor.FakeDB.start_link([])
    _ = Task.Supervisor.start_link(name: SensorMonitor.TaskSup)

    generate_file!(path, total)

    try do
      summary =
        path
        |> stream_readings()
        |> Stream.chunk_every(@batch_size)
        |> process_batches(max_concurrency)

      IO.puts("Resumo do processamento: #{inspect(summary)}")
      IO.puts("Estado do banco fictício: #{inspect(SensorMonitor.FakeDB.stats())}")

      summary
    after
      # Remove o arquivo temporário ao final, mesmo em caso de erro.
      File.rm(path)
    end
  end

  # ---------------------------------------------------------------------------
  # Leitura preguiçosa
  # ---------------------------------------------------------------------------

  # Produz um stream de leituras válidas sem carregar o arquivo inteiro.
  defp stream_readings(path) do
    path
    |> File.stream!()
    |> Stream.map(&parse_line/1)
    |> Stream.filter(&(&1 != :error))
    |> Stream.map(fn {:ok, reading} -> reading end)
  end

  # Formato esperado: "sensor,timestamp,valor"
  defp parse_line(line) do
    with [sensor, ts, val] <- String.split(String.trim(line), ","),
         {ts_int, ""} <- Integer.parse(ts),
         {value, ""} <- Float.parse(val) do
      {:ok, %{sensor: sensor, ts: ts_int, value: value}}
    else
      _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Processamento concorrente com limite e detecção de falhas
  # ---------------------------------------------------------------------------

  defp process_batches(batches, max_concurrency) do
    # async_stream_nolink não derruba o chamador quando uma tarefa falha.
    # O resultado de cada tarefa chega como {:ok, valor} ou {:exit, motivo}.
    # Como o enumerável é consumido sob demanda, apenas `max_concurrency`
    # lotes existem em voo ao mesmo tempo.
    SensorMonitor.TaskSup
    |> Task.Supervisor.async_stream_nolink(batches, &handle_batch/1,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: @batch_timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce(%{batches: 0, rows: 0, failures: 0}, fn
      {:ok, {:ok, count}}, acc ->
        %{acc | batches: acc.batches + 1, rows: acc.rows + count}

      {:ok, {:error, reason}}, acc ->
        IO.puts(:stderr, "[monitor] falha lógica no lote: #{inspect(reason)}")
        %{acc | failures: acc.failures + 1}

      # Encerramento inesperado da tarefa (exceção, exit, timeout, kill etc.)
      {:exit, reason}, acc ->
        IO.puts(:stderr, "[monitor] encerramento inesperado de tarefa: #{inspect(reason)}")
        %{acc | failures: acc.failures + 1}
    end)
  end

  defp handle_batch(batch) do
    # Qualquer exceção aqui gera {:exit, _} no chamador.
    rows = Enum.map(batch, &enrich/1)
    SensorMonitor.FakeDB.insert_batch(rows)
  end

  defp enrich(%{value: value, sensor: sensor}) when value > @anomaly_threshold do
    raise ArgumentError, "leitura anômala no sensor #{sensor}: #{value}"
  end

  defp enrich(reading), do: Map.put(reading, :status, :valid)

  # ---------------------------------------------------------------------------
  # Geração de dados de exemplo (escrita também preguiçosa)
  # ---------------------------------------------------------------------------

  defp generate_file!(path, total) do
    Stream.repeatedly(&random_line/0)
    |> Stream.take(total)
    |> Stream.into(File.stream!(path))
    |> Stream.run()
  end

  defp random_line do
    cond do
      # Ocasionalmente gera linhas corrompidas para exercitar o parser.
      :rand.uniform(1_000) == 1 ->
        "linha_corrompida\n"

      true ->
        sensor = "S#{:rand.uniform(20)}"
        ts = System.system_time(:second) + :rand.uniform(100_000)

        # Ocasionalmente gera uma leitura anômala para exercitar a detecção de falhas.
        value =
          if :rand.uniform(2_000) == 1,
            do: 9_999.0,
            else: :rand.uniform() * 100

        "#{sensor},#{ts},#{:erlang.float_to_binary(value, decimals: 2)}\n"
    end
  end
end

# ---------------------------------------------------------------------------
# Banco de dados fictício (em memória, via Agent)
# ---------------------------------------------------------------------------

defmodule SensorMonitor.FakeDB do
  @moduledoc """
  Banco fictício que simula latência de escrita e contabiliza linhas gravadas.
  """

  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{rows: 0, batches: 0} end, name: __MODULE__)
  end

  @doc "Simula a gravação de um lote de leituras."
  def insert_batch(rows) do
    # Simula latência de I/O.
    Process.sleep(5)

    Agent.update(__MODULE__, fn state ->
      %{state | rows: state.rows + length(rows), batches: state.batches + 1}
    end)

    {:ok, length(rows)}
  end

  def stats, do: Agent.get(__MODULE__, & &1)
end

# Ponto de entrada
SensorMonitor.run(total: 100_000, max_concurrency: 4)
