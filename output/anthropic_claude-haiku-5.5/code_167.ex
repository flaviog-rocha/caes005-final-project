# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Manter um histórico das operações realizadas.
# 5. Impedir atualizações parcialmente concluídas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sensor_monitor.exs
# Execução: elixir sensor_monitor.exs

defmodule SensorMonitor.Store do
  # Banco de dados fictício em memória (Agent).
  #
  # Pontos-chave:
  #  * `transaction/1` executa a função dentro do processo do Agent, que processa
  #    uma requisição por vez. Se a função retornar {:ok, resultado, novo_db}, o
  #    novo estado é efetivado; se retornar {:error, motivo}, o estado anterior é
  #    mantido. Isso garante atomicidade (tudo ou nada) sem lock explícito.
  #  * O histórico de operações é append-only e fica no próprio "banco".
  #
  # Opcional: esta camada pode ser trocada por um adaptador Ecto. Nesse caso,
  # `transaction/1` delegaria para `Repo.transaction/1`, e as tabelas seriam
  # criadas por uma migration similar a:
  #
  #   defmodule SensorMonitor.Repo.Migrations.CreateSensorTables do
  #     use Ecto.Migration
  #
  #     def change do
  #       create table(:sensors, primary_key: false) do
  #         add :id, :string, primary_key: true
  #         add :count, :integer, null: false
  #         add :sum, :float, null: false
  #         add :min, :float
  #         add :max, :float
  #         add :last_batch_id, :string
  #       end
  #
  #       create table(:sensor_history) do
  #         add :seq, :bigint, null: false
  #         add :sensor_id, :string, null: false
  #         add :batch_id, :string
  #         add :status, :string, null: false
  #         add :count, :integer
  #         add :reason, :text
  #         add :at, :utc_datetime_usec
  #       end
  #     end
  #   end

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{sensors: %{}, history: [], seq: 0} end, name: __MODULE__)
  end

  @doc "Executa `fun` atomicamente. `fun` recebe o db e retorna {:ok, resultado, novo_db} ou {:error, motivo}."
  def transaction(fun) when is_function(fun, 1) do
    Agent.get_and_update(
      __MODULE__,
      fn db ->
        case fun.(db) do
          {:ok, result, new_db} -> {{:ok, result}, new_db}
          {:error, reason} -> {{:error, reason}, db}
        end
      end,
      :infinity
    )
  end

  def get_sensor(id), do: Agent.get(__MODULE__, &Map.get(&1.sensors, id))

  def history, do: Agent.get(__MODULE__, &Enum.reverse(&1.history))

  # Helpers usados dentro de transaction/1 (executam no processo do Agent).

  def put_sensor(db, sensor) do
    %{db | sensors: Map.put(db.sensors, sensor.id, sensor)}
  end

  def log(db, sensor_id, batch_id, status, count, reason) do
    seq = db.seq + 1

    entry = %{
      seq: seq,
      at: DateTime.utc_now(),
      sensor_id: sensor_id,
      batch_id: batch_id,
      status: status,
      count: count,
      reason: reason
    }

    %{db | seq: seq, history: [entry | db.history]}
  end
end

defmodule SensorMonitor.Sensor do
  # Processo independente por sensor. Cada entidade possui seu próprio estado
  # e seu próprio processo, então sensores diferentes nunca se bloqueiam.
  #
  # Memória: o estado guarda apenas agregados e uma janela limitada das
  # últimas leituras. As leituras brutas nunca são acumuladas.

  use GenServer

  alias SensorMonitor.Store

  @window_size 100
  @min_valid -50.0
  @max_valid 150.0

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(sensor_id) do
    GenServer.start_link(__MODULE__, sensor_id, name: via(sensor_id))
  end

  def via(sensor_id), do: {:via, Registry, {SensorMonitor.Registry, sensor_id}}

  @doc "Ingere um lote. Ou o lote inteiro é aplicado, ou nada é alterado."
  def ingest(sensor_id, batch_id, readings) do
    GenServer.call(via(sensor_id), {:ingest, batch_id, readings}, :infinity)
  end

  def stats(sensor_id), do: GenServer.call(via(sensor_id), :stats)

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(sensor_id) do
    state =
      case Store.get_sensor(sensor_id) do
        nil -> new_aggregate(sensor_id)
        persisted -> persisted
      end

    {:ok, state}
  end

  @impl true
  def handle_call({:ingest, batch_id, readings}, _from, state) do
    # Idempotência: se o mesmo lote já foi aplicado, não reaplica.
    if batch_id == state.last_batch_id do
      {:reply, {:ok, :duplicate}, state}
    else
      tx =
        Store.transaction(fn db ->
          case prepare(state, readings) do
            {:error, reason} ->
              # Rejeição de validação: registra no histórico, estado preservado.
              db = Store.log(db, state.id, batch_id, :rejected, length(readings), reason)
              {:ok, {:rejected, reason}, db}

            {:ok, new_state} ->
              # Estado e histórico são gravados juntos ou não são gravados.
              # Uma falha aqui retorna {:error, _}, e a transação é descartada.
              with :ok <- maybe_fail() do
                new_state = %{new_state | last_batch_id: batch_id}

                db =
                  db
                  |> Store.put_sensor(new_state)
                  |> Store.log(state.id, batch_id, :applied, length(readings), nil)

                {:ok, {:applied, new_state}, db}
              end
          end
        end)

      case tx do
        {:ok, {:applied, new_state}} -> {:reply, {:ok, :applied}, new_state}
        {:ok, {:rejected, reason}} -> {:reply, {:error, reason}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    end
  end

  def handle_call(:stats, _from, s) do
    avg = if s.count > 0, do: s.sum / s.count, else: nil

    stats = %{
      id: s.id,
      count: s.count,
      avg: avg,
      min: s.min,
      max: s.max,
      window_size: length(s.window)
    }

    {:reply, stats, s}
  end

  # ---------------------------------------------------------------------------
  # Lógica pura
  # ---------------------------------------------------------------------------

  defp new_aggregate(id) do
    %{id: id, count: 0, sum: 0.0, min: nil, max: nil, window: [], last_batch_id: nil}
  end

  # Valida o lote inteiro antes de qualquer alteração (tudo ou nada).
  defp prepare(state, readings) do
    case validate_all(readings) do
      :ok -> {:ok, Enum.reduce(readings, state, &accumulate/2)}
      {:error, _} = err -> err
    end
  end

  defp validate_all(readings) do
    Enum.reduce_while(readings, :ok, fn reading, :ok ->
      v = Map.get(reading, :value)

      if is_number(v) and v >= @min_valid and v <= @max_valid do
        {:cont, :ok}
      else
        {:halt, {:error, {:out_of_range, v}}}
      end
    end)
  end

  defp accumulate(%{value: v}, s) do
    %{
      s
      | count: s.count + 1,
        sum: s.sum + v,
        min: if(s.min, do: min(s.min, v), else: v),
        max: if(s.max, do: max(s.max, v), else: v),
        window: Enum.take([v | s.window], @window_size)
    }
  end

  # Injeção de falhas configurável, para demonstrar rollback.
  defp maybe_fail do
    rate = Application.get_env(:sensor_monitor, :failure_rate, 0.0)

    if :rand.uniform() < rate do
      {:error, :simulated_io_failure}
    else
      :ok
    end
  end
end

defmodule SensorMonitor do
  # Pipeline de processamento em streaming.
  #
  # Fluxo: arquivo (lazy) -> parse -> chunk -> agrupa por sensor -> ingestão
  # paralela entre sensores, com retry para falhas transitórias.

  alias SensorMonitor.{Sensor, Store}

  @batch_size 500
  @max_retries 3

  def main do
    Application.put_env(:sensor_monitor, :failure_rate, 0.02)

    children = [
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      Store,
      {DynamicSupervisor, strategy: :one_for_one, name: SensorMonitor.SensorSup}
    ]

    {:ok, _sup} = Supervisor.start_link(children, strategy: :one_for_one)

    path = Path.join(System.tmp_dir!(), "sensors_#{System.unique_integer([:positive])}.csv")

    generate(path, 200_000, 20)

    tally = process(path)

    IO.puts("== Resultado do processamento ==")
    IO.inspect(tally)

    history = Store.history()
    IO.puts("\n== Histórico ==")
    IO.inspect(Enum.frequencies_by(history, & &1.status), label: "entradas por status")
    IO.inspect(length(history), label: "total de entradas")

    IO.puts("\n== Estatísticas (amostra) ==")

    for i <- 0..2 do
      IO.inspect(Sensor.stats("sensor-#{i}"))
    end

    File.rm!(path)
  end

  # Processa o arquivo sem carregá-lo integralmente na memória.
  def process(path) do
    path
    |> File.stream!()
    |> Stream.map(&parse_line/1)
    |> Stream.filter(&match?({_, _}, &1))
    |> Stream.chunk_every(@batch_size)
    |> Stream.with_index(1)
    |> Stream.flat_map(fn {chunk, batch_no} -> dispatch(batch_no, chunk) end)
    |> Enum.reduce(%{applied: 0, rejected: 0, failed: 0}, &tally/2)
  end

  # Agrupa o chunk por sensor e processa cada grupo em paralelo.
  # Sensores diferentes são independentes entre si.
  defp dispatch(batch_no, chunk) do
    chunk
    |> Enum.group_by(fn {id, _} -> id end, fn {_, r} -> r end)
    |> Task.async_stream(
      fn {id, readings} ->
        ensure_started(id)
        batch_id = "#{batch_no}:#{id}"
        {id, with_retries(@max_retries, fn -> Sensor.ingest(id, batch_id, readings) end)}
      end,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp with_retries(attempts, fun) do
    case fun.() do
      {:error, :simulated_io_failure} when attempts > 1 -> with_retries(attempts - 1, fun)
      other -> other
    end
  end

  defp ensure_started(id) do
    case DynamicSupervisor.start_child(SensorMonitor.SensorSup, {Sensor, id}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end
  end

  defp tally({_id, {:ok, :applied}}, acc), do: %{acc | applied: acc.applied + 1}
  defp tally({_id, {:ok, :duplicate}}, acc), do: acc
  defp tally({_id, {:error, {:out_of_range, _}}}, acc), do: %{acc | rejected: acc.rejected + 1}
  defp tally({_id, {:error, _}}, acc), do: %{acc | failed: acc.failed + 1}

  # Formato: "sensor-N,timestamp,valor". Linhas inválidas são descartadas.
  defp parse_line(line) do
    case String.split(String.trim(line), ",") do
      [id, ts, value] ->
        case Float.parse(value) do
          {v, ""} -> {id, %{ts: ts, value: v}}
          _ -> :invalid
        end

      _ ->
        :invalid
    end
  end

  # Gera o arquivo de forma lazy. A cada 50_000 leituras injeta um valor
  # fora da faixa válida, para exercitar a rejeição atômica do lote.
  defp generate(path, total, sensors) do
    0..(total - 1)
    |> Stream.map(fn i ->
      sensor = "sensor-#{rem(i, sensors)}"

      value =
        if rem(i, 50_000) == 0,
          do: 999.0,
          else: :rand.uniform() * 120 - 20

      "#{sensor},#{i},#{Float.round(value, 3)}\n"
    end)
    |> Stream.into(File.stream!(path))
    |> Stream.run()
  end
end

SensorMonitor.main()
