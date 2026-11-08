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
#
# Monitoramento de sensores em Elixir (somente biblioteca padrão).
# Execução: elixir sensor_monitor.exs

defmodule SensorMonitor.Supervisor do
  # Árvore de supervisão: Registry (entidades nomeadas), Storage fictício
  # (Agent) e DynamicSupervisor (um processo por sensor, sob demanda).
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      SensorMonitor.Storage.Memory,
      {DynamicSupervisor, name: SensorMonitor.SensorSup, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorMonitor.Storage do
  # Contrato de armazenamento.
  # `transaction/2` executa a função sobre o registro atual. As alterações só são
  # confirmadas se a função retornar `{:ok, novo_registro, valor}`. Em caso de
  # `{:error, _}` ou exceção nada é gravado (tudo ou nada).
  # Uma implementação com Ecto poderia adotar o mesmo contrato, usando
  # `Ecto.Multi`/transações e migrações (Ecto.Migration), sem tornar isso obrigatório.
  @type id :: term()
  @type record :: map()

  @callback get(id()) :: record() | nil
  @callback transaction(id(), (record() | nil -> {:ok, record(), term()} | {:error, term()})) ::
              {:ok, term()} | {:error, term()}
end

defmodule SensorMonitor.Storage.Memory do
  # Banco de dados fictício em memória.
  # Cada chave guarda {versão, registro}. A confirmação usa compare-and-swap:
  # só grava se a versão não mudou desde a leitura, evitando escritas concorrentes
  # parciais ou perdidas.
  use Agent
  @behaviour SensorMonitor.Storage

  @max_retries 5

  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @impl true
  def get(id) do
    case Agent.get(__MODULE__, &Map.get(&1, id)) do
      {_version, record} -> record
      nil -> nil
    end
  end

  @impl true
  def transaction(id, fun), do: do_transaction(id, fun, @max_retries)

  defp do_transaction(_id, _fun, 0), do: {:error, :too_many_conflicts}

  defp do_transaction(id, fun, retries) do
    {version, record} = Agent.get(__MODULE__, &Map.get(&1, id, {0, nil}))

    case run(fun, record) do
      {:ok, new_record, value} ->
        case compare_and_swap(id, version, new_record) do
          :ok -> {:ok, value}
          :conflict -> do_transaction(id, fun, retries - 1)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A função de transação é executada fora do Agent: uma exceção não derruba
  # o banco nem deixa estado corrompido.
  defp run(fun, record) do
    case fun.(record) do
      {:ok, _, _} = ok -> ok
      {:error, _} = err -> err
      other -> {:error, {:invalid_return, other}}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end

  defp compare_and_swap(id, version, new_record) do
    Agent.get_and_update(__MODULE__, fn db ->
      case Map.get(db, id, {0, nil}) do
        {^version, _} -> {:ok, Map.put(db, id, {version + 1, new_record})}
        _ -> {:conflict, db}
      end
    end)
  end
end

defmodule SensorMonitor.Sensor do
  # Entidade independente: um processo GenServer por sensor.
  # Serializa as operações do próprio sensor sem bloquear os demais.
  use GenServer, restart: :transient

  alias SensorMonitor.Storage.Memory, as: Storage

  @default_opts [range: {-1_000.0, 1_000.0}, history_limit: 100]

  def start_link({id, opts}) do
    GenServer.start_link(__MODULE__, {id, Keyword.merge(@default_opts, opts)}, name: via(id))
  end

  def via(id), do: {:via, Registry, {SensorMonitor.Registry, id}}

  def ingest(id, readings), do: GenServer.call(via(id), {:ingest, readings}, :infinity)

  @impl true
  def init({id, opts}) do
    {:ok,
     %{
       id: id,
       range: Keyword.fetch!(opts, :range),
       history_limit: Keyword.fetch!(opts, :history_limit)
     }}
  end

  @impl true
  def handle_call({:ingest, readings}, _from, state) do
    case Storage.transaction(state.id, &apply_batch(&1, readings, state)) do
      {:ok, count} ->
        {:reply, {:ok, count}, state}

      {:error, reason} ->
        # O lote inteiro foi descartado; apenas o registro de rejeição é gravado.
        _ = Storage.transaction(state.id, &log_rejection(&1, reason, length(readings), state))
        {:reply, {:error, reason}, state}
    end
  end

  # Calcula o novo estado de forma pura. Se qualquer leitura for inválida,
  # o lote inteiro é rejeitado.
  defp apply_batch(record, readings, ctx) do
    rec = record || new_record()

    case fold(readings, rec.stats, ctx.range) do
      {:ok, stats} ->
        seq = rec.seq + 1
        entry = %{seq: seq, op: :batch_applied, count: length(readings), at: now()}

        new_rec = %{
          rec
          | stats: stats,
            seq: seq,
            history: Enum.take([entry | rec.history], ctx.history_limit)
        }

        {:ok, new_rec, length(readings)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp log_rejection(record, reason, count, ctx) do
    rec = record || new_record()
    seq = rec.seq + 1
    entry = %{seq: seq, op: :batch_rejected, count: count, reason: reason, at: now()}

    {:ok, %{rec | seq: seq, history: Enum.take([entry | rec.history], ctx.history_limit)},
     :rejected}
  end

  defp fold(readings, stats, {min_v, max_v}) do
    Enum.reduce_while(readings, {:ok, stats}, fn reading, {:ok, acc} ->
      case validate(reading, min_v, max_v) do
        :ok -> {:cont, {:ok, add(acc, reading)}}
        {:error, reason} -> {:halt, {:error, {reason, reading}}}
      end
    end)
  end

  defp validate(%{value: v, ts: _}, min_v, max_v) when is_number(v) do
    if v >= min_v and v <= max_v, do: :ok, else: {:error, :out_of_range}
  end

  defp validate(%{value: _, ts: _}, _min_v, _max_v), do: {:error, :invalid_value}
  defp validate(_, _min_v, _max_v), do: {:error, :malformed}

  defp add(%{count: c, sum: s, min: mn, max: mx} = st, %{value: v, ts: ts}) do
    %{
      st
      | count: c + 1,
        sum: s + v,
        min: if(mn == nil, do: v, else: min(mn, v)),
        max: if(mx == nil, do: v, else: max(mx, v)),
        last: %{value: v, ts: ts}
    }
  end

  defp new_record do
    %{stats: %{count: 0, sum: 0.0, min: nil, max: nil, last: nil}, seq: 0, history: []}
  end

  defp now, do: DateTime.utc_now()
end

defmodule SensorMonitor.Ingestor do
  # Consumo em streaming: `Stream.chunk_every/2` mantém em memória apenas um lote
  # por vez. Dentro de cada lote, as leituras são agrupadas por sensor e cada
  # grupo é enviado ao processo correspondente em paralelo.
  alias SensorMonitor.Sensor

  @default_batch_size 5_000
  @empty_summary %{
    applied_readings: 0,
    rejected_readings: 0,
    applied_batches: 0,
    rejected_batches: 0
  }

  def run(readings, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    sensor_opts = Keyword.get(opts, :sensor_opts, [])

    readings
    |> Stream.chunk_every(batch_size)
    |> Stream.map(&process_chunk(&1, sensor_opts))
    |> Enum.reduce(@empty_summary, fn results, acc -> Enum.reduce(results, acc, &merge/2) end)
  end

  defp process_chunk(chunk, sensor_opts) do
    chunk
    |> Enum.group_by(& &1.sensor)
    |> Task.async_stream(
      fn {id, rs} ->
        ensure_sensor(id, sensor_opts)

        case Sensor.ingest(id, rs) do
          {:ok, n} -> {:applied, n}
          {:error, _reason} -> {:rejected, length(rs)}
        end
      end,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity,
      ordered: false
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  defp ensure_sensor(id, opts) do
    case DynamicSupervisor.start_child(SensorMonitor.SensorSup, {Sensor, {id, opts}}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp merge({:applied, n}, acc) do
    %{acc | applied_batches: acc.applied_batches + 1, applied_readings: acc.applied_readings + n}
  end

  defp merge({:rejected, n}, acc) do
    %{acc | rejected_batches: acc.rejected_batches + 1, rejected_readings: acc.rejected_readings + n}
  end
end

defmodule SensorMonitor do
  # API pública e demonstração.
  alias SensorMonitor.{Ingestor, Storage.Memory}

  def start(opts \\ []), do: SensorMonitor.Supervisor.start_link(opts)

  def stats(id) do
    case Memory.get(id) do
      nil -> nil
      rec -> Map.put(rec.stats, :avg, avg(rec.stats))
    end
  end

  def history(id, n \\ 20) do
    case Memory.get(id) do
      nil -> []
      rec -> Enum.take(rec.history, n)
    end
  end

  defp avg(%{count: 0}), do: nil
  defp avg(%{count: c, sum: s}), do: s / c

  def run_demo(total \\ 1_000_000) do
    {:ok, _} = start()

    summary =
      total
      |> synthetic_readings()
      |> Ingestor.run(batch_size: 5_000, sensor_opts: [range: {-100.0, 100.0}])

    IO.inspect(summary, label: "resumo")

    for id <- ["s0", "s1"] do
      IO.inspect(stats(id), label: id)
      IO.inspect(history(id, 3), label: "historico #{id}")
    end
  end

  # Fonte lazy: gera leituras sob demanda, sem materializar a lista completa.
  # Leituras inválidas periódicas demonstram a rejeição atômica de lotes.
  defp synthetic_readings(total) do
    Stream.unfold(0, fn
      i when i >= total -> nil
      i -> {reading(i), i + 1}
    end)
  end

  defp reading(i) do
    sensor = "s#{rem(i, 50)}"
    value = if rem(i, 400_000) == 399_999, do: 1.0e9, else: :rand.uniform() * 200 - 100
    %{sensor: sensor, ts: i, value: value}
  end
end

SensorMonitor.run_demo()
