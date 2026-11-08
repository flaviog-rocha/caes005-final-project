# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Implementar expiração automática de registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: monitoramento concorrente de sensores em Elixir.
#
# Requisitos atendidos:
#   1. Operações simultâneas: cada sensor é um GenServer independente,
#      registrado no Registry, e lotes são processados com Task.async_stream.
#   2. Histórico: toda operação é registrada no processo SensorMonitor.History.
#   3. Diferenciação de erros:
#        {:error, {:validation, reason}}  -> entrada inválida, nada é executado
#        {:error, {:operational, reason}} -> falha durante a execução
#   4. Expiração automática: leituras e entradas do histórico expiram por TTL.

defmodule SensorMonitor do
  @moduledoc """
  Monitoramento concorrente de sensores.

  Uso:

      {:ok, _} = SensorMonitor.start_link()
      :ok = SensorMonitor.start_sensor("temp-1", :temperature, reading_ttl_ms: 30_000)
      {:ok, 25.5} = SensorMonitor.record("temp-1", 25.5)
      {:ok, [25.5]} = SensorMonitor.read("temp-1")
      {:error, {:validation, _}} = SensorMonitor.record("temp-1", 999.0)
      results = SensorMonitor.record_many([{"temp-1", 20.0}, {"x", 1.0}])
      {:ok, entries} = SensorMonitor.history(limit: 10)
  """

  use Supervisor

  alias SensorMonitor.{History, Sensor, Validator}

  @registry SensorMonitor.Registry
  @sensor_sup SensorMonitor.SensorSupervisor
  @default_ttl 60_000
  @default_max 100
  @call_timeout 5_000

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc """
  Inicia a árvore: Registry, DynamicSupervisor de sensores e processo de histórico.

  Opções:
    * `:history` - keyword list repassada ao `SensorMonitor.History`
      (`:ttl_ms`, `:sweep_ms`, `:max_entries`)
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @sensor_sup},
      {History, Keyword.get(opts, :history, [])}
    ]

    # rest_for_one: se o Registry cair, os sensores (dependentes dele) são reiniciados.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia um sensor sob o DynamicSupervisor.

  Opções:
    * `:reading_ttl_ms` - tempo de vida de cada leitura (padrão #{@default_ttl})
    * `:max_readings`   - número máximo de leituras mantidas (padrão #{@default_max})
  """
  def start_sensor(id, type, opts \\ []) do
    track(:start_sensor, id, fn ->
      with :ok <- Validator.sensor_id(id),
           :ok <- Validator.sensor_type(type),
           :ok <- Validator.sensor_opts(opts) do
        spec =
          {Sensor,
           id: id,
           type: type,
           reading_ttl_ms: Keyword.get(opts, :reading_ttl_ms, @default_ttl),
           max_readings: Keyword.get(opts, :max_readings, @default_max)}

        case DynamicSupervisor.start_child(@sensor_sup, spec) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> {:error, {:operational, :already_started}}
          {:error, reason} -> {:error, {:operational, reason}}
        end
      end
    end)
  end

  @doc "Encerra um sensor em execução."
  def stop_sensor(id) do
    track(:stop_sensor, id, fn ->
      with :ok <- Validator.sensor_id(id) do
        case lookup(id) do
          {:ok, pid} ->
            case DynamicSupervisor.terminate_child(@sensor_sup, pid) do
              :ok -> :ok
              {:error, reason} -> {:error, {:operational, reason}}
            end

          :error ->
            {:error, {:operational, :sensor_not_found}}
        end
      end
    end)
  end

  @doc "Registra uma leitura em um sensor."
  def record(id, value) do
    track(:record, id, fn ->
      with :ok <- Validator.sensor_id(id) do
        call(id, {:record, value})
      end
    end)
  end

  @doc "Retorna as leituras válidas (não expiradas) de um sensor."
  def read(id) do
    track(:read, id, fn ->
      with :ok <- Validator.sensor_id(id) do
        call(id, :readings)
      end
    end)
  end

  @doc """
  Executa um lote de gravações de forma concorrente.

  Recebe uma lista de `{id, value}` e retorna a lista de resultados na mesma ordem.
  Timeouts individuais são reportados como falhas operacionais.
  """
  def record_many(entries, opts \\ []) when is_list(entries) do
    timeout = Keyword.get(opts, :timeout, @call_timeout)

    results =
      entries
      |> Task.async_stream(
        fn {id, value} -> record(id, value) end,
        timeout: timeout,
        on_timeout: :kill_task,
        ordered: true
      )
      |> Enum.to_list()

    results
    |> Enum.zip(entries)
    |> Enum.map(fn
      {{:ok, result}, _entry} ->
        result

      {{:exit, reason}, {id, _value}} ->
        result = {:error, {:operational, {:exit, reason}}}
        log(:record, id, result)
        result
    end)
  end

  @doc """
  Consulta o histórico de operações.

  Opções:
    * `:sensor_id` - filtra por sensor
    * `:limit`     - máximo de entradas (padrão 100)
  """
  def history(opts \\ []), do: History.list(opts)

  # ---------------------------------------------------------------------------
  # Auxiliares privados
  # ---------------------------------------------------------------------------

  defp lookup(id) do
    case Registry.lookup(@registry, id) do
      [{pid, _value}] -> {:ok, pid}
      [] -> :error
    end
  end

  defp call(id, message) do
    case lookup(id) do
      {:ok, pid} -> GenServer.call(pid, message, @call_timeout)
      :error -> {:error, {:operational, :sensor_not_found}}
    end
  end

  # Executa a operação, converte exceções/saídas em erro operacional e registra no histórico.
  defp track(op, id, fun) do
    result = run(fun)
    log(op, id, result)
    result
  end

  defp run(fun) do
    fun.()
  catch
    kind, reason -> {:error, {:operational, {kind, reason}}}
  end

  defp log(op, id, result) do
    {status, reason} = outcome(result)
    History.append(%{op: op, sensor_id: id, status: status, reason: reason})
  end

  defp outcome(:ok), do: {:ok, nil}
  defp outcome({:ok, _}), do: {:ok, nil}
  defp outcome({:error, {:validation, reason}}), do: {:validation_error, reason}
  defp outcome({:error, {:operational, reason}}), do: {:operational_error, reason}
  defp outcome(other), do: {:operational_error, {:unexpected_result, other}}
end

# ---------------------------------------------------------------------------
# Validação pura: separa claramente erros de validação de falhas operacionais.
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Validator do
  @moduledoc false

  # Faixas válidas por tipo de sensor: {mínimo, máximo}
  @types %{
    temperature: {-50.0, 150.0},
    humidity: {0.0, 100.0},
    pressure: {300.0, 1100.0}
  }

  @max_id_bytes 64

  def sensor_id(id) when is_binary(id) and byte_size(id) in 1..@max_id_bytes, do: :ok
  def sensor_id(id), do: invalid({:invalid_sensor_id, id})

  def sensor_type(type) do
    if Map.has_key?(@types, type),
      do: :ok,
      else: invalid({:unknown_sensor_type, type})
  end

  def sensor_opts(opts) do
    if Keyword.keyword?(opts) do
      case Enum.find([:reading_ttl_ms, :max_readings], &(not valid_opt?(opts, &1))) do
        nil -> :ok
        key -> invalid({:invalid_option, key, Keyword.get(opts, key)})
      end
    else
      invalid({:invalid_opts, opts})
    end
  end

  def reading(type, value) when is_number(value) do
    {min, max} = Map.fetch!(@types, type)

    if value >= min and value <= max,
      do: :ok,
      else: invalid({:out_of_range, value, {min, max}})
  end

  def reading(_type, value), do: invalid({:not_a_number, value})

  defp valid_opt?(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> true
      {:ok, value} -> is_integer(value) and value > 0
    end
  end

  defp invalid(reason), do: {:error, {:validation, reason}}
end

# ---------------------------------------------------------------------------
# Histórico de operações com expiração automática.
# ---------------------------------------------------------------------------
defmodule SensorMonitor.History do
  @moduledoc false
  use GenServer

  @default_ttl 300_000
  @default_sweep 5_000
  @default_max 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def append(entry), do: GenServer.call(__MODULE__, {:append, entry})

  def list(opts \\ []), do: GenServer.call(__MODULE__, {:list, opts})

  @impl true
  def init(opts) do
    state = %{
      entries: [],
      seq: 0,
      ttl: Keyword.get(opts, :ttl_ms, @default_ttl),
      sweep: Keyword.get(opts, :sweep_ms, @default_sweep),
      max: Keyword.get(opts, :max_entries, @default_max)
    }

    schedule(state.sweep)
    {:ok, state}
  end

  @impl true
  def handle_call({:append, entry}, _from, state) do
    now = now()
    seq = state.seq + 1

    record =
      Map.merge(entry, %{
        seq: seq,
        at: DateTime.utc_now(),
        expires_at: now + state.ttl
      })

    entries =
      [record | live(state.entries, now)]
      |> Enum.take(state.max)

    {:reply, :ok, %{state | entries: entries, seq: seq}}
  end

  def handle_call({:list, opts}, _from, state) do
    now = now()
    limit = Keyword.get(opts, :limit, 100)

    result =
      state.entries
      |> live(now)
      |> filter_sensor(Keyword.fetch(opts, :sensor_id))
      |> Enum.take(limit)

    {:reply, {:ok, result}, %{state | entries: live(state.entries, now)}}
  end

  @impl true
  def handle_info(:sweep, state) do
    schedule(state.sweep)
    {:noreply, %{state | entries: live(state.entries, now())}}
  end

  defp filter_sensor(entries, :error), do: entries
  defp filter_sensor(entries, {:ok, id}), do: Enum.filter(entries, &(&1.sensor_id == id))

  defp live(entries, now), do: Enum.filter(entries, &(&1.expires_at > now))

  defp schedule(ms), do: Process.send_after(self(), :sweep, ms)

  defp now, do: System.monotonic_time(:millisecond)
end

# ---------------------------------------------------------------------------
# Processo de sensor: um GenServer por sensor, endereçado via Registry.
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Sensor do
  @moduledoc false
  use GenServer

  alias SensorMonitor.Validator

  @sweep_ms 1_000

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {SensorMonitor.Registry, id}})
  end

  @impl true
  def init(opts) do
    schedule()

    state = %{
      type: Keyword.fetch!(opts, :type),
      ttl: Keyword.fetch!(opts, :reading_ttl_ms),
      max: Keyword.fetch!(opts, :max_readings),
      readings: []
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:record, value}, _from, state) do
    case Validator.reading(state.type, value) do
      :ok ->
        now = now()

        reading = %{value: value, at: DateTime.utc_now(), expires_at: now + state.ttl}

        readings =
          [reading | live(state.readings, now)]
          |> Enum.take(state.max)

        {:reply, {:ok, value}, %{state | readings: readings}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:readings, _from, state) do
    readings = live(state.readings, now())
    {:reply, {:ok, Enum.map(readings, & &1.value)}, %{state | readings: readings}}
  end

  @impl true
  def handle_info(:sweep, state) do
    schedule()
    {:noreply, %{state | readings: live(state.readings, now())}}
  end

  defp live(readings, now), do: Enum.filter(readings, &(&1.expires_at > now))

  defp schedule, do: Process.send_after(self(), :sweep, @sweep_ms)

  defp now, do: System.monotonic_time(:millisecond)
end
