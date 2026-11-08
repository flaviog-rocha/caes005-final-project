# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Monitor de sensores: valida leituras, executa ciclos periódicos e mantém histórico.
# Arquivo autocontido, usando apenas a biblioteca padrão do Elixir (GenServer, Stream, Enum).

defmodule SensorMonitor.Validator do
  @moduledoc """
  Valida leituras de sensores antes de qualquer operação.
  Uma leitura válida é um mapa com `sensor_id` (string não vazia),
  `type` (átomo conhecido), `value` (número dentro da faixa) e `timestamp` (DateTime).
  """

  # Faixas físicas aceitas por tipo de sensor
  @ranges %{
    temperature: {-40.0, 85.0},
    humidity: {0.0, 100.0},
    pressure: {300.0, 1100.0}
  }

  @doc "Retorna {:ok, leitura} ou {:error, leitura, motivo}."
  def validate(%{sensor_id: id, type: type, value: value, timestamp: %DateTime{}} = reading)
      when is_binary(id) and byte_size(id) > 0 and is_number(value) do
    case Map.fetch(@ranges, type) do
      {:ok, {min, max}} when value >= min and value <= max -> {:ok, reading}
      {:ok, _} -> {:error, reading, :out_of_range}
      :error -> {:error, reading, :unknown_type}
    end
  end

  def validate(reading), do: {:error, reading, :malformed}
end

defmodule SensorMonitor.Simulator do
  @moduledoc """
  Fonte de dados simulada. Gera leituras aleatórias e, ocasionalmente,
  leituras inválidas para demonstrar a validação.
  """

  @sensors [
    {"t-01", :temperature, 22.0, 6.0},
    {"h-01", :humidity, 50.0, 20.0},
    {"p-01", :pressure, 1013.0, 10.0}
  ]

  @doc "Retorna uma lista de leituras brutas (sem validação)."
  def read do
    @sensors
    |> Stream.map(fn {id, type, base, spread} ->
      value =
        if :rand.uniform() < 0.1 do
          # Injeta ruído: valor ausente, para testar a validação
          nil
        else
          base + (:rand.uniform() - 0.5) * spread
        end

      %{sensor_id: id, type: type, value: value, timestamp: DateTime.utc_now()}
    end)
    |> Enum.to_list()
  end
end

defmodule SensorMonitor do
  @moduledoc """
  GenServer que executa periodicamente um ciclo de monitoramento:

    1. Obtém leituras brutas da fonte configurada.
    2. Valida cada leitura (via Stream) antes de qualquer operação.
    3. Agrega as leituras válidas por tipo de sensor (via Stream).
    4. Registra o resultado do ciclo no histórico (limitado a `:max_history`).

  Uso:
      {:ok, _pid} = SensorMonitor.start_link(interval: 2_000)
      SensorMonitor.history()
  """

  use GenServer

  @default_interval 1_000
  @default_max_history 100

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o monitor.

  Opções:
    * `:name` - nome do processo (padrão: `__MODULE__`)
    * `:interval` - intervalo entre ciclos em ms (padrão: #{@default_interval})
    * `:source` - função zero-arity que retorna leituras brutas
    * `:max_history` - quantidade máxima de entradas no histórico
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Retorna o histórico de ciclos, do mais antigo para o mais recente."
  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  @doc "Dispara um ciclo imediatamente, sem alterar o agendamento periódico."
  def run_now(server \\ __MODULE__) do
    GenServer.cast(server, :run_now)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    state = %{
      interval: Keyword.get(opts, :interval, @default_interval),
      source: Keyword.get(opts, :source, &SensorMonitor.Simulator.read/0),
      max_history: Keyword.get(opts, :max_history, @default_max_history),
      # Histórico armazenado do mais recente para o mais antigo (prepend é O(1))
      history: []
    }

    schedule(state.interval)
    {:ok, state}
  end

  @impl true
  def handle_info(:cycle, state) do
    schedule(state.interval)
    {:noreply, record_cycle(state)}
  end

  @impl true
  def handle_cast(:run_now, state) do
    {:noreply, record_cycle(state)}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp schedule(interval) do
    Process.send_after(self(), :cycle, interval)
  end

  # Executa um ciclo e insere o resultado no histórico, respeitando o limite
  defp record_cycle(state) do
    entry = run_cycle(state.source)

    history =
      [entry | state.history]
      |> Enum.take(state.max_history)

    %{state | history: history}
  end

  # Executa um ciclo completo de monitoramento e devolve o registro da operação
  defp run_cycle(source) do
    started_at = System.monotonic_time(:millisecond)

    {accepted, rejected} =
      source.()
      |> process_readings()

    summary = summarize(accepted)

    %{
      at: DateTime.utc_now(),
      duration_ms: System.monotonic_time(:millisecond) - started_at,
      accepted_count: length(accepted),
      rejected_count: length(rejected),
      rejected: rejected,
      summary: summary
    }
  end

  # Pipeline lazy: valida cada leitura antes de qualquer operação
  defp process_readings(raw_readings) do
    validated =
      raw_readings
      |> Stream.map(&SensorMonitor.Validator.validate/1)
      |> Enum.to_list()

    accepted =
      validated
      |> Stream.filter(&match?({:ok, _}, &1))
      |> Stream.map(fn {:ok, reading} -> reading end)
      |> Enum.to_list()

    rejected =
      validated
      |> Stream.filter(&match?({:error, _, _}, &1))
      |> Stream.map(fn {:error, reading, reason} -> %{reason: reason, raw: reading} end)
      |> Enum.to_list()

    {accepted, rejected}
  end

  # Agrega leituras válidas por tipo: contagem, média, mínimo e máximo
  defp summarize(readings) do
    readings
    |> Stream.map(&{&1.type, &1.value})
    |> Enum.group_by(fn {type, _} -> type end, fn {_, value} -> value end)
    |> Map.new(fn {type, values} ->
      count = length(values)

      stats = %{
        count: count,
        avg: Float.round(Enum.sum(values) / count, 2),
        min: Enum.min(values),
        max: Enum.max(values)
      }

      {type, stats}
    end)
  end
end
