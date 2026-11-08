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

# Arquivo único: monitoramento periódico de sensores com validação, Stream e histórico.
# Uso de exemplo (no iex):
#   {:ok, _} = SensorMonitor.start_link(period: 2_000, source: &SensorMonitor.Simulator.readings/0)
#   SensorMonitor.history()

defmodule SensorMonitor do
  @moduledoc """
  Monitora sensores periodicamente:
    1. Valida cada leitura antes de qualquer operação.
    2. Executa a operação de resumo (min/max/média por sensor) a cada período.
    3. Mantém um histórico limitado das execuções.
  """

  use GenServer

  # Tipos aceitos e faixas físicas plausíveis (lo, hi)
  @ranges %{
    temperature: {-50.0, 100.0},
    humidity: {0.0, 100.0},
    pressure: {300.0, 1100.0}
  }

  # Quantidade máxima de entradas no histórico
  @max_history 100

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o monitor. Opções: :period (ms), :source (fun/0 que retorna lista de leituras), :name."
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Retorna o histórico de execuções em ordem cronológica."
  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  # ---------------------------------------------------------------------------
  # Validação (funções puras)
  # ---------------------------------------------------------------------------

  @doc """
  Valida uma leitura. Espera um mapa com:
    :sensor_id (string não vazia), :type (átomo), :value (número), :timestamp (inteiro em ms)
  Retorna {:ok, leitura} ou {:error, motivo}.
  """
  def validate(%{sensor_id: id, type: type, value: value, timestamp: ts} = reading)
      when is_binary(id) and byte_size(id) > 0 and is_atom(type) and
             is_number(value) and is_integer(ts) do
    case Map.fetch(@ranges, type) do
      {:ok, {lo, hi}} when value >= lo and value <= hi ->
        {:ok, reading}

      {:ok, _range} ->
        {:error, {:out_of_range, id, value}}

      :error ->
        {:error, {:unknown_type, id, type}}
    end
  end

  def validate(other), do: {:error, {:malformed, other}}

  # ---------------------------------------------------------------------------
  # Operações (funções puras)
  # ---------------------------------------------------------------------------

  @doc "Calcula count, min, max e média por sensor a partir de leituras já validadas."
  def summarize(valid_readings) do
    valid_readings
    |> Enum.group_by(& &1.sensor_id)
    |> Stream.map(fn {id, readings} -> {id, stats(readings)} end)
    |> Enum.into(%{})
  end

  defp stats(readings) do
    values = Enum.map(readings, & &1.value)

    %{
      count: length(values),
      min: Enum.min(values),
      max: Enum.max(values),
      avg: Enum.sum(values) / length(values)
    }
  end

  # ---------------------------------------------------------------------------
  # Execução de um ciclo (validação -> operação -> registro)
  # ---------------------------------------------------------------------------

  defp tick(source) do
    now = System.system_time(:millisecond)

    {valid, rejected} =
      source.()
      |> Stream.map(&validate/1)
      |> Enum.reduce({[], []}, fn
        {:ok, reading}, {ok, err} -> {[reading | ok], err}
        {:error, reason}, {ok, err} -> {ok, [reason | err]}
      end)

    valid = Enum.reverse(valid)

    %{
      at: now,
      processed: length(valid),
      rejected: Enum.reverse(rejected),
      summary: summarize(valid)
    }
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    period = Keyword.get(opts, :period, 5_000)
    source = Keyword.get(opts, :source, &SensorMonitor.Simulator.readings/0)
    parent = self()

    # Pipeline periódico com Stream: a cada `period` ms executa o ciclo e envia o
    # resultado ao servidor para ser registrado no histórico.
    {:ok, _task} =
      Task.start_link(fn ->
        Stream.interval(period)
        |> Stream.map(fn _tick -> tick(source) end)
        |> Stream.each(fn entry -> GenServer.cast(parent, {:record, entry}) end)
        |> Stream.run()
      end)

    {:ok, %{history: []}}
  end

  @impl true
  def handle_cast({:record, entry}, state) do
    # Mantém apenas as últimas @max_history entradas (lista em ordem inversa)
    history = Enum.take([entry | state.history], @max_history)
    {:noreply, %{state | history: history}}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end
end

defmodule SensorMonitor.Simulator do
  @moduledoc "Fonte simulada de leituras, incluindo dados inválidos para exercitar a validação."

  @doc "Gera um lote de leituras aleatórias (algumas inválidas de propósito)."
  def readings do
    now = System.system_time(:millisecond)

    for i <- 1..6 do
      %{
        sensor_id: "sensor-#{rem(i, 3)}",
        type: Enum.random([:temperature, :humidity, :pressure, :unknown]),
        value: :rand.uniform() * 130.0 - 10.0,
        timestamp: now
      }
    end
    |> Stream.concat([%{sensor_id: "", type: :humidity, value: 50, timestamp: now}, :lixo])
    |> Enum.to_list()
  end
end
