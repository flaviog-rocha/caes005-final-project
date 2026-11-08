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

defmodule SensorMonitor do
  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name)
    server_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  def submit(server, sensor) do
    with :ok <- validate_sensor(sensor) do
      GenServer.call(server, {:submit, sensor})
    end
  end

  def history(server) do
    GenServer.call(server, :history)
  end

  def validate_sensor(%{id: id, value: value})
      when is_binary(id) and is_number(value) do
    if String.trim(id) != "", do: :ok, else: {:error, :invalid_sensor}
  end

  def validate_sensor(_sensor), do: {:error, :invalid_sensor}

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, 1_000)
    threshold = Keyword.get(opts, :threshold, 50)
    max_history = Keyword.get(opts, :max_history, 100)

    if is_integer(interval) and interval > 0 and
         is_number(threshold) and
         is_integer(max_history) and max_history > 0 do
      schedule_tick(interval)

      {:ok,
       %{
         interval: interval,
         threshold: threshold,
         max_history: max_history,
         pending: [],
         history: []
       }}
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:submit, sensor}, _from, state) do
    {:reply, :ok, %{state | pending: [sensor | state.pending]}}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_info(:tick, state) do
    entries =
      state.pending
      |> Enum.reverse()
      |> Stream.filter(&(validate_sensor(&1) == :ok))
      |> Stream.map(&perform_operation(&1, state.threshold))
      |> Enum.to_list()

    history =
      (entries ++ state.history)
      |> Enum.take(state.max_history)

    schedule_tick(state.interval)

    {:noreply, %{state | pending: [], history: history}}
  end

  defp perform_operation(sensor, threshold) do
    %{
      sensor_id: sensor.id,
      value: sensor.value,
      result: if(sensor.value >= threshold, do: :threshold_exceeded, else: :normal),
      performed_at: DateTime.utc_now()
    }
  end

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end
end
