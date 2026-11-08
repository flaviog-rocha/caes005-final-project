# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @type sensor_id :: atom() | binary()
  @type direction :: :above | :below

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Registra um sensor com um limite e a direção da condição de alerta."
  def register_sensor(server \\ __MODULE__, sensor_id, threshold, direction \\ :above) do
    GenServer.call(server, {:register_sensor, sensor_id, threshold, direction})
  end

  @doc "Envia uma leitura para processamento assíncrono e retorna o ID da operação."
  def submit_reading(server \\ __MODULE__, sensor_id, value) do
    GenServer.call(server, {:submit_reading, sensor_id, value})
  end

  @doc "Consulta o estado de uma operação."
  def operation_status(server \\ __MODULE__, operation_id) do
    GenServer.call(server, {:operation_status, operation_id})
  end

  @doc "Consulta sensores, operações em andamento, notificações e leituras persistidas."
  def current_state(server \\ __MODULE__) do
    GenServer.call(server, :current_state)
  end

  @doc "Simula falhas nas próximas gravações do banco de dados fictício."
  def inject_database_failure(server \\ __MODULE__, count \\ 1) do
    GenServer.call(server, {:inject_database_failure, count})
  end

  @impl true
  def init(opts) do
    processing_delay = Keyword.get(opts, :processing_delay, 0)

    if is_integer(processing_delay) and processing_delay >= 0 do
      {:ok,
       %{
         processing_delay: processing_delay,
         sensors: %{},
         operations: %{},
         next_operation_id: 1,
         notifications: [],
         stored_readings: [],
         failures_remaining: 0
       }}
    else
      {:stop, {:validation, :invalid_processing_delay}}
    end
  end

  @impl true
  def handle_call({:register_sensor, sensor_id, threshold, direction}, _from, state) do
    cond do
      not valid_sensor_id?(sensor_id) ->
        {:reply, {:error, {:validation, :invalid_sensor_id}}, state}

      not is_number(threshold) ->
        {:reply, {:error, {:validation, :invalid_threshold}}, state}

      direction not in [:above, :below] ->
        {:reply, {:error, {:validation, :invalid_direction}}, state}

      Map.has_key?(state.sensors, sensor_id) ->
        {:reply, {:error, {:validation, :sensor_already_registered}}, state}

      true ->
        sensor = %{
          threshold: threshold,
          direction: direction,
          last_value: nil
        }

        {:reply, :ok, put_in(state.sensors[sensor_id], sensor)}
    end
  end

  def handle_call({:submit_reading, sensor_id, value}, _from, state) do
    cond do
      not valid_sensor_id?(sensor_id) ->
        {:reply, {:error, {:validation, :invalid_sensor_id}}, state}

      not is_number(value) ->
        {:reply, {:error, {:validation, :invalid_reading}}, state}

      not Map.has_key?(state.sensors, sensor_id) ->
        {:reply, {:error, {:validation, :sensor_not_registered}}, state}

      true ->
        operation_id = state.next_operation_id
        now = timestamp()

        operation = %{
          id: operation_id,
          sensor_id: sensor_id,
          value: value,
          status: :running,
          submitted_at: now,
          completed_at: nil,
          error: nil
        }

        Process.send_after(
          self(),
          {:complete_operation, operation_id},
          state.processing_delay
        )

        state =
          state
          |> put_in([:operations, operation_id], operation)
          |> Map.put(:next_operation_id, operation_id + 1)

        {:reply, {:ok, operation_id}, state}
    end
  end

  def handle_call({:operation_status, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        {:reply, {:ok, operation}, state}

      :error ->
        {:reply, {:error, {:validation, :operation_not_found}}, state}
    end
  end

  def handle_call(:current_state, _from, state) do
    in_progress =
      state.operations
      |> Enum.filter(fn {_id, operation} -> operation.status == :running end)
      |> Enum.map(fn {_id, operation} -> operation end)

    result = %{
      sensors: state.sensors,
      operations: state.operations,
      in_progress: in_progress,
      notifications: state.notifications,
      stored_readings: state.stored_readings
    }

    {:reply, result, state}
  end

  def handle_call({:inject_database_failure, count}, _from, state) do
    if is_integer(count) and count > 0 do
      {:reply, :ok, %{state | failures_remaining: state.failures_remaining + count}}
    else
      {:reply, {:error, {:validation, :invalid_failure_count}}, state}
    end
  end

  @impl true
  def handle_info({:complete_operation, operation_id}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: :running} = operation} ->
        if state.failures_remaining > 0 do
          failed_operation = %{
            operation
            | status: :failed,
              completed_at: timestamp(),
              error: {:operation, :database_unavailable}
          }

          state =
            state
            |> put_in([:operations, operation_id], failed_operation)
            |> Map.update!(:failures_remaining, &(&1 - 1))

          {:noreply, state}
        else
          complete_successfully(operation, state)
        end

      _ ->
        {:noreply, state}
    end
  end

  defp complete_successfully(operation, state) do
    sensor = Map.fetch!(state.sensors, operation.sensor_id)

    reading = %{
      operation_id: operation.id,
      sensor_id: operation.sensor_id,
      value: operation.value,
      recorded_at: timestamp()
    }

    updated_sensor = %{sensor | last_value: operation.value}

    completed_operation = %{
      operation
      | status: :completed,
        completed_at: timestamp()
    }

    state =
      state
      |> put_in([:sensors, operation.sensor_id], updated_sensor)
      |> put_in([:operations, operation.id], completed_operation)
      |> Map.update!(:stored_readings, &(&1 ++ [reading]))

    if threshold_reached?(operation.value, sensor.threshold, sensor.direction) do
      notification = %{
        type: :threshold_reached,
        sensor_id: operation.sensor_id,
        value: operation.value,
        threshold: sensor.threshold,
        direction: sensor.direction,
        operation_id: operation.id,
        created_at: timestamp()
      }

      {:noreply, Map.update!(state, :notifications, &(&1 ++ [notification]))}
    else
      {:noreply, state}
    end
  end

  defp threshold_reached?(value, threshold, :above), do: value >= threshold
  defp threshold_reached?(value, threshold, :below), do: value <= threshold

  defp valid_sensor_id?(id), do: (is_atom(id) or is_binary(id)) and id != ""

  defp timestamp, do: System.system_time(:millisecond)
end
