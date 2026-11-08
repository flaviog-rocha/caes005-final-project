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

  @moduledoc """
  Monitora leituras de sensores e notifica inscritos quando os limites são
  atingidos ou quando uma condição de alerta é resolvida.

  As notificações são enviadas como mensagens:
    {:sensor_monitor, :alert, alerta}
    {:sensor_monitor, :resolved, alerta}

  Leituras inválidas retornam erros `:validation`; falhas na comunicação com
  o processo retornam erros `:operational`.
  """

  @type sensor_id :: binary()
  @type reading :: %{required(:sensor_id) => sensor_id(), required(:value) => number()}

  def start_link(opts \\ []) do
    with :ok <- validate_options(opts) do
      thresholds = Keyword.get(opts, :thresholds, %{})
      GenServer.start_link(__MODULE__, thresholds, Keyword.take(opts, [:name]))
    end
  end

  @spec ingest(GenServer.server(), reading()) :: {:ok, map()} | {:error, term()}
  def ingest(server, reading) do
    with {:ok, sensor_id, value} <- validate_reading(reading) do
      safe_call(server, {:ingest, sensor_id, value})
    end
  end

  @doc "Retorna as últimas leituras e as operações de alerta ainda em andamento."
  def status(server), do: safe_call(server, :status)

  @doc "Inscreve o processo chamador para receber notificações."
  def subscribe(server) do
    safe_call(server, {:subscribe, self()})
  end

  @doc "Remove a inscrição do processo chamador."
  def unsubscribe(server) do
    safe_call(server, {:unsubscribe, self()})
  end

  @impl true
  def init(thresholds) do
    {:ok,
     %{
       thresholds: thresholds,
       latest: %{},
       active: %{},
       subscribers: MapSet.new(),
       next_operation_id: 1
     }}
  end

  @impl true
  def handle_call({:ingest, sensor_id, value}, _from, state) do
    sampled_at = System.system_time(:millisecond)
    latest_reading = %{sensor_id: sensor_id, value: value, sampled_at: sampled_at}
    latest = Map.put(state.latest, sensor_id, latest_reading)

    {active, event, next_operation_id} =
      update_operation(state, sensor_id, value, sampled_at)

    next_state = %{
      state
      | latest: latest,
        active: active,
        next_operation_id: next_operation_id
    }

    if event do
      notify_subscribers(next_state.subscribers, event)
    end

    result = %{reading: latest_reading, notification: event}
    {:reply, {:ok, result}, next_state}
  end

  def handle_call(:status, _from, state) do
    operations =
      state.active
      |> Map.values()
      |> Enum.sort_by(& &1.sensor_id)

    result = %{
      latest_readings: state.latest,
      operations: operations,
      operation_count: length(operations)
    }

    {:reply, {:ok, result}, state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, subscriber)}}
  end

  def handle_call({:unsubscribe, subscriber}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.delete(state.subscribers, subscriber)}}
  end

  defp update_operation(state, sensor_id, value, sampled_at) do
    threshold = Map.get(state.thresholds, sensor_id)
    operation = Map.get(state.active, sensor_id)

    cond do
      is_nil(threshold) ->
        {state.active, nil, state.next_operation_id}

      value >= threshold and is_nil(operation) ->
        new_operation = %{
          operation_id: state.next_operation_id,
          sensor_id: sensor_id,
          threshold: threshold,
          latest_value: value,
          started_at: sampled_at
        }

        event = {:sensor_monitor, :alert, new_operation}

        {
          Map.put(state.active, sensor_id, new_operation),
          event,
          state.next_operation_id + 1
        }

      value >= threshold ->
        updated_operation = %{operation | latest_value: value}
        {Map.put(state.active, sensor_id, updated_operation), nil, state.next_operation_id}

      not is_nil(operation) ->
        resolved_operation = Map.put(operation, :ended_at, sampled_at)
        event = {:sensor_monitor, :resolved, resolved_operation}

        {
          Map.delete(state.active, sensor_id),
          event,
          state.next_operation_id
        }

      true ->
        {state.active, nil, state.next_operation_id}
    end
  end

  defp notify_subscribers(subscribers, event) do
    Enum.each(subscribers, &send(&1, event))
  end

  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case Keyword.get(opts, :thresholds, %{}) do
        thresholds when is_map(thresholds) ->
          if Enum.all?(thresholds, fn {sensor_id, threshold} ->
               valid_sensor_id?(sensor_id) and is_number(threshold)
             end) do
            :ok
          else
            {:error, {:validation, :invalid_thresholds}}
          end

        _ ->
          {:error, {:validation, :thresholds_must_be_a_map}}
      end
    else
      {:error, {:validation, :options_must_be_a_keyword_list}}
    end
  end

  defp validate_options(_), do: {:error, {:validation, :options_must_be_a_keyword_list}}

  defp validate_reading(reading) when is_map(reading) do
    with {:ok, sensor_id} <- fetch_reading_field(reading, :sensor_id),
         true <- valid_sensor_id?(sensor_id) || {:error, {:validation, :invalid_sensor_id}},
         {:ok, value} <- fetch_reading_field(reading, :value),
         true <- is_number(value) || {:error, {:validation, :value_must_be_numeric}} do
      {:ok, sensor_id, value}
    end
  end

  defp validate_reading(_), do: {:error, {:validation, :reading_must_be_a_map}}

  defp fetch_reading_field(reading, field) do
    case Map.fetch(reading, field) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:validation, {:missing_field, field}}}
    end
  end

  defp valid_sensor_id?(sensor_id) do
    is_binary(sensor_id) and byte_size(sensor_id) > 0
  end

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason ->
        {:error, {:operational, {:server_call_failed, reason}}}
    rescue
      error ->
        {:error, {:operational, {:request_failed, error.__struct__}}}
    end
  end
end
