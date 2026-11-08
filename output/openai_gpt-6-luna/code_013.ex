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

defmodule SensorMonitor do
  @moduledoc """
  Gerencia sensores, leituras concorrentes e um histórico com expiração automática.

  Inicie o monitor com `SensorMonitor.start_link/1` antes de usar as operações.
  """

  use Supervisor

  @registry SensorMonitor.Registry
  @sensors SensorMonitor.Sensors
  @history SensorMonitor.History

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    ttl = Keyword.get(opts, :history_ttl, 60_000)
    cleanup_interval = Keyword.get(opts, :cleanup_interval, 1_000)

    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @sensors},
      {@history, ttl: ttl, cleanup_interval: cleanup_interval}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Registra um sensor identificado por um átomo, inteiro ou string não vazia."
  def register_sensor(id) do
    result =
      cond do
        not valid_id?(id) ->
          validation_error(:invalid_sensor_id)

        true ->
          case registry_lookup(id) do
            {:ok, [_ | _]} ->
              operational_error(:already_registered)

            {:ok, []} ->
              case DynamicSupervisor.start_child(@sensors, {Sensor, id}) do
                {:ok, pid} -> {:ok, pid}
                {:error, {:already_started, _pid}} -> operational_error(:already_registered)
                {:error, reason} -> operational_error(reason)
              end

            {:error, reason} ->
              operational_error(reason)
          end
      end

    record_and_return(id, :register_sensor, result)
  end

  @doc "Remove um sensor registrado."
  def unregister_sensor(id) do
    result =
      cond do
        not valid_id?(id) ->
          validation_error(:invalid_sensor_id)

        true ->
          case registry_lookup(id) do
            {:ok, [{pid, _value} | _]} ->
              case DynamicSupervisor.terminate_child(@sensors, pid) do
                :ok -> :ok
                {:error, reason} -> operational_error(reason)
              end

            {:ok, []} ->
              operational_error(:sensor_not_found)

            {:error, reason} ->
              operational_error(reason)
          end
      end

    record_and_return(id, :unregister_sensor, result)
  end

  @doc "Registra uma leitura numérica para o sensor."
  def submit_reading(id, value) do
    result =
      cond do
        not valid_id?(id) ->
          validation_error(:invalid_sensor_id)

        not is_number(value) ->
          validation_error(:reading_must_be_numeric)

        true ->
          call_sensor(id, {:submit_reading, value})
      end

    record_and_return(id, :submit_reading, result)
  end

  @doc "Obtém a leitura mais recente do sensor, ou nil se ainda não houver leitura."
  def latest_reading(id) do
    result =
      if valid_id?(id) do
        call_sensor(id, :latest_reading)
      else
        validation_error(:invalid_sensor_id)
      end

    record_and_return(id, :latest_reading, result)
  end

  @doc """
  Submete várias leituras em paralelo.

  Cada item deve ser uma tupla `{sensor_id, leitura}`. Os resultados são
  devolvidos na mesma ordem dos itens recebidos.
  """
  def submit_many(entries, opts \\ []) do
    if is_list(entries) do
      max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
      timeout = Keyword.get(opts, :timeout, 5_000)

      entries
      |> Task.async_stream(
        fn
          {id, value} -> submit_reading(id, value)
          _other -> record_and_return(nil, :submit_reading, validation_error(:invalid_entry))
        end,
        max_concurrency: max_concurrency,
        timeout: timeout,
        on_timeout: :kill_task
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, reason} -> operational_error({:task_exit, reason})
      end)
    else
      validation_error(:entries_must_be_a_list)
    end
  end

  @doc "Retorna os registros ainda válidos, em ordem cronológica."
  def history do
    try do
      GenServer.call(@history, :history)
    catch
      :exit, reason -> operational_error(reason)
    end
  end

  defp call_sensor(id, request) do
    case registry_lookup(id) do
      {:ok, [{pid, _value} | _]} ->
        try do
          GenServer.call(pid, request)
        catch
          :exit, reason -> operational_error(reason)
        end

      {:ok, []} ->
        operational_error(:sensor_not_found)

      {:error, reason} ->
        operational_error(reason)
    end
  end

  defp registry_lookup(id) do
    try do
      {:ok, Registry.lookup(@registry, id)}
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp record_and_return(id, operation, result) do
    record = %{
      sensor_id: id,
      operation: operation,
      result: result,
      occurred_at: DateTime.utc_now()
    }

    try do
      GenServer.cast(@history, {:record, record})
    catch
      :exit, _reason -> :ok
    end

    result
  end

  defp valid_id?(id) when is_binary(id), do: byte_size(id) > 0
  defp valid_id?(id) when is_atom(id), do: not is_nil(id)
  defp valid_id?(id) when is_integer(id), do: true
  defp valid_id?(_id), do: false

  defp validation_error(reason), do: {:error, {:validation, reason}}
  defp operational_error(reason), do: {:error, {:operational, reason}}

  defmodule Sensor do
    use GenServer

    def start_link(id) do
      GenServer.start_link(__MODULE__, id, name: via(id))
    end

    defp via(id) do
      {:via, Registry, {SensorMonitor.Registry, id}}
    end

    @impl true
    def init(id), do: {:ok, %{id: id, reading: nil}}

    @impl true
    def handle_call({:submit_reading, value}, _from, state) do
      {:reply, {:ok, value}, %{state | reading: value}}
    end

    def handle_call(:latest_reading, _from, state) do
      {:reply, {:ok, state.reading}, state}
    end
  end

  defmodule History do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(opts) do
      ttl = Keyword.get(opts, :ttl, 60_000)
      cleanup_interval = Keyword.get(opts, :cleanup_interval, 1_000)

      if not (is_integer(ttl) and ttl > 0 and is_integer(cleanup_interval) and cleanup_interval > 0) do
        {:stop, :invalid_history_expiration_options}
      else
        schedule_cleanup(cleanup_interval)
        {:ok, %{records: [], ttl: ttl, cleanup_interval: cleanup_interval}}
      end
    end

    @impl true
    def handle_cast({:record, record}, state) do
      now = System.system_time(:millisecond)
      entry = Map.put(record, :expires_at, now + state.ttl)
      records = [entry | remove_expired(state.records, now)]
      {:noreply, %{state | records: records}}
    end

    @impl true
    def handle_call(:history, _from, state) do
      now = System.system_time(:millisecond)
      records = remove_expired(state.records, now)
      {:reply, {:ok, Enum.reverse(records)}, %{state | records: records}}
    end

    @impl true
    def handle_info(:expire, state) do
      schedule_cleanup(state.cleanup_interval)
      now = System.system_time(:millisecond)
      {:noreply, %{state | records: remove_expired(state.records, now)}}
    end

    defp remove_expired(records, now) do
      Enum.reject(records, fn record -> record.expires_at <= now end)
    end

    defp schedule_cleanup(interval) do
      Process.send_after(self(), :expire, interval)
    end
  end
end
