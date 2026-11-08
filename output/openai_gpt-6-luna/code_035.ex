# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitora leituras de sensores com limite de concorrência, persistência,
  notificações e novas tentativas para falhas temporárias.
  """

  use Supervisor

  @registry __MODULE__.Registry
  @task_supervisor __MODULE__.TaskSupervisor
  @server __MODULE__.Server

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :duplicate, name: @registry},
      {Task.Supervisor, name: @task_supervisor},
      {@server, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Enfileira uma leitura e aguarda seu processamento."
  def submit(sensor_id, value) do
    GenServer.call(@server, {:submit, sensor_id, value}, :infinity)
  end

  @doc "Retorna a última leitura persistida para o sensor."
  def read(sensor_id) do
    GenServer.call(@server, {:read, sensor_id})
  end

  @doc "Registra o processo chamador para receber alertas do sensor."
  def subscribe(sensor_id) do
    if valid_sensor_id?(sensor_id) do
      Registry.register(@registry, sensor_id, nil)
    else
      {:error, {:validation, :invalid_sensor_id}}
    end
  end

  @doc "Remove o registro de notificações do processo chamador."
  def unsubscribe(sensor_id) do
    if valid_sensor_id?(sensor_id) do
      Registry.unregister(@registry, sensor_id)
    else
      {:error, {:validation, :invalid_sensor_id}}
    end
  end

  defp valid_sensor_id?(sensor_id), do: not is_nil(sensor_id)

  defmodule Server do
    use GenServer

    @registry SensorMonitor.Registry
    @task_supervisor SensorMonitor.TaskSupervisor
    @table :sensor_monitor_dets

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: SensorMonitor.Server)
    end

    @impl true
    def init(opts) do
      with {:ok, config} <- validate_options(opts),
           {:ok, table} <- open_table(config.file) do
        readings =
          :dets.foldl(
            fn {sensor_id, reading}, acc -> Map.put(acc, sensor_id, reading) end,
            %{},
            table
          )

        {:ok, Map.merge(config, %{table: table, readings: readings, queue: :queue.new(), active: %{}})}
      else
        {:error, {:validation, reason}} -> {:stop, {:validation, reason}}
        {:error, reason} -> {:stop, {:operational, reason}}
      end
    end

    @impl true
    def handle_call({:submit, sensor_id, value}, from, state) do
      cond do
        not valid_sensor_id?(sensor_id) ->
          {:reply, {:error, {:validation, :invalid_sensor_id}}, state}

        not is_number(value) ->
          {:reply, {:error, {:validation, :reading_must_be_numeric}}, state}

        true ->
          state = %{state | queue: :queue.in({sensor_id, value, from}, state.queue)}
          {:noreply, dispatch(state)}
      end
    end

    def handle_call({:read, sensor_id}, _from, state) do
      if valid_sensor_id?(sensor_id) do
        case Map.fetch(state.readings, sensor_id) do
          {:ok, reading} -> {:reply, {:ok, reading}, state}
          :error -> {:reply, {:error, :not_found}, state}
        end
      else
        {:reply, {:error, {:validation, :invalid_sensor_id}}, state}
      end
    end

    @impl true
    def handle_info({:completed, token, result}, state) do
      case Map.pop(state.active, token) do
        {nil, _active} ->
          {:noreply, state}

        {%{from: from, sensor_id: sensor_id, value: value}, active} ->
          state = %{state | active: active}

          case result do
            :ok ->
              timestamp = System.system_time(:millisecond)
              reading = %{sensor_id: sensor_id, value: value, timestamp: timestamp}

              case persist(state.table, sensor_id, reading) do
                :ok ->
                  GenServer.reply(from, {:ok, reading})
                  notify_if_needed(state.threshold, reading)
                  readings = Map.put(state.readings, sensor_id, reading)
                  {:noreply, dispatch(%{state | readings: readings})}

                {:error, reason} ->
                  GenServer.reply(from, {:error, {:operational, reason}})
                  {:noreply, dispatch(state)}
              end

            {:error, reason} ->
              GenServer.reply(from, {:error, reason})
              {:noreply, dispatch(state)}
          end
      end
    end

    @impl true
    def terminate(_reason, state) do
      :dets.close(state.table)
      :ok
    end

    defp dispatch(state) do
      if map_size(state.active) >= state.max_concurrency do
        state
      else
        case :queue.out(state.queue) do
          {:empty, _queue} ->
            state

          {{:value, {sensor_id, value, from}}, queue} ->
            token = make_ref()
            server = self()
            operation = state.operation
            retries = state.retries
            retry_delay = state.retry_delay

            child =
              fn ->
                result = run_with_retries(operation, sensor_id, value, retries, retry_delay)
                send(server, {:completed, token, result})
              end

            case Task.Supervisor.start_child(@task_supervisor, child) do
              {:ok, _pid} ->
                job = %{from: from, sensor_id: sensor_id, value: value}
                active = Map.put(state.active, token, job)
                dispatch(%{state | queue: queue, active: active})

              {:error, reason} ->
                GenServer.reply(from, {:error, {:operational, reason}})
                dispatch(%{state | queue: queue})
            end
        end
      end
    end

    defp run_with_retries(operation, sensor_id, value, retries, retry_delay) do
      result =
        try do
          normalize_result.(operation.(sensor_id, value))
        rescue
          exception -> {:temporary, {:exception, Exception.message(exception)}}
        catch
          kind, reason -> {:temporary, {kind, reason}}
        end

      case result do
        :ok ->
          :ok

        {:validation, reason} ->
          {:error, {:validation, reason}}

        {:temporary, reason} when retries > 0 ->
          Process.sleep(retry_delay)
          run_with_retries(operation, sensor_id, value, retries - 1, retry_delay)

        {:temporary, reason} ->
          {:error, {:operational, {:temporary_failure, reason}}}

        {:operational, reason} ->
          {:error, {:operational, reason}}
      end
    end

    defp normalize_result do
      fn
        :ok -> :ok
        {:ok, _result} -> :ok
        {:error, {:validation, reason}} -> {:validation, reason}
        {:error, {:temporary, reason}} -> {:temporary, reason}
        {:error, {:operational, reason}} -> {:operational, reason}
        {:error, reason} -> {:operational, reason}
        other -> {:operational, {:invalid_operation_result, other}}
      end
    end

    defp persist(table, sensor_id, reading) do
      case :dets.insert(table, {sensor_id, reading}) do
        :ok ->
          case :dets.sync(table) do
            :ok -> :ok
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    rescue
      exception -> {:error, Exception.message(exception)}
    end

    defp notify_if_needed(threshold, %{sensor_id: sensor_id, value: value} = reading) do
      if value >= threshold do
        Registry.dispatch(@registry, sensor_id, fn subscribers ->
          Enum.each(subscribers, fn {pid, _metadata} ->
            send(pid, {:sensor_alert, reading})
          end)
        end)
      end
    end

    defp open_table(file) do
      case :dets.open_file(@table, file: String.to_charlist(file), type: :set) do
        {:ok, table} -> {:ok, table}
        {:error, reason} -> {:error, reason}
      end
    end

    defp validate_options(opts) do
      config = %{
        max_concurrency: Keyword.get(opts, :max_concurrency, 4),
        retries: Keyword.get(opts, :retries, 3),
        retry_delay: Keyword.get(opts, :retry_delay, 100),
        threshold: Keyword.get(opts, :threshold, 100),
        operation: Keyword.get(opts, :operation, fn _sensor_id, _value -> :ok end),
        file: Keyword.get(opts, :file, "sensor_monitor.dets")
      }

      cond do
        not (is_integer(config.max_concurrency) and config.max_concurrency > 0) ->
          {:error, {:validation, :max_concurrency_must_be_positive}}

        not (is_integer(config.retries) and config.retries >= 0) ->
          {:error, {:validation, :retries_must_be_non_negative}}

        not (is_integer(config.retry_delay) and config.retry_delay >= 0) ->
          {:error, {:validation, :retry_delay_must_be_non_negative}}

        not is_number(config.threshold) ->
          {:error, {:validation, :threshold_must_be_numeric}}

        not is_function(config.operation, 2) ->
          {:error, {:validation, :operation_must_accept_two_arguments}}

        not is_binary(config.file) ->
          {:error, {:validation, :file_must_be_a_string}}

        true ->
          {:ok, config}
      end
    end

    defp valid_sensor_id?(sensor_id), do: not is_nil(sensor_id)
  end
end
