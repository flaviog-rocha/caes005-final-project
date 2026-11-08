# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @table :sensor_monitor_records
  @call_timeout 5_000

  @type sensor_id :: binary() | integer()
  @type sensor :: %{
          required(:id) => sensor_id(),
          required(:value) => number(),
          required(:timestamp) => non_neg_integer()
        }

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put(opts, :name, __MODULE__))
  end

  @spec create(map()) :: {:ok, sensor()} | {:error, term()}
  def create(attrs) do
    with {:ok, record} <- validate_new_sensor(attrs) do
      call_server({:create, record})
    end
  end

  @spec get(sensor_id()) :: {:ok, sensor()} | {:error, term()}
  def get(id) do
    with :ok <- validate_id(id) do
      call_server({:get, id})
    end
  end

  @spec update(sensor_id(), map()) :: {:ok, sensor()} | {:error, term()}
  def update(id, attrs) do
    with :ok <- validate_id(id),
         {:ok, changes} <- validate_changes(attrs) do
      call_server({:update, id, changes})
    end
  end

  @spec delete(sensor_id()) :: {:ok, sensor()} | {:error, term()}
  def delete(id) do
    with :ok <- validate_id(id) do
      call_server({:delete, id})
    end
  end

  @impl true
  def init(:ok) do
    table =
      :ets.new(@table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, table}
  end

  @impl true
  def handle_call({:create, sensor}, _from, table) do
    result =
      if :ets.insert_new(table, {sensor.id, sensor}) do
        {:ok, sensor}
      else
        operation_error(:already_exists)
      end

    {:reply, result, table}
  end

  def handle_call({:get, id}, _from, table) do
    result =
      case :ets.lookup(table, id) do
        [{^id, sensor}] -> {:ok, sensor}
        [] -> operation_error(:not_found)
      end

    {:reply, result, table}
  end

  def handle_call({:update, id, changes}, _from, table) do
    result =
      case :ets.lookup(table, id) do
        [{^id, sensor}] ->
          updated = Map.merge(sensor, changes)
          true = :ets.insert(table, {id, updated})
          {:ok, updated}

        [] ->
          operation_error(:not_found)
      end

    {:reply, result, table}
  end

  def handle_call({:delete, id}, _from, table) do
    result =
      case :ets.lookup(table, id) do
        [{^id, sensor}] ->
          true = :ets.delete(table, id)
          {:ok, sensor}

        [] ->
          operation_error(:not_found)
      end

    {:reply, result, table}
  end

  defp validate_new_sensor(attrs) when is_map(attrs) do
    allowed_keys = [:id, :value, :timestamp]

    with :ok <- reject_unknown_keys(attrs, allowed_keys),
         {:ok, id} <- fetch_required(attrs, :id),
         :ok <- validate_id(id),
         {:ok, value} <- fetch_required(attrs, :value),
         :ok <- validate_value(value),
         {:ok, timestamp} <- validate_timestamp(Map.get(attrs, :timestamp, now())) do
      {:ok, %{id: id, value: value, timestamp: timestamp}}
    end
  end

  defp validate_new_sensor(_attrs), do: validation_error(:expected_map)

  defp validate_changes(attrs) when is_map(attrs) do
    allowed_keys = [:value, :timestamp]

    with :ok <- reject_unknown_keys(attrs, allowed_keys),
         :ok <- reject_empty_changes(attrs),
         :ok <- validate_optional_value(attrs),
         :ok <- validate_optional_timestamp(attrs) do
      {:ok, attrs}
    end
  end

  defp validate_changes(_attrs), do: validation_error(:expected_map)

  defp validate_optional_value(attrs) do
    case Map.fetch(attrs, :value) do
      {:ok, value} -> validate_value(value)
      :error -> :ok
    end
  end

  defp validate_optional_timestamp(attrs) do
    case Map.fetch(attrs, :timestamp) do
      {:ok, timestamp} -> validate_timestamp(timestamp)
      :error -> :ok
    end
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(id) when is_integer(id), do: :ok
  defp validate_id(_id), do: validation_error(:invalid_id)

  defp validate_value(value) when is_number(value), do: :ok
  defp validate_value(_value), do: validation_error(:invalid_value)

  defp validate_timestamp(timestamp)
       when is_integer(timestamp) and timestamp >= 0,
       do: :ok

  defp validate_timestamp(_timestamp), do: validation_error(:invalid_timestamp)

  defp fetch_required(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> validation_error({:missing_field, key})
    end
  end

  defp reject_unknown_keys(attrs, allowed_keys) do
    case Map.keys(attrs) -- allowed_keys do
      [] -> :ok
      [key | _] -> validation_error({:unknown_field, key})
    end
  end

  defp reject_empty_changes(attrs) do
    if map_size(attrs) > 0 do
      :ok
    else
      validation_error(:empty_update)
    end
  end

  defp call_server(message) do
    with :ok <- ensure_started() do
      try do
        GenServer.call(__MODULE__, message, @call_timeout)
      catch
        :exit, reason -> operation_error({:server_unavailable, reason})
      rescue
        error -> operation_error({:server_failure, error})
      end
    end
  end

  defp ensure_started do
    case GenServer.start(__MODULE__, :ok, name: __MODULE__) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> operation_error({:server_start_failed, reason})
    end
  rescue
    error -> operation_error({:server_start_failed, error})
  end

  defp now, do: System.system_time(:millisecond)

  defp validation_error(reason), do: {:error, {:validation, reason}}
  defp operation_error(reason), do: {:error, {:operation, reason}}
end
