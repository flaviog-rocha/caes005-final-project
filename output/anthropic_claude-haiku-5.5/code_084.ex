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

# Arquivo único: monitoramento de sensores com ETS.
# Erros são classificados como:
#   {:error, {:validation, [{campo, mensagem}]}}  -> dados de entrada inválidos (cliente)
#   {:error, {:operational, motivo}}              -> falha na operação (store, timeout, crash, não encontrado, conflito)

defmodule SensorMonitor do
  @moduledoc """
  Monitoramento de sensores baseado em ETS.

  * Dados são validados antes de qualquer operação no store.
  * Cada sensor é uma entidade independente: leituras são processadas em
    tarefas isoladas, e a falha de uma não afeta as demais.
  * Erros de validação e falhas operacionais são retornados em formatos distintos.
  * Operações CRUD: create/1, get/1, list/0, update/2, delete/1.
  * Leituras: process_readings/2.
  """

  use GenServer

  @table :sensor_monitor_sensors
  @default_timeout 5_000

  # ---------------------------------------------------------------------------
  # Ciclo de vida
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo dono da tabela ETS."
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    # Tabela pública com leitura/escrita concorrentes: os chamadores acessam
    # diretamente o ETS. O GenServer apenas mantém o ciclo de vida da tabela.
    :ets.new(@table, [
      :set,
      :public,
      :named_table,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end

  # ---------------------------------------------------------------------------
  # CRUD
  # ---------------------------------------------------------------------------

  @doc "Cadastra um sensor. Atributos: :id, :type, :location, :min, :max."
  def create(attrs) do
    with {:ok, {id, config}} <- Validator.sensor(attrs),
         :ok <- insert_new(id, config) do
      {:ok, id}
    end
  end

  @doc "Consulta um sensor pelo id."
  def get(id) do
    with :ok <- Validator.id(id),
         {:ok, {rid, config, last, count}} <- fetch_raw(id) do
      {:ok, to_map(rid, config, last, count)}
    end
  end

  @doc "Lista todos os sensores cadastrados."
  def list do
    guard_store(fn ->
      sensors = for {id, config, last, count} <- :ets.tab2list(@table), do: to_map(id, config, last, count)
      {:ok, sensors}
    end)
  end

  @doc "Atualiza parcialmente um sensor. Campos permitidos: :type, :location, :min, :max."
  def update(id, attrs) do
    with :ok <- Validator.id(id),
         :ok <- Validator.update_attrs(attrs),
         {:ok, {_, current, _, _}} <- fetch_raw(id),
         {:ok, {_, config}} <- Validator.sensor(current |> Map.merge(attrs) |> Map.put(:id, id)),
         :ok <- replace_config(id, config) do
      {:ok, id}
    end
  end

  @doc "Remove um sensor pelo id."
  def delete(id) do
    with :ok <- Validator.id(id) do
      guard_store(fn ->
        case :ets.take(@table, id) do
          [_] -> :ok
          [] -> not_found(id)
        end
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Processamento de leituras
  # ---------------------------------------------------------------------------

  @doc """
  Processa uma lista de leituras no formato `%{id: id, value: valor}`.

  Cada leitura é executada em uma tarefa isolada. Retorna uma lista de
  `{id, {:ok, valor} | {:error, ...}}` na mesma ordem da entrada.
  """
  def process_readings(readings, opts \\ []) when is_list(readings) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    readings
    |> Task.async_stream(&safe_process/1,
      timeout: timeout,
      on_timeout: :kill_task,
      ordered: true,
      max_concurrency: System.schedulers_online()
    )
    |> Enum.zip(readings)
    |> Enum.map(fn
      {{:ok, result}, _reading} ->
        result

      {{:exit, :timeout}, reading} ->
        {reading_id(reading), {:error, {:operational, :timeout}}}

      {{:exit, reason}, reading} ->
        {reading_id(reading), {:error, {:operational, {:exit, reason}}}}
    end)
  end

  # ---------------------------------------------------------------------------
  # Funções internas
  # ---------------------------------------------------------------------------

  defp safe_process(reading) do
    process_one(reading)
  catch
    kind, reason -> {reading_id(reading), {:error, {:operational, {kind, reason}}}}
  end

  defp process_one(reading) when is_map(reading) do
    id = Map.get(reading, :id)
    value = Map.get(reading, :value)

    result =
      with :ok <- Validator.id(id),
           {:ok, config} <- fetch_config(id),
           :ok <- Validator.reading(value, config),
           :ok <- record(id, value) do
        {:ok, value}
      end

    {id, result}
  end

  defp process_one(_reading) do
    {nil, {:error, {:validation, [{:reading, "deve ser um mapa com :id e :value"}]}}}
  end

  defp reading_id(reading) when is_map(reading), do: Map.get(reading, :id)
  defp reading_id(_reading), do: nil

  # Registra a leitura de forma atômica: atualiza o último valor e incrementa o contador.
  defp record(id, value) do
    guard_store(fn ->
      if :ets.update_element(@table, id, {3, value}) do
        :ets.update_counter(@table, id, {4, 1})
        :ok
      else
        not_found(id)
      end
    end)
  end

  defp insert_new(id, config) do
    guard_store(fn ->
      if :ets.insert_new(@table, {id, config, nil, 0}) do
        :ok
      else
        {:error, {:operational, {:already_exists, id}}}
      end
    end)
  end

  defp replace_config(id, config) do
    guard_store(fn ->
      if :ets.update_element(@table, id, {2, config}), do: :ok, else: not_found(id)
    end)
  end

  defp fetch_raw(id) do
    guard_store(fn ->
      case :ets.lookup(@table, id) do
        [obj] -> {:ok, obj}
        [] -> not_found(id)
      end
    end)
  end

  defp fetch_config(id) do
    with {:ok, {_, config, _, _}} <- fetch_raw(id), do: {:ok, config}
  end

  defp to_map(id, config, last, count) do
    Map.merge(config, %{id: id, last_value: last, reading_count: count})
  end

  defp not_found(id), do: {:error, {:operational, {:not_found, id}}}

  # Converte falhas de acesso ao ETS (ex.: tabela inexistente) em erro operacional.
  defp guard_store(fun) do
    fun.()
  rescue
    ArgumentError -> {:error, {:operational, :store_unavailable}}
  end
end

defmodule SensorMonitor.Validator do
  @moduledoc """
  Validações puras. Nenhuma função aqui acessa o store.
  Retornos: :ok | {:ok, dados} | {:error, {:validation, [{campo, mensagem}]}}.
  """

  @types [:temperature, :humidity, :pressure]
  @updatable [:type, :location, :min, :max]

  @doc "Valida atributos completos de um sensor e devolve `{id, config}`."
  def sensor(attrs) when is_map(attrs) do
    id = Map.get(attrs, :id)
    type = Map.get(attrs, :type)
    location = Map.get(attrs, :location)
    min = Map.get(attrs, :min)
    max = Map.get(attrs, :max)

    errors =
      []
      |> check(:id, is_binary(id) and byte_size(id) > 0, "deve ser uma string não vazia")
      |> check(:type, type in @types, "deve ser um de #{inspect(@types)}")
      |> check(:location, is_binary(location) and byte_size(location) > 0, "deve ser uma string não vazia")
      |> check(:min, is_number(min), "deve ser numérico")
      |> check(:max, is_number(max), "deve ser numérico")
      |> check(:range, is_number(min) and is_number(max) and min < max, "min deve ser menor que max")

    case errors do
      [] -> {:ok, {id, %{type: type, location: location, min: min, max: max}}}
      _ -> {:error, {:validation, Enum.reverse(errors)}}
    end
  end

  def sensor(_attrs), do: {:error, {:validation, [{:attrs, "deve ser um mapa"}]}}

  @doc "Valida o identificador de um sensor."
  def id(id) when is_binary(id) and byte_size(id) > 0, do: :ok

  def id(_id), do: {:error, {:validation, [{:id, "deve ser uma string não vazia"}]}}

  @doc "Valida os campos de uma atualização parcial."
  def update_attrs(attrs) when is_map(attrs) do
    case Map.keys(attrs) -- @updatable do
      [] -> :ok
      invalid -> {:error, {:validation, [{:attrs, "campos não permitidos: #{inspect(invalid)}"}]}}
    end
  end

  def update_attrs(_attrs), do: {:error, {:validation, [{:attrs, "deve ser um mapa"}]}}

  @doc "Valida uma leitura contra a configuração do sensor."
  def reading(value, %{min: min, max: max}) do
    cond do
      not is_number(value) ->
        {:error, {:validation, [{:value, "deve ser numérico"}]}}

      value < min or value > max ->
        {:error, {:validation, [{:value, "fora do intervalo [#{min}, #{max}]"}]}}

      true ->
        :ok
    end
  end

  defp check(errors, field, valid?, message) do
    if valid?, do: errors, else: [{field, message} | errors]
  end
end
