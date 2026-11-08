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

  @moduledoc """
  Gerencia registros de sensores em uma tabela ETS.

  Registros têm o formato `%{id: id, reading: reading}`. O ID deve ser um
  inteiro positivo ou uma string não vazia; a leitura deve ser numérica.
  """

  # Inicia um monitor. Use `name: nil` para criar uma instância sem nome.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    server_opts = if is_nil(name), do: [], else: [name: name]

    GenServer.start_link(__MODULE__, :ok, server_opts)
  end

  # Cadastra um registro de sensor.
  def register(attrs, server \\ __MODULE__) do
    with :ok <- validate_record(attrs) do
      safe_call(server, {:register, attrs})
    end
  end

  # Consulta um sensor pelo ID.
  def get(id, server \\ __MODULE__) do
    with :ok <- validate_id(id) do
      safe_call(server, {:get, id})
    end
  end

  # Atualiza a leitura de um sensor existente.
  def update(id, reading, server \\ __MODULE__) do
    with :ok <- validate_id(id),
         :ok <- validate_reading(reading) do
      safe_call(server, {:update, id, reading})
    end
  end

  # Remove um sensor existente.
  def remove(id, server \\ __MODULE__) do
    with :ok <- validate_id(id) do
      safe_call(server, {:remove, id})
    end
  end

  @impl true
  def init(:ok) do
    table = :ets.new(:sensor_monitor_records, [:set, :protected, {:read_concurrency, true}])
    {:ok, table}
  end

  @impl true
  def handle_call(request, _from, table) do
    {:reply, guarded(fn -> perform(request, table) end), table}
  end

  defp perform({:register, attrs}, table) do
    id = attrs.id

    if :ets.insert_new(table, {id, attrs}) do
      {:ok, attrs}
    else
      {:error, {:operation, :already_exists}}
    end
  end

  defp perform({:get, id}, table) do
    case :ets.lookup(table, id) do
      [{^id, record}] -> {:ok, record}
      [] -> {:error, {:operation, :not_found}}
    end
  end

  defp perform({:update, id, reading}, table) do
    case :ets.lookup(table, id) do
      [{^id, record}] ->
        updated = %{record | reading: reading}
        true = :ets.insert(table, {id, updated})
        {:ok, updated}

      [] ->
        {:error, {:operation, :not_found}}
    end
  end

  defp perform({:remove, id}, table) do
    case :ets.lookup(table, id) do
      [{^id, _record}] ->
        true = :ets.delete(table, id)
        :ok

      [] ->
        {:error, {:operation, :not_found}}
    end
  end

  defp validate_record(attrs) when is_map(attrs) do
    if map_size(attrs) == 2 and Map.has_key?(attrs, :id) and Map.has_key?(attrs, :reading) do
      with :ok <- validate_id(attrs.id),
           :ok <- validate_reading(attrs.reading) do
        :ok
      end
    else
      {:error, {:validation, :invalid_record}}
    end
  end

  defp validate_record(_), do: {:error, {:validation, :invalid_record}}

  defp validate_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_), do: {:error, {:validation, :invalid_id}}

  defp validate_reading(reading) when is_number(reading), do: :ok
  defp validate_reading(_), do: {:error, {:validation, :invalid_reading}}

  defp safe_call(server, request) do
    GenServer.call(server, request)
  catch
    :exit, reason -> {:error, {:operation, {:server_call_failed, reason}}}
  end

  defp guarded(operation) do
    operation.()
  rescue
    error ->
      {:error, {:operation, {:ets_failure, error.__struct__, Exception.message(error)}}}
  catch
    kind, reason ->
      {:error, {:operation, {:ets_failure, kind, reason}}}
  end
end
