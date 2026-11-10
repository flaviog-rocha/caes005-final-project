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
  @moduledoc """
  Sistema de monitoramento de sensores utilizando ETS para armazenamento em memória.
  """

  @table :sensors_table

  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  def init do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    :ok
  end

  # 1. Validação de Dados
  defp validate_sensor!(%{id: id, type: type, value: value}) when is_binary(id) and is_number(value) do
    if String.trim(type) == "", do: raise(ValidationError, message: "Tipo inválido")
    %{id: id, type: type, value: value, updated_at: System.system_time(:second)}
  end
  defp validate_sensor!(_), do: raise(ValidationError, message: "Dados malformados")

  # 4. CRUD
  def register(sensor) do
    try do
      data = validate_sensor!(sensor)
      case :ets.insert_new(@table, {data.id, data}) do
        true -> {:ok, data.id}
        false -> raise OperationalError, message: "Sensor já cadastrado"
      end
    rescue
      e in [ValidationError, OperationalError] -> {:error, e}
    end
  end

  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  def update(id, new_data) do
    try do
      data = validate_sensor!(Map.put(new_data, :id, id))
      case :ets.update_element(@table, id, {2, data}) do
        true -> {:ok, id}
        false -> raise OperationalError, message: "Sensor não encontrado"
      end
    rescue
      e in [ValidationError, OperationalError] -> {:error, e}
    end
  end

  def delete(id) do
    case :ets.delete(@table, id) do
      true -> :ok
      _ -> {:error, :not_found}
    end
  end

  # 2. Processamento independente (via execução assíncrona/concorrente)
  def process_sensor_async(id) do
    Task.start(fn ->
      case get(id) do
        {:ok, data} -> IO.puts("Processando sensor #{id}: #{inspect(data)}")
        _ -> IO.puts("Falha ao processar: sensor #{id} inexistente")
      end
    end)
  end
end
