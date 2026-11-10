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
  Sistema de monitoramento de sensores utilizando ETS.
  """

  @table :sensor_data

  defstruct [:id, :type, :value, :status]

  @type sensor :: %__MODULE__{id: any, type: String.t(), value: float, status: atom}
  @type error_type :: :validation_error | :operational_failure

  def init do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    :ok
  end

  # --- Validação ---

  defp validate(%__MODULE__{id: nil}), do: {:error, :validation_error, "ID é obrigatório"}
  defp validate(%__MODULE__{type: nil}), do: {:error, :validation_error, "Tipo é obrigatório"}
  defp validate(%__MODULE__{value: v}) when not is_number(v), do: {:error, :validation_error, "Valor deve ser numérico"}
  defp validate(sensor), do: {:ok, sensor}

  # --- Operações CRUD ---

  def create(sensor) when is_struct(sensor) do
    with {:ok, valid_sensor} <- validate(sensor) do
      case :ets.insert_new(@table, {valid_sensor.id, valid_sensor}) do
        true -> {:ok, valid_sensor}
        false -> {:error, :operational_failure, "Sensor já existe"}
      end
    end
  end

  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, sensor}] -> {:ok, sensor}
      [] -> {:error, :operational_failure, "Sensor não encontrado"}
    end
  end

  def update(id, updates) when is_map(updates) do
    case get(id) do
      {:ok, sensor} ->
        updated_sensor = struct(sensor, updates)
        with {:ok, valid_sensor} <- validate(updated_sensor) do
          :ets.insert(@table, {id, valid_sensor})
          {:ok, valid_sensor}
        end
      error -> error
    end
  end

  def delete(id) do
    case :ets.delete(@table, id) do
      true -> :ok
      false -> {:error, :operational_failure, "Falha ao remover sensor"}
    end
  end

  # --- Exemplo de uso isolado ---
  # SensorMonitor.init()
  # s = %SensorMonitor{id: 1, type: "temperatura", value: 25.5, status: :active}
  # SensorMonitor.create(s)
end
