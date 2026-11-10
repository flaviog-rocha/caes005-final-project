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
  Sistema de monitoramento de sensores com suporte a concorrência,
  rastreamento de histórico, tratamento de erros e expiração de dados.
  """

  use GenServer
  require Logger

  # Tipos de erro customizados
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalFailure, do: defexception [:message]

  @registry_name :sensor_registry
  @expiration_ms 60_000 # 1 minuto para expiração automática

  # API Pública

  def start_link(_) do
    Registry.start_link(keys: :duplicate, name: @registry_name)
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def record_reading(sensor_id, value) when is_number(value) do
    GenServer.call(__MODULE__, {:record, sensor_id, {:ok, value}})
  end

  def record_reading(_sensor_id, _value), do: raise(ValidationError, message: "Valor inválido")

  def record_failure(sensor_id, reason) do
    GenServer.call(__MODULE__, {:record, sensor_id, {:error, reason}})
  end

  def get_history(sensor_id) do
    Registry.lookup(@registry_name, sensor_id)
    |> Enum.map(fn {_, data} -> data end)
  end

  # Callbacks GenServer

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:record, sensor_id, result}, _from, state) do
    timestamp = System.system_time(:millisecond)
    data = %{result: result, timestamp: timestamp}

    # Registro para busca simultânea
    {:ok, _} = Registry.register(@registry_name, sensor_id, data)

    # Agenda limpeza automática
    Process.send_after(self(), {:expire, sensor_id, timestamp}, @expiration_ms)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:expire, sensor_id, timestamp}, state) do
    # Remove registros específicos baseados no timestamp para evitar concorrência desnecessária
    # em uma implementação real, usaríamos ETS com :expire_after ou similar
    Registry.select(@registry_name, [{{sensor_id, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(fn data ->
      if data.timestamp == timestamp do
        Registry.unregister_match(@registry_name, sensor_id, data)
      end
    end)

    {:noreply, state}
  end
end
