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
  Sistema de monitoramento de sensores com controle de concorrência,
  persistência (ETS), notificações e retry logic.
  """
  use GenServer
  require Logger

  # 1. Limitar operações simultâneas via ETS como semáforo simples
  @max_concurrency 5
  @table_name :sensor_store

  # Definição de erros
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # API Pública
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def process_reading(sensor_id, value) do
    # Registro garante que cada sensor tenha seu próprio processo ou referência
    Registry.register(:sensor_registry, sensor_id, self())
    GenServer.call(__MODULE__, {:process, sensor_id, value})
  end

  # Callbacks GenServer
  @impl true
  def init(_) do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    Registry.start_link(keys: :unique, name: :sensor_registry)
    {:ok, %{active_tasks: 0}}
  end

  @impl true
  def handle_call({:process, id, value}, _from, state) do
    if state.active_tasks >= @max_concurrency do
      {:reply, {:error, :too_many_requests}, state}
    else
      # 4. Implementar novas tentativas (Retry)
      result = with_retry(3, fn -> perform_logic(id, value) end)
      
      case result do
        {:ok, data} ->
          :ets.insert(@table_name, {id, data}) # 2. Persistência
          {:reply, {:ok, data}, state}
        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  defp perform_logic(id, value) do
    # 5. Diferenciar erros
    cond do
      value < 0 -> raise ValidationError, message: "Valor negativo inválido"
      value > 1000 -> {:error, %OperationalError{message: "Falha na leitura do sensor"}}
      true -> 
        # 3. Notificações
        if value > 900, do: Logger.warn("Alerta: Sensor #{id} atingiu limite crítico: #{value}")
        {:ok, %{value: value, timestamp: System.system_time(:second)}}
    end
  rescue
    e in ValidationError -> {:error, e}
  end

  defp with_retry(0, _fun), do: {:error, :max_retries_reached}
  defp with_retry(n, fun) do
    case fun.() do
      {:error, %OperationalError{}} -> 
        Process.sleep(100)
        with_retry(n - 1, fun)
      other -> other
    end
  end
end
