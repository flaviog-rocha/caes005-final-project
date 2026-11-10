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

  # Definição de tipos de erro
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: via_tuple(opts[:sensor_id]))
  end

  def process_reading(sensor_id, value) do
    GenServer.call(via_tuple(sensor_id), {:process, value})
  end

  def get_history(sensor_id) do
    GenServer.call(via_tuple(sensor_id), :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_opts) do
    {:ok, %{history: [], registry: Registry.SensorRegistry}}
  end

  @impl true
  def handle_call({:process, value}, _from, state) do
    timestamp = System.system_time(:second)
    
    try do
      validate!(value)
      perform_operation!(value)

      entry = %{value: value, status: :ok, ts: timestamp}
      new_state = update_history(state, entry)
      {:reply, {:ok, entry}, new_state}
    rescue
      e in [ValidationError, OperationalError] ->
        entry = %{value: value, status: :error, type: type_of(e), ts: timestamp}
        new_state = update_history(state, entry)
        {:reply, {:error, e.message}, new_state}
    end
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  # --- Lógica Interna ---

  defp validate!(value) when not is_number(value), 
    do: raise(ValidationError, message: "Valor inválido")
  defp validate!(_), do: :ok

  defp perform_operation!(value) when value < 0, 
    do: raise(OperationalError, message: "Falha na comunicação com hardware")
  defp perform_operation!(_), do: :ok

  defp update_history(state, entry) do
    # Expiração: mantém apenas registros dos últimos 60 segundos
    now = System.system_time(:second)
    history = [entry | state.history]
              |> Enum.filter(fn e -> (now - e.ts) < 60 end)
    
    %{state | history: history}
  end

  defp type_of(%ValidationError{}), do: :validation
  defp type_of(%OperationalError{}), do: :operational

  defp via_tuple(sensor_id) do
    {:via, Registry, {Registry.SensorRegistry, sensor_id}}
  end
end

# --- Inicialização ---

# No seu app: Registry.start_link(keys: :unique, name: Registry.SensorRegistry)
