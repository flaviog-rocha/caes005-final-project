# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliveryManager do
  @moduledoc """
  Gerenciador de entregas utilizando DETS para persistência local e
  GenServer para controle de concorrência e consistência de estado.
  """
  use GenServer

  @table_name :delivery_history
  @max_concurrent_ops 5

  # API Pública

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def track_delivery(id, status) do
    GenServer.call(__MODULE__, {:track, id, status})
  end

  def get_history(id) do
    GenServer.call(__MODULE__, {:history, id})
  end

  # Callbacks do GenServer

  @impl true
  def init(_) do
    # Inicializa o DETS para persistência em disco
    {:ok, _} = :dets.open_file(@table_name, [type: :bag, file: 'delivery.dets'])
    {:ok, %{active_ops: 0}}
  end

  @impl true
  def handle_call({:track, id, status}, _from, state) do
    if state.active_ops < @max_concurrent_ops do
      state = %{state | active_ops: state.active_ops + 1}
      
      # Simulação de escrita em DB/DETS
      entry = {id, %{status: status, timestamp: System.system_time(:second)}}
      :dets.insert(@table_name, entry)
      :dets.sync(@table_name)

      state = %{state | active_ops: state.active_ops - 1}
      {:reply, {:ok, :registered}, state}
    else
      {:reply, {:error, :busy}, state}
    end
  end

  @impl true
  def handle_call({:history, id}, _from, state) do
    # Consulta o DETS pelo ID
    results = :dets.lookup(@table_name, id)
    {:reply, {:ok, results}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table_name)
    :ok
  end
end

# Exemplo de uso:
# {:ok, pid} = DeliveryManager.start_link([])
# DeliveryManager.track_delivery("DEL-001", "IN_TRANSIT")
# DeliveryManager.track_delivery("DEL-001", "DELIVERED")
# {:ok, history} = DeliveryManager.get_history("DEL-001")
