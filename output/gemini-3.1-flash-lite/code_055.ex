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
  GenServer para controle de concorrência e consistência.
  """
  use GenServer

  @table_name :delivery_history
  @max_concurrent_ops 5

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc "Registra uma nova operação de entrega"
  def record_delivery(delivery_id, status) do
    GenServer.call(__MODULE__, {:record, delivery_id, status})
  end

  @doc "Recupera o histórico de uma entrega"
  def get_history(delivery_id) do
    GenServer.call(__MODULE__, {:get, delivery_id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Inicializa DETS (Disk Based Term Storage)
    {:ok, _} = :dets.open_file(@table_name, [type: :bag, file: 'delivery_ops.dets'])
    {:ok, %{active_ops: 0}}
  end

  @impl true
  def handle_call({:record, id, status}, _from, state) do
    if state.active_ops < @max_concurrent_ops do
      new_state = %{state | active_ops: state.active_ops + 1}
      
      # Simulação de escrita em banco persistente
      entry = {id, %{status: status, timestamp: System.system_time(:second)}}
      :dets.insert(@table_name, entry)
      :dets.sync(@table_name)

      # Libera slot após a operação
      {:reply, {:ok, :registered}, %{new_state | active_ops: new_state.active_ops - 1}}
    else
      {:reply, {:error, :limit_exceeded}, state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    # Busca registros no DETS
    history = :dets.lookup(@table_name, id)
    {:reply, {:ok, history}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table_name)
    :ok
  end
end

# --- Exemplo de Uso ---
# {:ok, pid} = DeliveryManager.start_link([])
# DeliveryManager.record_delivery("ORD-123", "Pendente")
# DeliveryManager.record_delivery("ORD-123", "Em trânsito")
# IO.inspect(DeliveryManager.get_history("ORD-123"))
