# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Detectar encerramentos inesperados de processos.
# 3. Implementar expiração automática de registros.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem do
  @moduledoc """
  Sistema de gerenciamento de entregas usando GenServer para rastreamento,
  supervisão para resiliência e ETS para persistência efêmera com TTL.
  """

  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def create_delivery(id, metadata) do
    GenServer.call(__MODULE__, {:create, id, metadata})
  end

  def get_history(id) do
    GenServer.call(__MODULE__, {:history, id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Tabela ETS para rastreamento (id -> {status, history, expires_at})
    :ets.new(:deliveries, [:set, :named_table, :protected])
    # Timer para limpar registros expirados
    :timer.send_interval(60_000, :cleanup)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, id, metadata}, _from, state) do
    expires_at = System.system_time(:second) + 3600 # Expira em 1 hora
    entry = %{status: :created, history: ["Created at #{DateTime.utc_now()}"], expires_at: expires_at}
    :ets.insert(:deliveries, {id, entry})
    
    # Simula tentativa de envio assíncrono com retry
    Task.start(fn -> process_delivery_with_retry(id, metadata, 0) end)
    
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:history, id}, _from, state) do
    case :ets.lookup(:deliveries, id) do
      [{^id, data}] -> {:reply, {:ok, data.history}, state}
      [] -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    :ets.match_delete(:deliveries, {:_, %{expires_at: :"$1"}})
    # Lógica simplificada de limpeza baseada em timestamp
    Enum.each(:ets.tab2list(:deliveries), fn {id, data} ->
      if data.expires_at < now, do: :ets.delete(:deliveries, id)
    end)
    {:noreply, state}
  end

  # --- Lógica de Negócio e Resiliência ---

  defp process_delivery_with_retry(id, metadata, attempts) when attempts < 3 do
    try do
      # Simulação de serviço externo
      :random.uniform(10) > 8 && raise "Connection Timeout"
      update_history(id, "Delivered successfully")
    rescue
      e ->
        Logger.warning("Attempt #{attempts + 1} failed for #{id}: #{inspect(e)}")
        Process.sleep(1000 * (attempts + 1))
        process_delivery_with_retry(id, metadata, attempts + 1)
    end
  end

  defp process_delivery_with_retry(id, _, _), do: update_history(id, "Failed after max retries")

  defp update_history(id, event) do
    case :ets.lookup(:deliveries, id) do
      [{^id, data}] ->
        new_history = data.history ++ ["#{event} at #{DateTime.utc_now()}"]
        :ets.insert(:deliveries, {id, %{data | history: new_history}})
      _ -> :ok
    end
  end
end

# --- Supervisão para detectar encerramentos inesperados ---

defmodule DeliverySupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {DeliverySystem, []}
    ]
    # :one_for_one garante que se o DeliverySystem cair, ele é reiniciado
    Supervisor.init(children, strategy: :one_for_one)
  end
end
