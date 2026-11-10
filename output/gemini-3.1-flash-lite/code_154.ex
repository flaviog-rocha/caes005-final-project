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
  Sistema de gerenciamento de entregas usando OTP.
  Gerencia rastreamento, retentativas e expiração de registros.
  """

  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def register_delivery(id, metadata) do
    GenServer.call(__MODULE__, {:register, id, metadata})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Processo para limpar registros expirados (Requisito 3)
    :timer.send_interval(:timer.minutes(1), :cleanup)
    {:ok, %{deliveries: %{}, history: []}}
  end

  @impl true
  def handle_call({:register, id, metadata}, _from, state) do
    new_delivery = %{
      id: id,
      data: metadata,
      status: :pending,
      attempts: 0,
      created_at: System.system_time(:second)
    }

    state = put_in(state, [:deliveries, id], new_delivery)
    |> log_operation("CREATED", id)

    # Inicia processo de monitoramento para a entrega (Requisito 2 e 4)
    spawn_delivery_worker(id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    now = System.system_time(:second)
    # Expira registros com mais de 24h
    new_deliveries = Map.filter(state.deliveries, fn {_, d} -> 
      (now - d.created_at) < 86400 
    end)
    {:noreply, %{state | deliveries: new_deliveries}}
  end

  # Requisito 2: Detectar encerramentos inesperados
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.error("Processo de entrega falhou: #{inspect(reason)}")
    {:noreply, state}
  end

  # --- Funções Auxiliares ---

  defp spawn_delivery_worker(id) do
    spawn_monitor(fn ->
      process_delivery_with_retry(id, 0)
    end)
  end

  # Requisito 4: Retentativas após falhas temporárias
  defp process_delivery_with_retry(id, attempts) when attempts < 3 do
    case simulate_external_api() do
      :ok -> 
        Logger.info("Entrega #{id} realizada com sucesso.")
      :error ->
        Process.sleep(1000)
        process_delivery_with_retry(id, attempts + 1)
    end
  end
  defp process_delivery_with_retry(id, _), do: Logger.error("Entrega #{id} falhou após tentativas.")

  defp simulate_external_api do
    if :rand.uniform(10) > 7, do: :ok, else: :error
  end

  # Requisito 5: Histórico de operações
  defp log_operation(state, action, id) do
    entry = %{action: action, id: id, timestamp: System.system_time(:second)}
    update_in(state.history, &[entry | &1])
  end
end

# Exemplo de uso:
# DeliverySystem.start_link([])
# DeliverySystem.register_delivery("ENT-001", %{address: "Rua A, 123"})
