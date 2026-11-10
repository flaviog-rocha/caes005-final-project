# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventoryManager do
  @moduledoc """
  Gerenciador de estoque que utiliza GenServer para tarefas periódicas
  e implementa estratégia de retentativa (exponential backoff).
  """
  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{inventory: %{}, retry_count: 0}, name: __MODULE__)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Agendar a primeira execução do ciclo de sincronização
    schedule_sync()
    {:ok, state}
  end

  @impl true
  def handle_info(:sync_inventory, state) do
    case sync_with_database() do
      :ok ->
        {:noreply, %{state | retry_count: 0}, 60_000}

      {:error, :transient_failure} ->
        new_state = handle_retry(state)
        {:noreply, new_state, 5_000}
    end
  end

  # --- Lógica de Domínio ---

  defp sync_with_database do
    # Simulação de IO com banco de dados
    # Retorna :ok ou erro temporário aleatoriamente
    if :rand.uniform(10) > 7 do
      IO.puts("[#{DateTime.utc_now()}] Falha temporária no DB. Tentando novamente...")
      {:error, :transient_failure}
    else
      IO.puts("[#{DateTime.utc_now()}] Sincronização de estoque concluída com sucesso.")
      :ok
    end
  end

  defp handle_retry(state) do
    # Exponential backoff simples
    backoff = :math.pow(2, state.retry_count) |> round()
    Process.send_after(self(), :sync_inventory, backoff * 1000)
    %{state | retry_count: state.retry_count + 1}
  end

  defp schedule_sync do
    Process.send_after(self(), :sync_inventory, 10_000)
  end
end

# --- Exemplo de uso ---
# Para rodar:
# {:ok, pid} = InventoryManager.start_link([])
# Process.sleep(:infinity)
