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
  e um mecanismo de retentativa para operações falhas.
  """
  use GenServer

  @interval 5_000 # 5 segundos
  @max_retries 3

  # API Pública
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{inventory: %{}}, name: __MODULE__)
  end

  # Callbacks do GenServer
  @impl true
  def init(state) do
    # Agenda a primeira execução
    schedule_work()
    {:ok, state}
  end

  @impl true
  def handle_info(:sync_inventory, state) do
    new_state = perform_sync_with_retry(state)
    schedule_work()
    {:noreply, new_state}
  end

  # Lógica de negócio
  defp perform_sync_with_retry(state, attempt \\ 0) do
    try do
      # Simulação de interação com DB (Ex: Ecto.Repo.all(Product))
      IO.puts("Sincronizando estoque com banco de dados... (Tentativa #{attempt + 1})")
      
      # Simula falha aleatória para testar retry
      if :rand.uniform(10) > 7, do: raise "Erro de conexão temporário"
      
      %{state | inventory: %{"item_a" => 100, "item_b" => 50}}
    rescue
      e ->
        if attempt < @max_retries - 1 do
          Process.sleep(1_000)
          perform_sync_with_retry(state, attempt + 1)
        else
          IO.puts("Falha crítica ao sincronizar estoque: #{inspect(e)}")
          state
        end
    end
  end

  defp schedule_work do
    Process.send_after(self(), :sync_inventory, @interval)
  end
end

# Para rodar este exemplo em um ambiente Elixir:
# {:ok, pid} = InventoryManager.start_link([])
# Process.sleep(20_000)
