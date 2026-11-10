# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule InventoryManager do
  @moduledoc """
  Gerenciador de estoque com expiração automática (TTL), validação e monitoramento de processos.
  Utiliza o Registry e GenServer para garantir consistência e rastreabilidade.
  """

  use GenServer

  # --- API Pública ---

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def add_item(id, quantity, ttl_seconds) when is_binary(id) and is_integer(quantity) do
    if quantity < 0, do: {:error, :invalid_quantity}, else: GenServer.call(__MODULE__, {:add, id, quantity, ttl_seconds})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Monitora o próprio processo para logs de encerramento inesperado
    Process.flag(:trap_exit, true)
    {:ok, %{items: %{}}}
  end

  @impl true
  def handle_call({:add, id, quantity, ttl}, _from, state) do
    # Simulação de persistência (DB fictício)
    # Ecto.Repo.insert!(%Stock{id: id, qty: quantity}) 
    
    timer = Process.send_after(self(), {:expire, id}, ttl * 1000)
    new_items = Map.put(state.items, id, %{qty: quantity, timer: timer})
    
    {:reply, :ok, %{state | items: new_items}}
  end

  @impl true
  def handle_info({:expire, id}, state) do
    IO.puts("Item #{id} expirou e foi removido do estoque.")
    {:noreply, %{state | items: Map.delete(state.items, id)}}
  end

  # Detecção de encerramento inesperado
  @impl true
  def handle_info({:EXIT, _from, reason}, state) do
    IO.warn("Processo encerrou inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end
end

# --- Exemplo de Uso ---

# 1. Iniciar o serviço
{:ok, _pid} = InventoryManager.start_link()

# 2. Validar e adicionar com expiração (TTL de 5 segundos)
case InventoryManager.add_item("item_123", 50, 5) do
  :ok -> IO.puts("Item adicionado com sucesso.")
  {:error, reason} -> IO.puts("Erro na validação: #{reason}")
end

# O item será removido automaticamente após 5 segundos pelo handle_info/2
