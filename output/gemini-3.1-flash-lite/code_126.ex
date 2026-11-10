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
  Gerenciador de estoque com expiração automática (TTL), validação e monitoramento.
  Utiliza GenServer para isolamento de estado e monitoramento de processos.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def add_item(id, quantity, ttl_ms) when is_integer(quantity) and quantity >= 0 do
    GenServer.call(__MODULE__, {:add, id, quantity, ttl_ms})
  end

  def get_item(id), do: GenServer.call(__MODULE__, {:get, id})

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Monitora processos que interagem com o estoque, se necessário
    Process.flag(:trap_exit, true)
    {:ok, %{items: %{}, timers: %{}}}
  end

  @impl true
  def handle_call({:add, id, quantity, ttl}, _from, state) do
    # Cancelar timer anterior se existir para atualizar o TTL
    if Map.has_key?(state.timers, id), do: Process.cancel_timer(state.timers[id])

    # Definir novo timer de expiração
    timer_ref = Process.send_after(self(), {:expire, id}, ttl)

    new_state = %{
      state |
      items: Map.put(state.items, id, quantity),
      timers: Map.put(state.timers, id, timer_ref)
    }

    {:reply, :ok, new_state}
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.items, id, :not_found), state}
  end

  # --- Lógica de Expiração ---

  @impl true
  def handle_info({:expire, id}, state) do
    new_items = Map.delete(state.items, id)
    new_timers = Map.delete(state.timers, id)
    {:noreply, %{state | items: new_items, timers: new_timers}}
  end

  # Detecção de encerramentos inesperados
  @impl true
  def handle_info({:EXIT, from_pid, reason}, state) do
    IO.puts("Processo #{inspect(from_pid)} encerrado inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end

  # --- Exemplo de Integração (Conceitual) ---
  # Em um cenário real com Ecto, a função abaixo salvaria no banco de dados.
  # defp persist_to_db(id, quantity) do
  #   # Repo.insert!(%Inventory{id: id, quantity: quantity})
  # end
end

# Exemplo de uso:
# {:ok, _} = InventoryManager.start_link([])
# InventoryManager.add_item("notebook", 10, 5000) # Expira em 5 segundos
# IO.inspect InventoryManager.get_item("notebook")
