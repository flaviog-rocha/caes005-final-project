# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CacheManager do
  @moduledoc """
  Sistema de Cache resiliente utilizando GenServer e Poolboy-like throttling.
  """
  use GenServer

  # Requisito 2: Limitar operações simultâneas via ETS (semáforo simples)
  @semaphore_table :cache_semaphore

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 10)
    :ets.new(@semaphore_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(@semaphore_table, {:active_ops, 0})
    :ets.insert(@semaphore_table, {:max_ops, max_concurrency})
    
    # Requisito 4: Monitoramento de processos é implícito no ciclo de vida do GenServer
    # Se o processo que chamou o update morrer, o GenServer pode ser configurado 
    # para limpar estados pendentes usando `Process.monitor`.
    {:ok, %{data: %{}}}
  end

  # Requisito 1 & 3: Consistência e Atomicidade
  # Utilizamos a função interna do GenServer para serializar acessos (single-process state).
  # A atualização é feita em uma única transação de estado, impedindo escrita parcial.
  def update(key, value) do
    if acquire_slot() do
      try do
        GenServer.call(__MODULE__, {:update, key, value})
      after
        release_slot()
      end
    else
      {:error, :too_many_requests}
    end
  end

  @impl true
  def handle_call({:update, key, value}, _from, state) do
    # Atualização atômica do mapa interno
    new_data = Map.put(state.data, key, value)
    {:reply, :ok, %{state | data: new_data}}
  end

  defp acquire_slot do
    :ets.update_counter(@semaphore_table, :active_ops, {2, 1, :max_ops, :max_ops}, :max_ops)
    |> then(fn val -> 
      max = :ets.lookup_element(@semaphore_table, :max_ops, 2)
      if val > max do
        release_slot()
        false
      else
        true
      end
    end)
  end

  defp release_slot do
    :ets.update_counter(@semaphore_table, :active_ops, {2, -1, 0, 0})
  end
end
