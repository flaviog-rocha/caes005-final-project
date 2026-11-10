# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir comunicação entre diferentes processos.
# 4. Impedir que uma mesma operação seja executada mais de uma vez.
# 5. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TransactionManager do
  @moduledoc """
  Gerenciador de transações com idempotência, retentativas e estado compartilhado.
  """
  use GenServer

  # API Pública

  def start_link(_), do: GenServer.start_link(__MODULE__, %{ops: %{}, locks: MapSet.new()}, name: __MODULE__)

  def get_state, do: GenServer.call(__MODULE__, :get_state)

  def schedule_transaction(id, action) do
    GenServer.cast(__MODULE__, {:schedule, id, action})
  end

  # Callbacks do GenServer

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast({:schedule, id, action}, state) do
    if MapSet.member?(state.locks, id) do
      {:noreply, state}
    else
      new_state = %{state | locks: MapSet.put(state.locks, id), ops: Map.put(state.ops, id, :pending)}
      spawn(fn -> execute_with_retry(id, action, 3) end)
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:done, id}, state) do
    {:noreply, %{state | locks: MapSet.delete(state.locks, id), ops: Map.put(state.ops, id, :completed)}}
  end

  # Lógica de Execução com Retry

  defp execute_with_retry(id, action, retries) do
    try do
      action.()
      send(self(), {:done, id})
      GenServer.cast(__MODULE__, {:update_status, id, :success})
    rescue
      _ ->
        if retries > 0 do
          Process.sleep(1000)
          execute_with_retry(id, action, retries - 1)
        else
          GenServer.cast(__MODULE__, {:update_status, id, :failed})
        end
    end
  end

  @impl true
  def handle_cast({:update_status, id, status}, state) do
    new_ops = Map.put(state.ops, id, status)
    new_locks = if status == :failed, do: MapSet.delete(state.locks, id), else: state.locks
    {:noreply, %{state | ops: new_ops, locks: new_locks}}
  end
end

# Exemplo de uso:
# {:ok, _pid} = TransactionManager.start_link([])
# TransactionManager.schedule_transaction("tx_123", fn -> IO.puts("Executando...") end)
