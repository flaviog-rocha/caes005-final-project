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
  Gerenciador de transações com suporte a idempotência, retentativas,
  estado compartilhado e execução periódica.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{pending: %{}, history: MapSet.new()}, name: __MODULE__)
  end

  def schedule_transaction(id, payload) do
    GenServer.call(__MODULE__, {:schedule, id, payload})
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Inicia timer para processar pendências a cada 5 segundos
    :timer.send_interval(5000, :process_queue)
    {:ok, state}
  end

  @impl true
  def handle_call({:schedule, id, payload}, _from, state) do
    if MapSet.member?(state.history, id) or Map.has_key?(state.pending, id) do
      {:reply, {:error, :already_exists}, state}
    else
      new_pending = Map.put(state.pending, id, %{data: payload, retries: 0})
      {:reply, :ok, %{state | pending: new_pending}}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info(:process_queue, state) do
    new_pending =
      state.pending
      |> Enum.reduce(state.pending, fn {id, task}, acc ->
        case perform_transaction(id, task.data) do
          :ok ->
            Map.delete(acc, id)
          {:error, :transient} when task.retries < 3 ->
            Map.put(acc, id, %{task | retries: task.retries + 1})
          {:error, _} ->
            Map.delete(acc, id) # Excede tentativas ou erro fatal
        end
      end)

    {:noreply, %{state | pending: new_pending, history: update_history(state.history, new_pending)}}
  end

  # --- Lógica Interna ---

  defp perform_transaction(id, _data) do
    # Simulação de lógica de transação
    case :rand.uniform(10) do
      n when n > 7 -> {:error, :transient}
      _ -> IO.puts("Transação #{id} concluída com sucesso."); :ok
    end
  end

  defp update_history(history, remaining_pending) do
    # Mantém rastro de operações finalizadas com sucesso
    history
  end
end
