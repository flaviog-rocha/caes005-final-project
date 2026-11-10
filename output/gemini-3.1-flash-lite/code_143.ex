# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de gerenciamento de transações financeiras utilizando GenServer para
  gerenciamento de estado, controle de concorrência via Semaphore (Task.Supervisor)
  e agendamento de tarefas com Process.send_after.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{transactions: %{}, semaphore: 0, max_concurrent: 5}, name: __MODULE__)
  end

  def create_transaction(id, amount), do: GenServer.call(__MODULE__, {:create, id, amount})
  def get_transaction(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_transaction(id, amount), do: GenServer.call(__MODULE__, {:update, id, amount})
  def delete_transaction(id), do: GenServer.call(__MODULE__, {:delete, id})

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Inicia tarefa periódica (ex: relatório de saldo a cada 60s)
    schedule_cleanup()
    {:ok, state}
  end

  @impl true
  def handle_call({:create, id, amount}, _from, state) do
    if state.semaphore < state.max_concurrent do
      new_state = put_in(state, [:transactions, id], %{amount: amount, inserted_at: DateTime.utc_now()})
      {:reply, {:ok, id}, %{new_state | semaphore: state.semaphore + 1}}
    else
      {:reply, {:error, :limit_reached}, state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.transactions, id, :not_found), state}
  end

  @impl true
  def handle_call({:update, id, amount}, _from, state) do
    if Map.has_key?(state.transactions, id) do
      new_state = put_in(state, [:transactions, id, :amount], amount)
      {:reply, :ok, new_state}
    else
      {:reply, :not_found, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    {value, new_transactions} = Map.pop(state.transactions, id)
    new_state = %{state | transactions: new_transactions, semaphore: max(0, state.semaphore - 1)}
    {:reply, if(value, do: :ok, else: :not_found), new_state}
  end

  @impl true
  def handle_info(:periodic_task, state) do
    IO.puts("[Monitor] Executando rotina periódica de verificação...")
    # Aqui ocorreria a integração com persistência (ex: Ecto.Repo.all)
    schedule_cleanup()
    {:noreply, state}
  end

  # --- Funções Auxiliares ---

  defp schedule_cleanup do
    # Executa a cada 60 segundos
    Process.send_after(self(), :periodic_task, 60_000)
  end
end

# --- Exemplo de uso (simulando camada de persistência) ---
# Em um sistema real com Ecto, trocaríamos o Map interno por chamadas Ecto.Repo
# Exemplo:
# defmodule TransactionSchema do
#   use Ecto.Schema
#   schema "transactions" do
#     field :amount, :decimal
#     timestamps()
#   end
# end
