# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Sistema de gerenciamento de empréstimos de livros utilizando GenServer para concorrência
  e estado, e Process.send_after para tarefas periódicas.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{loans: %{}, db_sync_interval: 5000}, name: __MODULE__)
  end

  def borrow_book(user, book) do
    GenServer.call(__MODULE__, {:borrow, user, book})
  end

  def get_current_loans do
    GenServer.call(__MODULE__, :get_loans)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Inicia o ciclo de tarefas periódicas (simulando persistência no banco)
    schedule_db_sync()
    {:ok, state}
  end

  @impl true
  def handle_call({:borrow, user, book}, _from, state) do
    new_loans = Map.put(state.loans, book, %{user: user, timestamp: DateTime.utc_now()})
    {:reply, :ok, %{state | loans: new_loans}}
  end

  @impl true
  def handle_call(:get_loans, _from, state) do
    {:reply, state.loans, state}
  end

  @impl true
  def handle_info(:sync_to_db, state) do
    # Simulação de comunicação com banco de dados (ex: Ecto.Repo.insert/update)
    IO.puts("[DB Sync] Persistindo #{map_size(state.loans)} empréstimos no banco de dados...")
    
    schedule_db_sync()
    {:noreply, state}
  end

  # --- Funções Privadas ---

  defp schedule_db_sync do
    Process.send_after(self(), :sync_to_db, 5000)
  end
end

# --- Exemplo de Uso e Concorrência ---

# Para rodar, você pode iniciar o supervisor ou o processo manualmente:
{:ok, _pid} = LibraryManager.start_link([])

# Simulação de operações simultâneas usando Task
tasks = [
  Task.async(fn -> LibraryManager.borrow_book("Alice", "O Alquimista") end),
  Task.async(fn -> LibraryManager.borrow_book("Bob", "1984") end),
  Task.async(fn -> LibraryManager.borrow_book("Charlie", "Dom Casmurro") end)
]

Enum.map(tasks, &Task.await/1)

# Consulta o estado atual
IO.inspect(LibraryManager.get_current_loans(), label: "Estado Atual dos Empréstimos")
