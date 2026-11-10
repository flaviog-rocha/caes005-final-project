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
  Gerenciador de empréstimos de livros utilizando GenServer para concorrência
  e estado, com suporte a processamento periódico.
  """
  use GenServer

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def borrow_book(book_id, user) do
    GenServer.call(__MODULE__, {:borrow, book_id, user})
  end

  def list_loans do
    GenServer.call(__MODULE__, :list)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Simula carga inicial de um "banco de dados"
    initial_state = %{
      "1" => %{title: "Elixir in Action", user: nil, status: :available},
      "2" => %{title: "Programming Phoenix", user: nil, status: :available}
    }

    # Agenda a tarefa de limpeza periódica (ex: verificar prazos a cada 60s)
    schedule_maintenance()
    {:ok, initial_state}
  end

  @impl true
  def handle_call({:borrow, book_id, user}, _from, state) do
    case Map.get(state, book_id) do
      %{status: :available} = book ->
        new_state = Map.put(state, book_id, %{book | user: user, status: :borrowed})
        {:reply, {:ok, "Livro emprestado para #{user}"}, new_state}

      nil ->
        {:reply, {:error, :not_found}, state}

      _ ->
        {:reply, {:error, :already_borrowed}, state}
    end
  end

  @impl true
  def handle_call(:list, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_info(:perform_maintenance, state) do
    # Simulação de tarefa periódica (ex: logar estado ou disparar avisos)
    IO.puts("[Maintenance] Verificando integridade dos empréstimos...")
    
    # Re-agenda a tarefa
    schedule_maintenance()
    {:noreply, state}
  end

  # --- Funções Privadas ---

  defp schedule_maintenance do
    # Executa a tarefa de manutenção a cada 60 segundos
    Process.send_after(self(), :perform_maintenance, 60_000)
  end
end

# --- Exemplo de Execução Simultânea ---

# Para testar:
# {:ok, _pid} = LibraryManager.start_link([])

# Tarefas simultâneas usando Task
# Task.async(fn -> LibraryManager.borrow_book("1", "Alice") end)
# Task.async(fn -> LibraryManager.borrow_book("2", "Bob") end)

# IO.inspect(LibraryManager.list_loans())
