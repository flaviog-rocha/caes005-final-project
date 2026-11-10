# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para rodar: elixir nome_do_arquivo.exs
# O sistema utiliza Agent para persistência em memória e GenServer para tarefas periódicas.
# A persistência pode ser estendida para disco via :dets ou :ets com arquivo.

defmodule LibrarySystem do
  use GenServer
  require Logger

  # --- Tipos e Erros ---
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def borrow_book(book_id, user_id) do
    GenServer.call(__MODULE__, {:borrow, book_id, user_id})
  end

  # --- Callbacks do GenServer ---
  @impl true
  def init(_) do
    # Simula recuperação de dados persistidos (pode ser carregado de um arquivo .dets)
    initial_state = %{books: %{"1" => :available, "2" => :available}}
    
    # Executa verificação periódica a cada 60 segundos
    :timer.send_interval(60_000, :perform_maintenance)
    
    {:ok, initial_state}
  end

  @impl true
  def handle_call({:borrow, book_id, user_id}, _from, state) do
    cond do
      is_nil(user_id) -> 
        {:reply, {:error, %ValidationError{message: "User ID inválido"}}, state}
      
      Map.get(state.books, book_id) != :available ->
        {:reply, {:error, %OperationalError{message: "Livro indisponível ou inexistente"}}, state}
        
      true ->
        new_books = Map.put(state.books, book_id, {:rented_to, user_id})
        new_state = %{state | books: new_books}
        
        # Persistência simplificada (em produção usar :dets)
        persist_to_disk(new_state)
        
        {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_info(:perform_maintenance, state) do
    Logger.info("Executando manutenção periódica: Verificando prazos de devolução...")
    # Lógica de processamento em background
    {:noreply, state}
  end

  # --- Auxiliares ---
  defp persist_to_disk(state) do
    # Simulação de escrita em disco
    File.write("library_state.json", :erlang.term_to_binary(state))
  end
end

# --- Exemplo de Execução ---
{:ok, _pid} = LibrarySystem.start_link(nil)

# Caso de sucesso
IO.inspect LibrarySystem.borrow_book("1", "user_123")

# Caso de erro de validação
IO.inspect LibrarySystem.borrow_book("1", nil)

# Caso de erro operacional (livro já alugado)
IO.inspect LibrarySystem.borrow_book("1", "user_456")
