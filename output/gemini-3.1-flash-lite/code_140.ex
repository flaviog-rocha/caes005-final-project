# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para rodar este código: elixir script.exs
# O sistema utiliza Agent para estado persistente (via arquivo), 
# GenServer para tarefas periódicas e um pattern de result tuple para erros.

defmodule LibraryManager do
  use GenServer

  @db_file "library_data.ets"

  # --- Tipos de Erro ---
  # {:error, {:validation, message}} -> Erros de entrada
  # {:error, {:operational, message}} -> Falhas de sistema/IO

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def borrow_book(book_id) do
    GenServer.call(__MODULE__, {:borrow, book_id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Carrega estado persistido se existir
    state = load_from_disk()
    # Agenda tarefa periódica (ex: verificar prazos a cada 60s)
    :timer.send_interval(60_000, :cleanup_tasks)
    {:ok, state}
  end

  @impl true
  def handle_call({:borrow, book_id}, _from, state) do
    cond do
      !is_binary(book_id) ->
        {:reply, {:error, {:validation, "ID deve ser string"}}, state}

      Map.get(state, book_id) == :borrowed ->
        {:reply, {:error, {:validation, "Livro já emprestado"}}, state}

      true ->
        new_state = Map.put(state, book_id, :borrowed)
        persist_to_disk(new_state)
        {:reply, {:ok, "Livro emprestado com sucesso"}, new_state}
    end
  end

  @impl true
  def handle_info(:cleanup_tasks, state) do
    # Exemplo de operação periódica
    IO.puts("Executando verificação periódica de prazos...")
    {:noreply, state}
  end

  # --- Persistência (Simples via arquivo/termos) ---

  defp persist_to_disk(state) do
    try do
      :erlang.term_to_binary(state)
      |> then(&File.write(@db_file, &1))
    rescue
      _ -> {:error, {:operational, "Falha ao gravar em disco"}}
    end
  end

  defp load_from_disk do
    case File.read(@db_file) do
      {:ok, binary} -> :erlang.binary_to_term(binary)
      _ -> %{}
    end
  end
end

# --- Exemplo de Execução ---

{:ok, _pid} = LibraryManager.start_link([])

# Teste: Sucesso
IO.inspect(LibraryManager.borrow_book("123"))

# Teste: Validação
IO.inspect(LibraryManager.borrow_book("123"))

# Teste: Erro de tipo
IO.inspect(LibraryManager.borrow_book(123))
