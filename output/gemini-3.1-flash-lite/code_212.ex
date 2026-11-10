# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Sistema de gerenciamento de empréstimos de livros utilizando GenServer para
  isolamento de estado, concorrência e validação.
  """

  use GenServer

  # API Pública

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc "Registra um empréstimo. Valida se o livro está disponível."
  def borrow_book(book_id, user_id) do
    GenServer.call(__MODULE__, {:borrow, book_id, user_id})
  end

  @doc "Retorna um livro. Valida se o livro foi emprestado anteriormente."
  def return_book(book_id) do
    GenServer.call(__MODULE__, {:return, book_id})
  end

  # Callbacks do GenServer

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:borrow, book_id, user_id}, _from, state) do
    cond do
      !is_binary(book_id) or !is_binary(user_id) ->
        {:reply, {:error, :invalid_input}, state}

      Map.has_key?(state, book_id) ->
        {:reply, {:error, :book_already_borrowed}, state}

      true ->
        new_state = Map.put(state, book_id, user_id)
        {:reply, {:ok, :borrowed}, new_state}
    end
  end

  @impl true
  def handle_call({:return, book_id}, _from, state) do
    cond do
      !is_binary(book_id) ->
        {:reply, {:error, :invalid_input}, state}

      !Map.has_key?(state, book_id) ->
        {:reply, {:error, :book_not_borrowed}, state}

      true ->
        new_state = Map.delete(state, book_id)
        {:reply, {:ok, :returned}, new_state}
    end
  end

  # Exemplo de uso concorrente (Processamento independente)
  def simulate_concurrency do
    {:ok, _} = start_link(nil)

    tasks =
      for i <- 1..5 do
        Task.async(fn ->
          borrow_book("book_#{i}", "user_#{i}")
        end)
      end

    results = Enum.map(tasks, &Task.await/1)
    IO.inspect(results, label: "Resultados da concorrência")
  end
end
