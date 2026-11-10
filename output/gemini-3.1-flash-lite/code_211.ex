# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Library.BookServer do
  @moduledoc """
  Gerencia o estado de um livro individualmente.
  Garante consistência via serialização de mensagens no GenServer.
  """
  use GenServer

  # API Pública
  def start_link(book_id) do
    GenServer.start_link(__MODULE__, %{id: book_id, status: :available}, name: via_tuple(book_id))
  end

  def borrow(book_id) do
    GenServer.call(via_tuple(book_id), :borrow)
  end

  def return(book_id) do
    GenServer.call(via_tuple(book_id), :return)
  end

  # Callbacks
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call(:borrow, _from, %{status: :available} = state) do
    {:reply, {:ok, :borrowed}, %{state | status: :borrowed}}
  end

  @impl true
  def handle_call(:borrow, _from, %{status: :borrowed} = state) do
    {:reply, {:error, :already_borrowed}, state}
  end

  @impl true
  def handle_call(:return, _from, state) do
    {:reply, :ok, %{state | status: :available}}
  end

  defp via_tuple(book_id), do: {:via, Registry, {Library.Registry, book_id}}
end

defmodule Library.Manager do
  @moduledoc """
  Orquestrador que valida dados e delega operações para processos independentes.
  """
  
  def setup do
    Registry.start_link(keys: :unique, name: Library.Registry)
    DynamicSupervisor.start_link(strategy: :one_for_one, name: Library.Supervisor)
  end

  def process_loan(book_id, action) when action in [:borrow, :return] do
    # Validação de dados (Input sanitization/validation)
    case validate_input(book_id) do
      :ok -> 
        ensure_process_exists(book_id)
        apply(Library.BookServer, action, [book_id])
      {:error, reason} -> 
        {:error, reason}
    end
  end

  defp validate_input(book_id) when is_binary(book_id) and byte_size(book_id) > 0, do: :ok
  defp validate_input(_), do: {:error, :invalid_book_id}

  defp ensure_process_exists(book_id) do
    case Registry.lookup(Library.Registry, book_id) do
      [] -> DynamicSupervisor.start_child(Library.Supervisor, {Library.BookServer, book_id})
      _ -> :ok
    end
  end
end

# Exemplo de uso:
# Library.Manager.setup()
# Library.Manager.process_loan("clean-code", :borrow)
# Library.Manager.process_loan("clean-code", :return)
