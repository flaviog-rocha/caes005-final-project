# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Library do
  use Supervisor

  @moduledoc """
  Gerenciamento concorrente de empréstimos de livros.

  Cada livro é mantido por um processo independente; operações sobre o mesmo
  livro são serializadas pelo seu GenServer.
  """

  def start_link(_opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: Library.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Library.BookSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Cadastra um livro com a quantidade inicial de exemplares."
  def add_book(id, title, copies) do
    with :ok <- validate_id(id),
         :ok <- validate_text(title),
         :ok <- validate_copies(copies) do
      args = {id, String.trim(title), copies}

      case DynamicSupervisor.start_child(Library.BookSupervisor, {Library.Book, args}) do
        {:ok, _pid} ->
          {:ok, id}

        {:error, reason} ->
          if book_registered?(id) do
            {:error, :already_exists}
          else
            {:error, reason}
          end
      end
    end
  end

  @doc "Empresta um exemplar do livro a um usuário."
  def borrow(book_id, borrower_id) do
    with :ok <- validate_id(book_id),
         :ok <- validate_id(borrower_id) do
      call_book(book_id, {:borrow, borrower_id})
    end
  end

  @doc "Registra a devolução de um exemplar."
  def return_book(book_id, borrower_id) do
    with :ok <- validate_id(book_id),
         :ok <- validate_id(borrower_id) do
      call_book(book_id, {:return, borrower_id})
    end
  end

  @doc "Retorna os dados e a disponibilidade de um livro."
  def book_info(book_id) do
    with :ok <- validate_id(book_id) do
      call_book(book_id, :info)
    end
  end

  defp call_book(book_id, message) do
    case Registry.lookup(Library.Registry, book_id) do
      [{pid, _value}] ->
        try do
          GenServer.call(pid, message)
        catch
          :exit, _reason -> {:error, :book_unavailable}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp book_registered?(id) do
    Registry.lookup(Library.Registry, id) != []
  end

  defp validate_id(value)
       when is_binary(value) and byte_size(value) > 0 do
    if String.trim(value) == value, do: :ok, else: {:error, :invalid_id}
  end

  defp validate_id(_value), do: {:error, :invalid_id}

  defp validate_text(value)
       when is_binary(value) and byte_size(value) > 0 do
    if String.trim(value) != "", do: :ok, else: {:error, :invalid_title}
  end

  defp validate_text(_value), do: {:error, :invalid_title}

  defp validate_copies(copies) when is_integer(copies) and copies > 0, do: :ok
  defp validate_copies(_copies), do: {:error, :invalid_copies}
end

defmodule Library.Book do
  use GenServer

  @moduledoc false

  def child_spec({id, _title, _copies} = args) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [args]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link({id, title, copies} = args) do
    GenServer.start_link(__MODULE__, args,
      name: {:via, Registry, {Library.Registry, id}}
    )
  end

  @impl true
  def init({id, title, copies}) do
    {:ok,
     %{
       id: id,
       title: title,
       total_copies: copies,
       available_copies: copies,
       loans: %{}
     }}
  end

  @impl true
  def handle_call({:borrow, borrower_id}, _from, state) do
    with :ok <- validate_id(borrower_id) do
      if state.available_copies > 0 do
        loans = Map.update(state.loans, borrower_id, 1, &(&1 + 1))
        updated_state = %{state | loans: loans, available_copies: state.available_copies - 1}

        {:reply,
         {:ok,
          %{
            book_id: state.id,
            borrower_id: borrower_id,
            available_copies: updated_state.available_copies
          }}, updated_state}
      else
        {:reply, {:error, :unavailable}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:return, borrower_id}, _from, state) do
    with :ok <- validate_id(borrower_id),
         {:ok, loans} <- remove_loan(state.loans, borrower_id) do
      updated_state = %{state | loans: loans, available_copies: state.available_copies + 1}

      {:reply,
       {:ok,
        %{
          book_id: state.id,
          borrower_id: borrower_id,
          available_copies: updated_state.available_copies
        }}, updated_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply,
     {:ok,
      %{
        id: state.id,
        title: state.title,
        total_copies: state.total_copies,
        available_copies: state.available_copies,
        loans: state.loans
      }}, state}
  end

  defp remove_loan(loans, borrower_id) do
    case Map.fetch(loans, borrower_id) do
      {:ok, 1} -> {:ok, Map.delete(loans, borrower_id)}
      {:ok, count} when count > 1 -> {:ok, Map.put(loans, borrower_id, count - 1)}
      :error -> {:error, :no_loan}
    end
  end

  defp validate_id(value)
       when is_binary(value) and byte_size(value) > 0 do
    if String.trim(value) == value, do: :ok, else: {:error, :invalid_id}
  end

  defp validate_id(_value), do: {:error, :invalid_id}
end
