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

  @registry Library.Registry
  @book_supervisor Library.BookSupervisor

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: Library.Supervisor)
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @book_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def add_book(book_id) do
    with {:ok, book_id} <- validate_id(book_id, :book_id),
         :ok <- ensure_started() do
      case lookup_book(book_id) do
        {:ok, _pid} ->
          {:error, :book_exists}

        {:error, :book_not_found} ->
          case start_book(book_id) do
            {:ok, _pid, :created} -> :ok
            {:ok, _pid, :existing} -> {:error, :book_exists}
            error -> error
          end

        error ->
          error
      end
    end
  end

  def borrow(book_id, borrower_id) do
    with {:ok, book_id} <- validate_id(book_id, :book_id),
         {:ok, borrower_id} <- validate_id(borrower_id, :borrower_id),
         :ok <- ensure_started(),
         {:ok, pid} <- lookup_book(book_id) do
      safe_call(pid, {:borrow, borrower_id})
    end
  end

  def return_book(book_id, borrower_id) do
    with {:ok, book_id} <- validate_id(book_id, :book_id),
         {:ok, borrower_id} <- validate_id(borrower_id, :borrower_id),
         :ok <- ensure_started(),
         {:ok, pid} <- lookup_book(book_id) do
      safe_call(pid, {:return, borrower_id})
    end
  end

  def status(book_id) do
    with {:ok, book_id} <- validate_id(book_id, :book_id),
         :ok <- ensure_started(),
         {:ok, pid} <- lookup_book(book_id) do
      safe_call(pid, :status)
    end
  end

  defp start_book(book_id) do
    case DynamicSupervisor.start_child(
           @book_supervisor,
           {Library.Book, book_id}
         ) do
      {:ok, pid} ->
        {:ok, pid, :created}

      {:error, reason} ->
        case lookup_book(book_id) do
          {:ok, pid} -> {:ok, pid, :existing}
          _ -> {:error, {:could_not_start_book, reason}}
        end
    end
  end

  defp lookup_book(book_id) do
    case Registry.lookup(@registry, {:book, book_id}) do
      [{pid, _value}] -> {:ok, pid}
      [] -> {:error, :book_not_found}
    end
  end

  defp safe_call(pid, message) do
    try do
      GenServer.call(pid, message)
    catch
      :exit, _reason -> {:error, :book_unavailable}
    end
  end

  defp ensure_started do
    if Process.whereis(@registry) && Process.whereis(@book_supervisor) do
      :ok
    else
      {:error, :not_started}
    end
  end

  defp validate_id(value, field) when is_binary(value) do
    if String.valid?(value) do
      value = String.trim(value)

      if value == "" do
        {:error, {:invalid, field}}
      else
        {:ok, value}
      end
    else
      {:error, {:invalid, field}}
    end
  end

  defp validate_id(_value, field), do: {:error, {:invalid, field}}
end

defmodule Library.Book do
  use GenServer

  def start_link(book_id) do
    GenServer.start_link(
      __MODULE__,
      book_id,
      name: {:via, Registry, {Library.Registry, {:book, book_id}}}
    )
  end

  def child_spec(book_id) do
    %{
      id: {__MODULE__, book_id},
      start: {__MODULE__, :start_link, [book_id]},
      restart: :temporary,
      shutdown: 5_000,
      type: :worker
    }
  end

  @impl true
  def init(book_id) do
    {:ok, %{book_id: book_id, borrower_id: nil}}
  end

  @impl true
  def handle_call({:borrow, borrower_id}, _from, state) do
    case validate_id(borrower_id) do
      :ok when state.borrower_id == nil ->
        {:reply, :ok, %{state | borrower_id: borrower_id}}

      :ok ->
        {:reply, {:error, :already_borrowed}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:return, borrower_id}, _from, state) do
    case validate_id(borrower_id) do
      :ok when state.borrower_id == nil ->
        {:reply, {:error, :not_borrowed}, state}

      :ok when state.borrower_id == borrower_id ->
        {:reply, :ok, %{state | borrower_id: nil}}

      :ok ->
        {:reply, {:error, :not_borrowed_by_you}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:status, _from, state) do
    {:reply,
     {:ok,
      %{
        book_id: state.book_id,
        available: state.borrower_id == nil,
        borrower_id: state.borrower_id
      }}, state}
  end

  defp validate_id(value) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "" do
      :ok
    else
      {:error, {:invalid, :borrower_id}}
    end
  end

  defp validate_id(_value), do: {:error, {:invalid, :borrower_id}}
end
