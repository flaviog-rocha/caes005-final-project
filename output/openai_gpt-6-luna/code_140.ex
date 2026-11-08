# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryLoans do
  use GenServer

  @default_path "library_loans.db"
  @default_interval 60_000

  # API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def add_book(server \\ __MODULE__, isbn, title, copies \\ 1) do
    call(server, {:add_book, isbn, title, copies})
  end

  def borrow_book(server \\ __MODULE__, isbn, borrower, due_on) do
    call(server, {:borrow_book, isbn, borrower, due_on})
  end

  def return_book(server \\ __MODULE__, loan_id) do
    call(server, {:return_book, loan_id})
  end

  def list_books(server \\ __MODULE__), do: call(server, :list_books)
  def list_loans(server \\ __MODULE__), do: call(server, :list_loans)
  def health(server \\ __MODULE__), do: call(server, :health)

  defp call(server, request) do
    GenServer.call(server, request)
  catch
    :exit, reason -> {:error, {:operational, {:server_unavailable, reason}}}
  end

  # Inicialização e recuperação

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    interval = Keyword.get(opts, :interval, @default_interval)

    cond do
      not is_binary(path) or path == "" ->
        {:stop, {:validation, :invalid_path}}

      not is_integer(interval) or interval <= 0 ->
        {:stop, {:validation, :invalid_interval}}

      true ->
        case load_state(path) do
          {:ok, saved} ->
            state =
              Map.merge(saved, %{
                path: path,
                interval: interval,
                last_operational_error: nil
              })

            schedule_maintenance(interval)
            {:ok, state}

          {:error, reason} ->
            {:stop, {:operational, {:persistence, reason}}}
        end
    end
  end

  defp load_state(path) do
    case File.read(path) do
      {:ok, binary} ->
        decode_state(binary)

      {:error, :enoent} ->
        {:ok, %{books: %{}, loans: %{}, next_id: 1}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode_state(binary) do
    try do
      case :erlang.binary_to_term(binary, [:safe]) do
        %{version: 1, books: books, loans: loans, next_id: next_id}
        when is_map(books) and is_map(loans) and is_integer(next_id) and next_id > 0 ->
          {:ok, %{books: books, loans: loans, next_id: next_id}}

        _ ->
          {:error, :invalid_persisted_data}
      end
    rescue
      _ -> {:error, :invalid_persisted_data}
    catch
      _, _ -> {:error, :invalid_persisted_data}
    end
  end

  # Operações

  @impl true
  def handle_call({:add_book, isbn, title, copies}, _from, state) do
    with {:ok, isbn} <- validate_text(isbn, :isbn),
         {:ok, title} <- validate_text(title, :title),
         :ok <- validate_copies(copies),
         false <- Map.has_key?(state.books, isbn) do
      book = %{isbn: isbn, title: title, total: copies, available: copies}
      new_state = %{state | books: Map.put(state.books, isbn, book)}

      commit({:ok, book}, new_state, state)
    else
      true -> validation_error(:book_already_exists)
      {:error, reason} -> validation_error(reason)
    end
  end

  def handle_call({:borrow_book, isbn, borrower, due_on}, _from, state) do
    with {:ok, isbn} <- validate_text(isbn, :isbn),
         {:ok, borrower} <- validate_text(borrower, :borrower),
         :ok <- validate_due_date(due_on),
         {:ok, book} <- find_book(state.books, isbn),
         true <- book.available > 0 do
      loan = %{
        id: state.next_id,
        isbn: isbn,
        borrower: borrower,
        due_on: due_on,
        status: :active
      }

      updated_book = %{book | available: book.available - 1}

      new_state = %{
        state
        | books: Map.put(state.books, isbn, updated_book),
          loans: Map.put(state.loans, loan.id, loan),
          next_id: state.next_id + 1
      }

      commit({:ok, loan}, new_state, state)
    else
      false -> validation_error(:book_unavailable)
      {:error, reason} -> validation_error(reason)
    end
  end

  def handle_call({:return_book, loan_id}, _from, state) do
    with :ok <- validate_loan_id(loan_id),
         {:ok, loan} <- find_loan(state.loans, loan_id),
         true <- loan.status in [:active, :overdue],
         {:ok, book} <- find_book(state.books, loan.isbn) do
      returned_loan = %{loan | status: :returned}
      updated_book = %{book | available: book.available + 1}

      new_state = %{
        state
        | books: Map.put(state.books, loan.isbn, updated_book),
          loans: Map.put(state.loans, loan_id, returned_loan)
      }

      commit({:ok, returned_loan}, new_state, state)
    else
      false -> validation_error(:loan_already_returned)
      {:error, reason} -> validation_error(reason)
    end
  end

  def handle_call(:list_books, _from, state) do
    books = state.books |> Map.values() |> Enum.sort_by(& &1.isbn)
    {:reply, {:ok, books}, state}
  end

  def handle_call(:list_loans, _from, state) do
    loans = state.loans |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, loans}, state}
  end

  def handle_call(:health, _from, state) do
    {:reply,
     {:ok,
      %{
        last_operational_error: state.last_operational_error,
        books: map_size(state.books),
        loans: map_size(state.loans)
      }}, state}
  end

  # Manutenção periódica: marca como atrasados os empréstimos vencidos.
  @impl true
  def handle_info(:maintenance, state) do
    today = Date.utc_today()

    updated_loans =
      Map.new(state.loans, fn {id, loan} ->
        if loan.status == :active and Date.compare(loan.due_on, today) == :lt do
          {id, %{loan | status: :overdue}}
        else
          {id, loan}
        end
      end)

    new_state = %{state | loans: updated_loans}

    state =
      if updated_loans == state.loans do
        state
      else
        case persist(new_state) do
          :ok ->
            %{new_state | last_operational_error: nil}

          {:error, reason} ->
            %{state | last_operational_error: {:persistence, reason}}
        end
      end

    schedule_maintenance(state.interval)
    {:noreply, state}
  end

  # Persistência transacional: o estado em memória só muda após salvar.
  defp commit(reply, new_state, old_state) do
    case persist(new_state) do
      :ok ->
        {:reply, reply, %{new_state | last_operational_error: nil}}

      {:error, reason} ->
        error = {:persistence, reason}
        {:reply, {:error, {:operational, error}}, %{old_state | last_operational_error: error}}
    end
  end

  defp persist(state) do
    data =
      :erlang.term_to_binary(%{
        version: 1,
        books: state.books,
        loans: state.loans,
        next_id: state.next_id
      })

    path = state.path
    temporary_path = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(temporary_path, data, [:binary]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end

  defp schedule_maintenance(interval) do
    Process.send_after(self(), :maintenance, interval)
  end

  # Validação e busca

  defp validate_text(value, field) when is_binary(value) do
    trimmed = String.trim(value)

    if trimmed == "" do
      {:error, {:empty_field, field}}
    else
      {:ok, trimmed}
    end
  end

  defp validate_text(_value, field), do: {:error, {:invalid_field, field}}

  defp validate_copies(copies) when is_integer(copies) and copies > 0, do: :ok
  defp validate_copies(_copies), do: {:error, :invalid_copies}

  defp validate_due_date(%Date{} = date) do
    if Date.valid?(date), do: :ok, else: {:error, :invalid_due_date}
  end

  defp validate_due_date(_date), do: {:error, :invalid_due_date}

  defp validate_loan_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_loan_id(_id), do: {:error, :invalid_loan_id}

  defp find_book(books, isbn) do
    case Map.fetch(books, isbn) do
      {:ok, book} -> {:ok, book}
      :error -> {:error, :book_not_found}
    end
  end

  defp find_loan(loans, id) do
    case Map.fetch(loans, id) do
      {:ok, loan} -> {:ok, loan}
      :error -> {:error, :loan_not_found}
    end
  end

  defp validation_error(reason), do: {:reply, {:error, {:validation, reason}}, nil}
end
