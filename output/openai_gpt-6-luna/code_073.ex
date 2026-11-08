# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoans do
  @moduledoc """
  Gerenciamento de livros e empréstimos com validação, erros categorizados
  e atualização periódica do status de empréstimos vencidos.
  """

  @type result :: {:ok, term()} | {:error, {:validation | :operation, term()}}

  def start_link do
    Agent.start_link(fn ->
      %{books: %{}, loans: %{}, next_book_id: 1, next_loan_id: 1}
    end, name: __MODULE__)
  end

  def stop do
    safe_call(fn -> Agent.stop(__MODULE__) end)
  end

  def add_book(title, author, copies) do
    with :ok <- validate_text(title, :title),
         :ok <- validate_text(author, :author),
         :ok <- validate_positive_integer(copies, :copies) do
      safe_call(fn ->
        Agent.get_and_update(__MODULE__, fn state ->
          id = state.next_book_id

          book = %{
            id: id,
            title: String.trim(title),
            author: String.trim(author),
            total_copies: copies,
            available_copies: copies
          }

          {{:ok, book},
           %{
             state
             | books: Map.put(state.books, id, book),
               next_book_id: id + 1
           }}
        end)
      end)
    end
  end

  def borrow(book_id, borrower, loan_days \\ 14) do
    with :ok <- validate_positive_integer(book_id, :book_id),
         :ok <- validate_text(borrower, :borrower),
         :ok <- validate_positive_integer(loan_days, :loan_days) do
      borrowed_at = DateTime.utc_now()
      due_at = DateTime.add(borrowed_at, loan_days * 86_400, :second)

      safe_call(fn ->
        Agent.get_and_update(__MODULE__, fn state ->
          case Map.fetch(state.books, book_id) do
            :error ->
              {{:error, {:operation, :book_not_found}}, state}

            {:ok, %{available_copies: 0}} ->
              {{:error, {:operation, :no_copies_available}}, state}

            {:ok, book} ->
              loan_id = state.next_loan_id

              loan = %{
                id: loan_id,
                book_id: book_id,
                borrower: String.trim(borrower),
                borrowed_at: borrowed_at,
                due_at: due_at,
                status: :active
              }

              updated_book = %{book | available_copies: book.available_copies - 1}

              {{:ok, loan},
               %{
                 state
                 | books: Map.put(state.books, book_id, updated_book),
                   loans: Map.put(state.loans, loan_id, loan),
                   next_loan_id: loan_id + 1
               }}
          end
        end)
      end)
    end
  end

  def return_book(loan_id) do
    with :ok <- validate_positive_integer(loan_id, :loan_id) do
      safe_call(fn ->
        Agent.get_and_update(__MODULE__, fn state ->
          case Map.fetch(state.loans, loan_id) do
            :error ->
              {{:error, {:operation, :loan_not_found}}, state}

            {:ok, %{status: :returned}} ->
              {{:error, {:operation, :loan_already_returned}}, state}

            {:ok, loan} ->
              book = Map.fetch!(state.books, loan.book_id)
              updated_book = %{book | available_copies: book.available_copies + 1}
              updated_loan = %{loan | status: :returned, returned_at: DateTime.utc_now()}

              {{:ok, updated_loan},
               %{
                 state
                 | books: Map.put(state.books, book.id, updated_book),
                   loans: Map.put(state.loans, loan_id, updated_loan)
               }}
          end
        end)
      end)
    end
  end

  def list_books do
    safe_call(fn ->
      Agent.get(__MODULE__, fn state ->
        state.books
        |> Map.values()
        |> Enum.sort_by(& &1.id)
      end)
      |> then(&{:ok, &1})
    end)
  end

  def list_loans do
    safe_call(fn ->
      Agent.get(__MODULE__, fn state ->
        state.loans
        |> Map.values()
        |> Enum.sort_by(& &1.id)
      end)
      |> then(&{:ok, &1})
    end)
  end

  def start_periodic(interval_ms) do
    with :ok <- validate_positive_integer(interval_ms, :interval_ms) do
      pid = spawn(fn -> periodic_loop(interval_ms) end)
      {:ok, pid}
    end
  end

  def stop_periodic(pid) do
    if is_pid(pid) do
      send(pid, :stop)
      :ok
    else
      validation_error(:pid, :must_be_a_pid)
    end
  end

  def run_maintenance do
    now = DateTime.utc_now()

    safe_call(fn ->
      Agent.get_and_update(__MODULE__, fn state ->
        {loans, count} =
          Enum.reduce(state.loans, {%{}, 0}, fn {id, loan}, {acc, changed} ->
            if loan.status == :active and DateTime.compare(loan.due_at, now) != :gt do
              {Map.put(acc, id, %{loan | status: :overdue}), changed + 1}
            else
              {Map.put(acc, id, loan), changed}
            end
          end)

        {{:ok, count}, %{state | loans: loans}}
      end)
    end)
  end

  defp periodic_loop(interval_ms) do
    receive do
      :stop ->
        :ok
    after
      interval_ms ->
        run_maintenance()
        periodic_loop(interval_ms)
    end
  end

  defp validate_text(value, field) when is_binary(value) do
    if String.valid?(value) and String.trim(value) != "" do
      :ok
    else
      validation_error(field, :must_be_non_empty_text)
    end
  end

  defp validate_text(_value, field), do: validation_error(field, :must_be_text)

  defp validate_positive_integer(value, _field)
       when is_integer(value) and value > 0,
       do: :ok

  defp validate_positive_integer(_value, field),
    do: validation_error(field, :must_be_a_positive_integer)

  defp validation_error(field, reason),
    do: {:error, {:validation, {field, reason}}}

  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error ->
        {:error, {:operation, {:internal_error, Exception.message(error)}}}
    catch
      :exit, reason ->
        {:error, {:operation, {:agent_unavailable, reason}}}
    end
  end
end
