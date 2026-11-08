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

defmodule BookLoanManager do
  @moduledoc """
  Gerenciador de livros e empréstimos baseado em Agent.
  """

  @default_interval_ms 60_000
  @max_loan_days 365

  def start_link(opts \\ []) do
    with :ok <- validate_options(opts) do
      interval_ms = Keyword.get(opts, :interval_ms, @default_interval_ms)

      case Agent.start_link(fn ->
             %{
               books: %{},
               loans: %{},
               next_loan_id: 1
             }
           end) do
        {:ok, agent} ->
          spawn(fn -> periodic_loop(agent, interval_ms) end)
          {:ok, agent}

        {:error, reason} ->
          operational_error(reason)
      end
    end
  end

  def add_book(agent, book_id, title) do
    with :ok <- validate_agent(agent),
         :ok <- validate_identifier(book_id, :book_id),
         :ok <- validate_text(title, :title) do
      safe_call(fn ->
        Agent.get_and_update(agent, fn state ->
          if Map.has_key?(state.books, book_id) do
            {{:error, {:operational, :book_already_exists}}, state}
          else
            book = %{id: book_id, title: title, available: true}
            {{:ok, book}, put_in(state.books[book_id], book)}
          end
        end)
      end)
    end
  end

  def borrow_book(agent, book_id, borrower, days \\ 14) do
    with :ok <- validate_agent(agent),
         :ok <- validate_identifier(book_id, :book_id),
         :ok <- validate_text(borrower, :borrower),
         :ok <- validate_loan_days(days) do
      safe_call(fn ->
        Agent.get_and_update(agent, fn state ->
          case Map.fetch(state.books, book_id) do
            :error ->
              {{:error, {:operational, :book_not_found}}, state}

            {:ok, %{available: false}} ->
              {{:error, {:operational, :book_unavailable}}, state}

            {:ok, book} ->
              loan_id = state.next_loan_id

              loan = %{
                id: loan_id,
                book_id: book_id,
                borrower: borrower,
                borrowed_on: Date.utc_today(),
                due_date: Date.add(Date.utc_today(), days),
                status: :active,
                returned_at: nil
              }

              updated_book = %{book | available: false}

              updated_state =
                state
                |> put_in([:books, book_id], updated_book)
                |> put_in([:loans, loan_id], loan)
                |> Map.put(:next_loan_id, loan_id + 1)

              {{:ok, loan}, updated_state}
          end
        end)
      end)
    end
  end

  def return_book(agent, loan_id) do
    with :ok <- validate_agent(agent),
         :ok <- validate_loan_id(loan_id) do
      safe_call(fn ->
        Agent.get_and_update(agent, fn state ->
          case Map.fetch(state.loans, loan_id) do
            :error ->
              {{:error, {:operational, :loan_not_found}}, state}

            {:ok, %{status: :returned}} ->
              {{:error, {:operational, :loan_already_returned}}, state}

            {:ok, loan} ->
              returned_loan = %{
                loan
                | status: :returned,
                  returned_at: DateTime.utc_now()
              }

              updated_state =
                state
                |> put_in([:loans, loan_id], returned_loan)
                |> update_in([:books, loan.book_id], &%{&1 | available: true})

              {{:ok, returned_loan}, updated_state}
          end
        end)
      end)
    end
  end

  def list_books(agent) do
    with :ok <- validate_agent(agent) do
      safe_call(fn ->
        Agent.get(agent, fn state ->
          state.books
          |> Map.values()
          |> Enum.sort_by(& &1.id)
        end)
      end)
    end
  end

  def list_loans(agent) do
    with :ok <- validate_agent(agent) do
      safe_call(fn ->
        Agent.get(agent, fn state ->
          state.loans
          |> Map.values()
          |> Enum.sort_by(& &1.id)
        end)
      end)
    end
  end

  def run_periodic(agent) do
    with :ok <- validate_agent(agent) do
      safe_call(fn ->
        today = Date.utc_today()

        Agent.update(agent, fn state ->
          updated_loans =
            Map.new(state.loans, fn {loan_id, loan} ->
              updated_loan =
                if loan.status == :active and Date.compare(loan.due_date, today) == :lt do
                  %{loan | status: :overdue}
                else
                  loan
                end

              {loan_id, updated_loan}
            end)

          %{state | loans: updated_loans}
        end)
      end)
    end
  end

  defp periodic_loop(agent, interval_ms) do
    Process.sleep(interval_ms)

    case run_periodic(agent) do
      {:error, {:operational, _reason}} -> :ok
      _result -> periodic_loop(agent, interval_ms)
    end
  end

  defp validate_options(opts) do
    cond do
      not is_list(opts) or not Keyword.keyword?(opts) ->
        validation_error(:options_must_be_a_keyword_list)

      Keyword.keys(opts) -- [:interval_ms] != [] ->
        validation_error(:unknown_option)

      not valid_interval?(Keyword.get(opts, :interval_ms, @default_interval_ms)) ->
        validation_error(:invalid_interval_ms)

      true ->
        :ok
    end
  end

  defp valid_interval?(interval_ms), do: is_integer(interval_ms) and interval_ms > 0

  defp validate_agent(agent) when is_pid(agent) or is_atom(agent), do: :ok
  defp validate_agent(_agent), do: validation_error(:invalid_agent)

  defp validate_identifier(value, _field) when is_binary(value) and byte_size(value) > 0,
    do: :ok

  defp validate_identifier(_value, field), do: validation_error({:invalid, field})

  defp validate_text(value, _field)
       when is_binary(value) and byte_size(value) > 0 do
    if String.trim(value) == "" do
      validation_error(:text_must_not_be_blank)
    else
      :ok
    end
  end

  defp validate_text(_value, field), do: validation_error({:invalid, field})

  defp validate_loan_days(days)
       when is_integer(days) and days >= 1 and days <= @max_loan_days,
       do: :ok

  defp validate_loan_days(_days), do: validation_error(:invalid_loan_days)

  defp validate_loan_id(loan_id) when is_integer(loan_id) and loan_id > 0, do: :ok
  defp validate_loan_id(_loan_id), do: validation_error(:invalid_loan_id)

  defp validation_error(reason), do: {:error, {:validation, reason}}
  defp operational_error(reason), do: {:error, {:operational, reason}}

  defp safe_call(fun) do
    try do
      fun.()
    rescue
      error -> operational_error({:exception, error.__struct__})
    catch
      :exit, reason -> operational_error(reason)
      kind, reason -> operational_error({kind, reason})
    end
  end
end
