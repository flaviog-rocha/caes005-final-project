# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoans.NoopDB do
  @moduledoc false

  def persist_checkout(_loan), do: :ok
  def persist_return(_loan_id, _returned_at), do: :ok
end

defmodule BookLoans do
  use GenServer

  @moduledoc """
  Gerenciador concorrente de empréstimos. As alterações passam pelo GenServer,
  enquanto operações de persistência são executadas em tarefas separadas.
  """

  @history_limit 100

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def add_book(server \\ __MODULE__, book_id) do
    GenServer.call(server, {:add_book, book_id})
  end

  def borrow(server \\ __MODULE__, book_id, borrower, due_at \\ nil) do
    GenServer.call(server, {:borrow, book_id, borrower, due_at})
  end

  def return_loan(server \\ __MODULE__, loan_id) do
    GenServer.call(server, {:return_loan, loan_id})
  end

  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  def operation_status(server \\ __MODULE__, operation_ref) do
    GenServer.call(server, {:operation_status, operation_ref})
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :sweep_interval_ms, 60_000)

    if not (is_integer(interval) and interval > 0) do
      raise ArgumentError, "sweep_interval_ms deve ser um inteiro positivo"
    end

    timer = Process.send_after(self(), :sweep, interval)

    {:ok,
     %{
       books: MapSet.new(),
       loans: %{},
       reservations: %{},
       operations: %{},
       history: [],
       next_loan_id: 1,
       database: Keyword.get(opts, :database, BookLoans.NoopDB),
       sweep_interval_ms: interval,
       sweep_timer: timer,
       last_sweep_at: nil
     }}
  end

  @impl true
  def handle_call({:add_book, book_id}, _from, state) do
    if MapSet.member?(state.books, book_id) do
      {:reply, {:error, :book_already_exists}, state}
    else
      {:reply, :ok, %{state | books: MapSet.put(state.books, book_id)}}
    end
  end

  def handle_call({:borrow, book_id, borrower, due_at}, _from, state) do
    cond do
      not MapSet.member?(state.books, book_id) ->
        {:reply, {:error, :book_not_found}, state}

      Map.has_key?(state.reservations, book_id) ->
        {:reply, {:error, :book_operation_in_progress}, state}

      book_on_loan?(state, book_id) ->
        {:reply, {:error, :book_unavailable}, state}

      true ->
        due_at = due_at || DateTime.add(DateTime.utc_now(), 14 * 24 * 60 * 60, :second)
        loan_id = state.next_loan_id

        loan = %{
          id: loan_id,
          book_id: book_id,
          borrower: borrower,
          due_at: due_at,
          status: :active,
          returned_at: nil
        }

        operation = %{
          ref: make_ref(),
          type: :checkout,
          book_id: book_id,
          loan: loan,
          started_at: DateTime.utc_now()
        }

        start_operation(state, operation)
    end
  end

  def handle_call({:return_loan, loan_id}, _from, state) do
    case Map.get(state.loans, loan_id) do
      nil ->
        {:reply, {:error, :loan_not_found}, state}

      %{status: :returned} ->
        {:reply, {:error, :loan_already_returned}, state}

      loan ->
        if Map.has_key?(state.reservations, loan.book_id) do
          {:reply, {:error, :book_operation_in_progress}, state}
        else
          operation = %{
            ref: make_ref(),
            type: :return,
            loan_id: loan_id,
            book_id: loan.book_id,
            started_at: DateTime.utc_now()
          }

          start_operation(state, operation)
        end
    end
  end

  def handle_call(:snapshot, _from, state) do
    {:reply,
     %{
       books: MapSet.to_list(state.books),
       loans: state.loans,
       operations: state.operations,
       last_sweep_at: state.last_sweep_at
     }, state}
  end

  def handle_call({:operation_status, ref}, _from, state) do
    result =
      case Map.fetch(state.operations, ref) do
        {:ok, operation} -> {:in_progress, operation}
        :error -> Enum.find(state.history, fn entry -> entry.ref == ref end)
      end

    {:reply, result || {:error, :operation_not_found}, state}
  end

  @impl true
  def handle_cast({:operation_finished, ref, result}, state) do
    case Map.pop(state.operations, ref) do
      {nil, _operations} ->
        {:noreply, state}

      {operation, operations} ->
        state = %{state | operations: operations}
        state = release_reservation(state, operation)

        {state, outcome} =
          case result do
            :ok -> {apply_operation(state, operation), :ok}
            {:error, reason} -> {state, {:error, reason}}
          end

        history_entry = %{
          ref: ref,
          type: operation.type,
          status: outcome,
          finished_at: DateTime.utc_now()
        }

        history = Enum.take([history_entry | state.history], @history_limit)
        {:noreply, %{state | history: history}}
    end
  end

  @impl true
  def handle_info(:sweep, state) do
    now = DateTime.utc_now()

    loans =
      Map.new(state.loans, fn {id, loan} ->
        updated =
          if loan.status == :active and DateTime.compare(loan.due_at, now) != :gt do
            %{loan | status: :overdue}
          else
            loan
          end

        {id, updated}
      end)

    timer = Process.send_after(self(), :sweep, state.sweep_interval_ms)

    {:noreply, %{state | loans: loans, last_sweep_at: now, sweep_timer: timer}}
  end

  defp start_operation(state, operation) do
    ref = operation.ref
    server = self()
    database = state.database

    state = %{
      state
      | operations: Map.put(state.operations, ref, operation),
        reservations: Map.put(state.reservations, operation.book_id, ref)
    }

    task =
      Task.start(fn ->
        result = persist(database, operation)
        GenServer.cast(server, {:operation_finished, ref, result})
      end)

    case task do
      {:ok, _pid} ->
        {:reply, {:ok, ref}, increment_loan_id(state, operation)}

      {:error, reason} ->
        state = release_reservation(state, operation)
        state = %{state | operations: Map.delete(state.operations, ref)}
        {:reply, {:error, reason}, state}
    end
  end

  defp persist(database, %{type: :checkout, loan: loan}) do
    safe_apply(database, :persist_checkout, [loan])
  end

  defp persist(database, %{type: :return, loan_id: loan_id}) do
    safe_apply(database, :persist_return, [loan_id, DateTime.utc_now()])
  end

  defp safe_apply(database, function, args) do
    case apply(database, function, args) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_database_response, other}}
    end
  rescue
    error -> {:error, {:database_exception, error}}
  catch
    kind, reason -> {:error, {:database_failure, kind, reason}}
  end

  defp apply_operation(state, %{type: :checkout, loan: loan}) do
    %{state | loans: Map.put(state.loans, loan.id, loan)}
  end

  defp apply_operation(state, %{type: :return, loan_id: loan_id}) do
    returned_at = DateTime.utc_now()
    loan = Map.fetch!(state.loans, loan_id)
    updated_loan = %{loan | status: :returned, returned_at: returned_at}
    %{state | loans: Map.put(state.loans, loan_id, updated_loan)}
  end

  defp release_reservation(state, operation) do
    %{state | reservations: Map.delete(state.reservations, operation.book_id)}
  end

  defp increment_loan_id(state, %{type: :checkout}) do
    %{state | next_loan_id: state.next_loan_id + 1}
  end

  defp increment_loan_id(state, _operation), do: state

  defp book_on_loan?(state, book_id) do
    Enum.any?(state.loans, fn {_id, loan} ->
      loan.book_id == book_id and loan.status in [:active, :overdue]
    end)
  end
end
