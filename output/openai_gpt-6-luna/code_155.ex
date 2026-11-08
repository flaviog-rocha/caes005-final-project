# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryLoans do
  use GenServer

  @default_periodic_interval 60_000
  @default_loan_period_seconds 14 * 24 * 60 * 60

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  def add_book(server \\ __MODULE__, id, title, copies \\ 1) do
    GenServer.call(server, {:add_book, id, title, copies})
  end

  def borrow(server \\ __MODULE__, book_id, member_id) do
    GenServer.call(server, {:start_operation, :borrow, {book_id, member_id}})
  end

  def return_loan(server \\ __MODULE__, loan_id) do
    GenServer.call(server, {:start_operation, :return, loan_id})
  end

  def snapshot(server \\ __MODULE__) do
    GenServer.call(server, :snapshot)
  end

  def operations(server \\ __MODULE__) do
    GenServer.call(server, :operations)
  end

  @impl true
  def init(opts) do
    books = normalize_books(Keyword.get(opts, :books, []))

    {:ok, database} =
      Agent.start_link(fn ->
        %{books: books, loans: %{}}
      end)

    interval =
      Keyword.get(opts, :periodic_interval, @default_periodic_interval)

    state = %{
      database: database,
      operations: %{},
      monitors: %{},
      periodic_interval: interval,
      loan_period_seconds:
        Keyword.get(opts, :loan_period_seconds, @default_loan_period_seconds)
    }

    schedule_periodic_check(interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:add_book, id, title, copies}, _from, state)
      when is_integer(copies) and copies > 0 do
    result =
      Agent.get_and_update(state.database, fn database ->
        if Map.has_key?(database.books, id) do
          {{:error, :book_already_exists}, database}
        else
          book = %{id: id, title: title, copies: copies}
          {{:ok, book}, put_in(database.books[id], book)}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:add_book, _id, _title, _copies}, _from, state) do
    {:reply, {:error, :invalid_copies}, state}
  end

  def handle_call({:start_operation, kind, args}, _from, state) do
    id = make_ref()
    started_at = DateTime.utc_now()

    operation = %{
      id: id,
      kind: kind,
      status: :in_progress,
      started_at: started_at,
      result: nil
    }

    server = self()
    database = state.database
    loan_period = state.loan_period_seconds

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result = execute_operation(kind, args, database, loan_period)
        send(server, {:operation_result, id, result})
      end)

    _ = pid

    state = %{
      state
      | operations: Map.put(state.operations, id, operation),
        monitors: Map.put(state.monitors, monitor_ref, id)
    }

    {:reply, {:ok, id}, state}
  end

  def handle_call(:snapshot, _from, state) do
    database = Agent.get(state.database, & &1)

    {:reply,
     %{
       books: database.books,
       loans: database.loans,
       operations: current_operations(state.operations)
     }, state}
  end

  def handle_call(:operations, _from, state) do
    {:reply, current_operations(state.operations), state}
  end

  @impl true
  def handle_info(:periodic_tick, state) do
    state = start_periodic_operation(state)
    schedule_periodic_check(state.periodic_interval)
    {:noreply, state}
  end

  def handle_info({:operation_result, id, result}, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} ->
        status = if match?({:error, _}, result), do: :failed, else: :completed

        operation = %{operation | status: status, result: result}
        monitors = remove_operation_monitor(state.monitors, id)

        {:noreply,
         %{state | operations: Map.put(state.operations, id, operation), monitors: monitors}}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor_ref) do
      {nil, monitors} ->
        {:noreply, %{state | monitors: monitors}}

      {id, monitors} ->
        operations =
          case Map.fetch(state.operations, id) do
            {:ok, %{status: :in_progress} = operation} ->
              Map.put(state.operations, id, %{
                operation
                | status: :failed,
                  result: {:error, {:worker_exit, reason}}
              })

            _ ->
              state.operations
          end

        {:noreply, %{state | operations: operations, monitors: monitors}}
    end
  end

  defp start_periodic_operation(%{periodic_interval: interval} = state)
       when is_integer(interval) and interval > 0 do
    start_operation(state, :mark_overdue, nil)
  end

  defp start_periodic_operation(state), do: state

  defp start_operation(state, kind, args) do
    id = make_ref()
    started_at = DateTime.utc_now()
    operation = %{id: id, kind: kind, status: :in_progress, started_at: started_at, result: nil}
    server = self()
    database = state.database
    loan_period = state.loan_period_seconds

    {_pid, monitor_ref} =
      spawn_monitor(fn ->
        result = execute_operation(kind, args, database, loan_period)
        send(server, {:operation_result, id, result})
      end)

    %{
      state
      | operations: Map.put(state.operations, id, operation),
        monitors: Map.put(state.monitors, monitor_ref, id)
    }
  end

  defp execute_operation(:borrow, {book_id, member_id}, database, loan_period) do
    due_at = DateTime.add(DateTime.utc_now(), loan_period, :second)

    Agent.get_and_update(database, fn data ->
      case Map.fetch(data.books, book_id) do
        :error ->
          {{:error, :book_not_found}, data}

        {:ok, book} ->
          active_count =
            Enum.count(data.loans, fn {_id, loan} ->
              loan.book_id == book_id and loan.status in [:active, :overdue]
            end)

          if active_count >= book.copies do
            {{:error, :no_copies_available}, data}
          else
            loan_id = make_ref()

            loan = %{
              id: loan_id,
              book_id: book_id,
              member_id: member_id,
              borrowed_at: DateTime.utc_now(),
              due_at: due_at,
              returned_at: nil,
              status: :active
            }

            {{:ok, loan}, put_in(data.loans[loan_id], loan)}
          end
      end
    end)
  end

  defp execute_operation(:return, loan_id, database, _loan_period) do
    Agent.get_and_update(database, fn data ->
      case Map.fetch(data.loans, loan_id) do
        :error ->
          {{:error, :loan_not_found}, data}

        {:ok, %{status: status} = loan} when status in [:active, :overdue] ->
          returned = %{loan | status: :returned, returned_at: DateTime.utc_now()}
          {{:ok, returned}, put_in(data.loans[loan_id], returned)}

        {:ok, _loan} ->
          {{:error, :loan_already_returned}, data}
      end
    end)
  end

  defp execute_operation(:mark_overdue, _args, database, _loan_period) do
    now = DateTime.utc_now()

    Agent.get_and_update(database, fn data ->
      {loans, newly_overdue} =
        Enum.reduce(data.loans, {%{}, 0}, fn {id, loan}, {acc, count} ->
          if loan.status == :active and DateTime.compare(loan.due_at, now) != :gt do
            {Map.put(acc, id, %{loan | status: :overdue}), count + 1}
          else
            {Map.put(acc, id, loan), count}
          end
        end)

      {{:ok, %{newly_overdue: newly_overdue}}, %{data | loans: loans}}
    end)
  end

  defp current_operations(operations) do
    operations
    |> Enum.map(fn {_id, operation} -> operation end)
    |> Enum.filter(&(&1.status == :in_progress))
  end

  defp remove_operation_monitor(monitors, id) do
    monitors
    |> Enum.reject(fn {_ref, operation_id} -> operation_id == id end)
    |> Map.new()
  end

  defp schedule_periodic_check(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :periodic_tick, interval)
  end

  defp schedule_periodic_check(_interval), do: :ok

  defp normalize_books(books) do
    Enum.reduce(books, %{}, fn
      %{id: id, title: title} = book, acc ->
        copies = Map.get(book, :copies, 1)
        Map.put(acc, id, %{id: id, title: title, copies: copies})

      {id, title}, acc ->
        Map.put(acc, id, %{id: id, title: title, copies: 1})
    end)
  end
end
