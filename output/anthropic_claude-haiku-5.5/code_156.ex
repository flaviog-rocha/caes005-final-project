# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Executar determinadas operações periodicamente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de empréstimos de livros em Elixir (somente biblioteca padrão).
#
# Uso (ex.: `elixir emprestimos.exs` seguido de iex, ou carregado em iex -S mix):
#   {:ok, _sup} = LoanSystem.start_link(tick_ms: 2_000)
#   {:ok, op1} = LoanSystem.Manager.checkout(1, "ana")
#   {:ok, op2} = LoanSystem.Manager.checkout(2, "bruno")   # execução concorrente
#   LoanSystem.Manager.operations()                        # estado das operações
#   LoanSystem.Manager.operation(op1)                      # consulta uma operação
#   LoanSystem.Manager.loans()                             # estado dos empréstimos

defmodule LoanSystem do
  @moduledoc """
  Supervisor raiz do sistema de empréstimos.

  Componentes:
    * `LoanSystem.FakeDB`      - banco de dados fictício em memória (Agent).
    * `LoanSystem.TaskSupervisor` - supervisiona tarefas concorrentes.
    * `LoanSystem.Manager`     - GenServer que orquestra operações, expõe seu
                                 estado e executa verificações periódicas.
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      LoanSystem.FakeDB,
      {Task.Supervisor, name: LoanSystem.TaskSupervisor},
      {LoanSystem.Manager, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule LoanSystem.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória, implementado com `Agent`.

  Cada operação de escrita usa `Agent.get_and_update/2`, o que torna a
  verificação de disponibilidade e a escrita atômicas: dois empréstimos
  concorrentes do mesmo livro nunca são aceitos ao mesmo tempo.
  Não depende de Ecto; o modelo é apenas ilustrativo.
  """
  use Agent

  @loan_period_days 14

  def start_link(_opts \\ []) do
    Agent.start_link(&initial_state/0, name: __MODULE__)
  end

  @doc "Registra um empréstimo de forma atômica, se o livro estiver disponível."
  def checkout(book_id, member_id, now) do
    Agent.get_and_update(__MODULE__, fn db ->
      case db.books do
        %{^book_id => %{available: false}} ->
          {{:error, :book_unavailable}, db}

        %{^book_id => book} ->
          id = db.next_loan_id

          loan = %{
            id: id,
            book_id: book_id,
            member_id: member_id,
            status: :active,
            borrowed_at: now,
            due_at: DateTime.add(now, @loan_period_days * 86_400, :second),
            returned_at: nil
          }

          new_db = %{
            db
            | next_loan_id: id + 1,
              loans: Map.put(db.loans, id, loan),
              books: Map.put(db.books, book_id, %{book | available: false})
          }

          {{:ok, loan}, new_db}

        _ ->
          {{:error, :book_not_found}, db}
      end
    end)
  end

  @doc "Encerra um empréstimo e libera o livro, de forma atômica."
  def close_loan(loan_id, now) do
    Agent.get_and_update(__MODULE__, fn db ->
      case db.loans do
        %{^loan_id => %{status: :returned}} ->
          {{:error, :already_returned}, db}

        %{^loan_id => loan} ->
          returned = %{loan | status: :returned, returned_at: now}
          book = db.books[loan.book_id]

          new_db = %{
            db
            | loans: Map.put(db.loans, loan_id, returned),
              books: Map.put(db.books, loan.book_id, %{book | available: true})
          }

          {{:ok, returned}, new_db}

        _ ->
          {{:error, :loan_not_found}, db}
      end
    end)
  end

  @doc "Marca como vencidos os empréstimos ativos cujo prazo já passou. Retorna os ids afetados."
  def mark_overdue(now) do
    Agent.get_and_update(__MODULE__, fn db ->
      expired =
        for {id, %{status: :active, due_at: due}} <- db.loans,
            DateTime.compare(due, now) == :lt,
            do: id

      loans =
        Enum.reduce(expired, db.loans, fn id, acc ->
          Map.update!(acc, id, &%{&1 | status: :overdue})
        end)

      {expired, %{db | loans: loans}}
    end)
  end

  def list_loans do
    Agent.get(__MODULE__, fn db -> db.loans |> Map.values() |> Enum.sort_by(& &1.id) end)
  end

  def list_books do
    Agent.get(__MODULE__, fn db -> db.books |> Map.values() |> Enum.sort_by(& &1.id) end)
  end

  defp initial_state do
    %{
      books: %{
        1 => %{id: 1, title: "Programming Elixir", available: true},
        2 => %{id: 2, title: "Designing Data-Intensive Applications", available: true},
        3 => %{id: 3, title: "Structure and Interpretation of Computer Programs", available: true}
      },
      loans: %{},
      next_loan_id: 1
    }
  end
end

defmodule LoanSystem.Manager do
  @moduledoc """
  Orquestrador das operações de empréstimo.

    * Cada operação roda em sua própria tarefa (concorrência real).
    * O estado das operações em andamento e concluídas pode ser consultado.
    * A cada `tick_ms` milissegundos é disparada uma verificação periódica
      de empréstimos vencidos, sem sobrepor execuções dessa mesma verificação.
  """
  use GenServer

  @default_tick_ms 5_000
  @history_size 50
  @simulated_latency_ms 200

  # API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Inicia assincronamente um empréstimo. Retorna `{:ok, op_id}`."
  def checkout(book_id, member_id) do
    GenServer.call(__MODULE__, {:start, :checkout, [book_id, member_id]})
  end

  @doc "Inicia assincronamente uma devolução. Retorna `{:ok, op_id}`."
  def return_loan(loan_id) do
    GenServer.call(__MODULE__, {:start, :return, [loan_id]})
  end

  @doc "Retorna as operações em andamento e o histórico de concluídas."
  def operations do
    GenServer.call(__MODULE__, :operations)
  end

  @doc "Consulta uma operação pelo id."
  def operation(op_id) do
    GenServer.call(__MODULE__, {:operation, op_id})
  end

  def loans, do: LoanSystem.FakeDB.list_loans()
  def books, do: LoanSystem.FakeDB.list_books()

  # Callbacks do GenServer

  @impl true
  def init(opts) do
    tick_ms = Keyword.get(opts, :tick_ms, @default_tick_ms)
    schedule_tick(tick_ms)
    {:ok, %{running: %{}, finished: [], next_op_id: 1, tick_ms: tick_ms}}
  end

  @impl true
  def handle_call({:start, type, args}, _from, state) do
    {op_id, state} = start_operation(state, type, args)
    {:reply, {:ok, op_id}, state}
  end

  def handle_call(:operations, _from, state) do
    running = state.running |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, %{running: running, finished: state.finished}, state}
  end

  def handle_call({:operation, op_id}, _from, state) do
    all = Map.values(state.running) ++ state.finished
    {:reply, Enum.find(all, &(&1.id == op_id)) || {:error, :not_found}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    schedule_tick(state.tick_ms)

    # Evita sobreposição: só dispara nova verificação se não houver uma em execução.
    overdue_running? = Enum.any?(state.running, fn {_ref, op} -> op.type == :overdue end)

    if overdue_running? do
      {:noreply, state}
    else
      {_op_id, state} = start_operation(state, :overdue, [])
      {:noreply, state}
    end
  end

  # Resultado de uma tarefa concluída com sucesso
  def handle_info({ref, result}, %{running: running} = state) when is_map_key(running, ref) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish_operation(state, ref, status_of(result), result)}
  end

  # Tarefa que terminou abruptamente
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{running: running} = state)
      when is_map_key(running, ref) do
    {:noreply, finish_operation(state, ref, :crashed, {:exit, reason})}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Funções privadas

  defp start_operation(state, type, args) do
    task =
      Task.Supervisor.async_nolink(LoanSystem.TaskSupervisor, fn -> run(type, args) end)

    op = %{
      id: state.next_op_id,
      type: type,
      args: args,
      status: :running,
      result: nil,
      started_at: DateTime.utc_now(),
      finished_at: nil
    }

    state = %{
      state
      | next_op_id: op.id + 1,
        running: Map.put(state.running, task.ref, op)
    }

    {op.id, state}
  end

  defp finish_operation(state, ref, status, result) do
    {op, running} = Map.pop(state.running, ref)
    op = %{op | status: status, result: result, finished_at: DateTime.utc_now()}

    %{
      state
      | running: running,
        finished: Enum.take([op | state.finished], @history_size)
    }
  end

  defp status_of({:ok, _}), do: :done
  defp status_of(_), do: :failed

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)

  # Execução das operações (rodam dentro de tarefas separadas)

  defp run(:checkout, [book_id, member_id]) do
    Process.sleep(@simulated_latency_ms)
    LoanSystem.FakeDB.checkout(book_id, member_id, DateTime.utc_now())
  end

  defp run(:return, [loan_id]) do
    Process.sleep(@simulated_latency_ms)
    LoanSystem.FakeDB.close_loan(loan_id, DateTime.utc_now())
  end

  defp run(:overdue, []) do
    Process.sleep(@simulated_latency_ms)
    {:ok, LoanSystem.FakeDB.mark_overdue(DateTime.utc_now())}
  end
end
