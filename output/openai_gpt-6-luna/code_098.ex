# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Executar determinadas operações periodicamente.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryLoans do
  @moduledoc """
  Gerenciamento de empréstimos com persistência, notificações, operações idempotentes
  e verificação periódica de prazos.
  """

  use Supervisor

  alias LibraryLoans.{LoanManager, Notifications}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    storage_path =
      Keyword.get(opts, :storage_path, Path.join(File.cwd!(), "library_loans.term"))

    check_interval_ms = Keyword.get(opts, :check_interval_ms, 60_000)

    children = [
      {Notifications, []},
      {LoanManager, [storage_path: storage_path]},
      {LibraryLoans.Scheduler, [check_interval_ms: check_interval_ms]},
      {LibraryLoans.ProcessWatcher, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def borrow(book_id, borrower_id, due_at, operation_id) do
    LoanManager.borrow(book_id, borrower_id, due_at, operation_id)
  end

  def return_book(book_id, operation_id) do
    LoanManager.return_book(book_id, operation_id)
  end

  def loans do
    LoanManager.loans()
  end

  def subscribe(pid \\ self()) do
    Notifications.subscribe(pid)
  end

  def check_due do
    LoanManager.check_due()
  end
end

defmodule LibraryLoans.Notifications do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def subscribe(pid \\ self()) when is_pid(pid) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  def publish(notification) do
    GenServer.cast(__MODULE__, {:publish, notification})
  end

  @impl true
  def init(_opts), do: {:ok, MapSet.new()}

  @impl true
  def handle_call({:subscribe, pid}, _from, subscribers) do
    {:reply, :ok, MapSet.put(subscribers, pid)}
  end

  @impl true
  def handle_cast({:publish, notification}, subscribers) do
    Enum.each(subscribers, fn pid ->
      send(pid, {:library_notification, notification})
    end)

    {:noreply, subscribers}
  end
end

defmodule LibraryLoans.LoanManager do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def borrow(book_id, borrower_id, due_at, operation_id) do
    GenServer.call(__MODULE__, {:borrow, book_id, borrower_id, due_at, operation_id})
  end

  def return_book(book_id, operation_id) do
    GenServer.call(__MODULE__, {:return, book_id, operation_id})
  end

  def loans do
    GenServer.call(__MODULE__, :loans)
  end

  def check_due do
    GenServer.call(__MODULE__, :check_due)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :storage_path)

    state =
      case File.read(path) do
        {:ok, contents} ->
          try do
            contents
            |> :erlang.binary_to_term()
            |> normalize_state()
          rescue
            _ -> empty_state()
          end

        _ ->
          empty_state()
      end

    {:ok, Map.put(state, :storage_path, path)}
  end

  @impl true
  def handle_call({:borrow, book_id, borrower_id, due_at, operation_id}, _from, state) do
    execute(state, operation_id, fn current ->
      cond do
        not match?(%DateTime{}, due_at) ->
          {{:error, :invalid_due_at}, current}

        active_loan?(current, book_id) ->
          {{:error, :already_borrowed}, current}

        true ->
          loan = %{
            book_id: book_id,
            borrower_id: borrower_id,
            borrowed_at: DateTime.utc_now(),
            due_at: due_at,
            returned_at: nil,
            status: :active
          }

          {{:ok, loan}, put_in(current, [:loans, book_id], loan)}
      end
    end)
  end

  def handle_call({:return, book_id, operation_id}, _from, state) do
    execute(state, operation_id, fn current ->
      case Map.get(current.loans, book_id) do
        %{status: :active} = loan ->
          returned_loan =
            loan
            |> Map.put(:status, :returned)
            |> Map.put(:returned_at, DateTime.utc_now())

          {{:ok, returned_loan}, put_in(current, [:loans, book_id], returned_loan)}

        nil ->
          {{:error, :loan_not_found}, current}

        _loan ->
          {{:error, :already_returned}, current}
      end
    end)
  end

  def handle_call(:loans, _from, state) do
    {:reply, Map.values(state.loans), state}
  end

  def handle_call(:check_due, _from, state) do
    now = DateTime.utc_now()

    {updated_state, notifications} =
      Enum.reduce(state.loans, {state, []}, fn
        {_book_id, %{status: :active} = loan}, {current, emitted} ->
          collect_due_notifications(current, loan, now, emitted)

        _, accumulator ->
          accumulator
      end)

    if updated_state.alerts == state.alerts do
      {:reply, :ok, state}
    else
      case persist(updated_state) do
        :ok ->
          Enum.each(notifications, &LibraryLoans.Notifications.publish/1)
          {:reply, :ok, updated_state}

        {:error, reason} ->
          {:reply, {:error, {:persistence, reason}}, state}
      end
    end
  end

  defp execute(state, nil, _operation), do: {:reply, {:error, :operation_id_required}, state}

  defp execute(state, operation_id, operation) do
    if Map.has_key?(state.operations, operation_id) do
      {:reply, Map.fetch!(state.operations, operation_id), state}
    else
      {result, updated_state} = operation.(state)
      updated_state = put_in(updated_state, [:operations, operation_id], result)

      case persist(updated_state) do
        :ok -> {:reply, result, updated_state}
        {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
      end
    end
  end

  defp active_loan?(state, book_id) do
    match?(%{status: :active}, Map.get(state.loans, book_id))
  end

  defp collect_due_notifications(state, loan, now, emitted) do
    due_reached? = DateTime.compare(now, loan.due_at) in [:eq, :gt]
    overdue? = DateTime.compare(now, loan.due_at) == :gt

    {state, emitted} =
      if due_reached? do
        add_alert(state, loan, :due, emitted)
      else
        {state, emitted}
      end

    if overdue? do
      add_alert(state, loan, :overdue, emitted)
    else
      {state, emitted}
    end
  end

  defp add_alert(state, loan, type, emitted) do
    alert_key = {loan.book_id, loan.borrowed_at, type}

    if MapSet.member?(state.alerts, alert_key) do
      {state, emitted}
    else
      notification = %{
        type: type,
        book_id: loan.book_id,
        borrower_id: loan.borrower_id,
        due_at: loan.due_at
      }

      {Map.update!(state, :alerts, &MapSet.put(&1, alert_key)), [notification | emitted]}
    end
  end

  defp persist(state) do
    path = state.storage_path
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp"
    persisted_state = Map.drop(state, [:storage_path])

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, :erlang.term_to_binary(persisted_state)),
         :ok <- File.rename(temporary_path, path) do
      :ok
    end
  end

  defp empty_state do
    %{loans: %{}, operations: %{}, alerts: MapSet.new()}
  end

  defp normalize_state(%{loans: loans, operations: operations, alerts: alerts}) do
    %{loans: loans, operations: operations, alerts: alerts}
  end

  defp normalize_state(_), do: empty_state()
end

defmodule LibraryLoans.Scheduler do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval = max(Keyword.get(opts, :check_interval_ms, 60_000), 1)
    schedule_check(interval)
    {:ok, interval}
  end

  @impl true
  def handle_info(:check_due, interval) do
    try do
      LibraryLoans.LoanManager.check_due()
    catch
      :exit, _reason -> :ok
    end

    schedule_check(interval)
    {:noreply, interval}
  end

  defp schedule_check(interval) do
    Process.send_after(self(), :check_due, interval)
  end
end

defmodule LibraryLoans.ProcessWatcher do
  use GenServer

  @watched_processes [
    LibraryLoans.Notifications,
    LibraryLoans.LoanManager,
    LibraryLoans.Scheduler
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Process.send_after(self(), :monitor_processes, 0)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:monitor_processes, monitors) do
    updated =
      Enum.reduce(@watched_processes, monitors, fn name, current ->
        if Enum.any?(current, fn {_ref, {monitored_name, _pid}} -> monitored_name == name end) do
          current
        else
          case Process.whereis(name) do
            nil ->
              current

            pid ->
              Map.put(current, Process.monitor(pid), {name, pid})
          end
        end
      end)

    {:noreply, updated}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, monitors) do
    case Map.pop(monitors, ref) do
      {nil, remaining} ->
        {:noreply, remaining}

      {{name, _pid}, remaining} ->
        unless reason in [:normal, :shutdown] do
          LibraryLoans.Notifications.publish(%{
            type: :process_down,
            process: name,
            reason: reason
          })
        end

        Process.send_after(self(), :monitor_processes, 50)
        {:noreply, remaining}
    end
  end
end
