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
  use Supervisor

  @moduledoc """
  Gerenciamento de empréstimos com persistência, notificações, operações
  idempotentes, verificações periódicas e monitoramento de falhas.
  """

  @default_sweep_interval 60_000
  @default_notify_before 86_400

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    children = [
      {LibraryLoans.Notifications, []},
      {LibraryLoans.Server, opts},
      {LibraryLoans.Watchdog, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def subscribe(pid \\ self()), do: LibraryLoans.Notifications.subscribe(pid)
  def unsubscribe(pid \\ self()), do: LibraryLoans.Notifications.unsubscribe(pid)

  def checkout(operation_id, loan_id, borrower, book, due_at) do
    LibraryLoans.Server.checkout(operation_id, loan_id, borrower, book, due_at)
  end

  def return_book(operation_id, loan_id) do
    LibraryLoans.Server.return_book(operation_id, loan_id)
  end

  def list_loans do
    LibraryLoans.Server.list_loans()
  end
end

defmodule LibraryLoans.Notifications do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def subscribe(pid \\ self()) do
    GenServer.call(__MODULE__, {:subscribe, pid})
  end

  def unsubscribe(pid \\ self()) do
    GenServer.call(__MODULE__, {:unsubscribe, pid})
  end

  def publish(event) do
    GenServer.cast(__MODULE__, {:publish, event})
  end

  @impl true
  def init(_opts), do: {:ok, MapSet.new()}

  @impl true
  def handle_call({:subscribe, pid}, _from, subscribers) do
    {:reply, :ok, MapSet.put(subscribers, pid)}
  end

  def handle_call({:unsubscribe, pid}, _from, subscribers) do
    {:reply, :ok, MapSet.delete(subscribers, pid)}
  end

  @impl true
  def handle_cast({:publish, event}, subscribers) do
    Enum.each(subscribers, fn pid ->
      send(pid, {:library_notification, event})
    end)

    {:noreply, subscribers}
  end
end

defmodule LibraryLoans.Server do
  use GenServer

  @table __MODULE__
  @default_sweep_interval 60_000
  @default_notify_before 86_400

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def checkout(operation_id, loan_id, borrower, book, due_at) do
    GenServer.call(
      __MODULE__,
      {:operation, operation_id,
       {:checkout, loan_id, borrower, book, due_at}}
    )
  end

  def return_book(operation_id, loan_id) do
    GenServer.call(__MODULE__, {:operation, operation_id, {:return, loan_id}})
  end

  def list_loans do
    GenServer.call(__MODULE__, :list_loans)
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:data_file, "library_loans.dets")
      |> Path.expand()

    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} ->
        state = load_state()
        :ok = persist_state(state)

        interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)
        Process.send_after(self(), :sweep, interval)

        {:ok,
         %{
           data: state,
           sweep_interval: interval,
           notify_before: Keyword.get(opts, :notify_before, @default_notify_before)
         }}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(:list_loans, _from, state) do
    {:reply, Map.values(state.data.loans), state}
  end

  def handle_call({:operation, operation_id, operation}, _from, state) do
    case Map.fetch(state.data.operations, operation_id) do
      {:ok, previous_result} ->
        {:reply, previous_result, state}

      :error ->
        result = execute(operation, state.data)
        next_data = put_in(state.data.operations[operation_id], result)

        case persist_state(next_data) do
          :ok ->
            {:reply, result, %{state | data: next_data}}

          {:error, reason} ->
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end
    end
  end

  @impl true
  def handle_info(:sweep, state) do
    now = System.system_time(:second)
    {next_data, events} = find_notifications(state.data, now, state.notify_before)

    next_state =
      if next_data == state.data do
        state
      else
        case persist_state(next_data) do
          :ok ->
            Enum.each(events, &LibraryLoans.Notifications.publish/1)
            %{state | data: next_data}

          {:error, _reason} ->
            state
        end
      end

    Process.send_after(self(), :sweep, state.sweep_interval)
    {:noreply, next_state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp execute({:checkout, loan_id, borrower, book, due_at}, data)
       when is_integer(due_at) do
    if Map.has_key?(data.loans, loan_id) do
      {:error, :loan_id_already_exists}
    else
      loan = %{
        id: loan_id,
        borrower: borrower,
        book: book,
        due_at: due_at,
        checked_out_at: System.system_time(:second),
        returned_at: nil,
        status: :borrowed
      }

      {:ok, loan}
      |> then(fn result ->
        case result do
          {:ok, _} -> result
          error -> error
        end
      end)
      |> case do
        {:ok, _loan} = result -> result
        error -> error
      end
      |> maybe_store_loan(data, loan)
    end
  end

  defp execute({:checkout, _loan_id, _borrower, _book, _due_at}, _data),
    do: {:error, :invalid_due_at}

  defp execute({:return, loan_id}, data) do
    case Map.fetch(data.loans, loan_id) do
      {:ok, %{status: :borrowed} = loan} ->
        {:ok,
         Map.put(loan, :status, :returned)
         |> Map.put(:returned_at, System.system_time(:second))}

      {:ok, _loan} ->
        {:error, :loan_already_returned}

      :error ->
        {:error, :loan_not_found}
    end
  end

  defp maybe_store_loan({:ok, loan}, _data, loan), do: {:ok, loan}
  defp maybe_store_loan(result, _data, _loan), do: result

  defp find_notifications(data, now, notify_before) do
    Enum.reduce(data.loans, {data, []}, fn
      {_id, %{status: :borrowed} = loan}, {acc_data, events} ->
        kind =
          cond do
            loan.due_at <= now -> :overdue
            loan.due_at <= now + notify_before -> :due_soon
            true -> nil
          end

        notification_key = {loan.id, kind}

        if kind && not MapSet.member?(acc_data.notified, notification_key) do
          event = %{
            type: kind,
            loan_id: loan.id,
            borrower: loan.borrower,
            book: loan.book,
            due_at: loan.due_at
          }

          updated = %{
            acc_data
            | notified: MapSet.put(acc_data.notified, notification_key)
          }

          {updated, [event | events]}
        else
          {acc_data, events}
        end

      {_id, _loan}, result ->
        result
    end)
  end

  defp load_state do
    case :dets.lookup(@table, :state) do
      [{:state, state}] when is_map(state) ->
        %{
          loans: Map.get(state, :loans, %{}),
          operations: Map.get(state, :operations, %{}),
          notified: Map.get(state, :notified, MapSet.new())
        }

      _ ->
        %{loans: %{}, operations: %{}, notified: MapSet.new()}
    end
  end

  defp persist_state(state) do
    case :dets.insert(@table, {:state, state}) do
      :ok -> :dets.sync(@table)
      error -> error
    end
  end
end

defmodule LibraryLoans.Watchdog do
  use GenServer

  @check_interval 250

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :check)
    {:ok, %{pid: nil, ref: nil}}
  end

  @impl true
  def handle_info(:check, state) do
    pid = Process.whereis(LibraryLoans.Server)

    next_state =
      cond do
        is_nil(pid) ->
          state

        pid == state.pid ->
          state

        true ->
          if state.ref, do: Process.demonitor(state.ref, [:flush])
          %{pid: pid, ref: Process.monitor(pid)}
      end

    Process.send_after(self(), :check, @check_interval)
    {:noreply, next_state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %{ref: ref, pid: pid} = state) do
    unless reason in [:normal, :shutdown] do
      LibraryLoans.Notifications.publish(%{
        type: :process_crash,
        process: LibraryLoans.Server,
        reason: reason
      })
    end

    Process.send_after(self(), :check, 50)
    {:noreply, %{state | pid: nil, ref: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}
end
