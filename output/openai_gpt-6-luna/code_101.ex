# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Executar determinadas operações periodicamente.
# 3. Manter um histórico das operações realizadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryLoans do
  use GenServer

  @loans_table :library_loans_dets
  @history_table :library_loans_history_dets
  @default_interval 60_000
  @loan_fields [:book, :borrower, :due_at]

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def create_loan(server \\ __MODULE__, attrs) do
    GenServer.call(server, {:create_loan, attrs})
  end

  def get_loan(server \\ __MODULE__, id) do
    GenServer.call(server, {:get_loan, id})
  end

  def list_loans(server \\ __MODULE__) do
    GenServer.call(server, :list_loans)
  end

  def update_loan(server \\ __MODULE__, id, attrs) do
    GenServer.call(server, {:update_loan, id, attrs})
  end

  def delete_loan(server \\ __MODULE__, id) do
    GenServer.call(server, {:delete_loan, id})
  end

  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(opts) do
    loans_file = opts |> Keyword.get(:loans_file, "library_loans.dets") |> Path.expand()
    history_file = opts |> Keyword.get(:history_file, "library_loans_history.dets") |> Path.expand()
    interval = Keyword.get(opts, :interval, @default_interval)

    if not (is_integer(interval) and interval > 0) do
      {:stop, :invalid_interval}
    else
      case :dets.open_file(@loans_table, file: String.to_charlist(loans_file), type: :set, repair: true) do
        {:ok, _} ->
          case :dets.open_file(@history_table,
                 file: String.to_charlist(history_file),
                 type: :set,
                 repair: true
               ) do
            {:ok, _} ->
              state = %{
                interval: interval,
                next_id: next_key(@loans_table),
                next_history_id: next_key(@history_table)
              }

              schedule_sweep(interval)
              {:ok, state}

            {:error, reason} ->
              :dets.close(@loans_table)
              {:stop, reason}
          end

        {:error, reason} ->
          {:stop, reason}
      end
    end
  end

  @impl true
  def handle_call({:create_loan, attrs}, _from, state) when is_map(attrs) do
    if valid_attrs?(attrs) do
      now = System.system_time(:second)

      loan = %{
        id: state.next_id,
        book: Map.fetch!(attrs, :book),
        borrower: Map.fetch!(attrs, :borrower),
        due_at: Map.fetch!(attrs, :due_at),
        status: :active,
        created_at: now,
        updated_at: now
      }

      case :dets.insert(@loans_table, {loan.id, loan}) do
        :ok ->
          event = %{type: :created, loan: loan}

          case append_history(state, loan.id, event) do
            {:ok, new_state} ->
              {:reply, {:ok, loan}, %{new_state | next_id: state.next_id + 1}}

            {:error, reason, new_state} ->
              {:reply, {:error, reason}, new_state}
          end

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :invalid_loan}, state}
    end
  end

  def handle_call({:create_loan, _attrs}, _from, state) do
    {:reply, {:error, :invalid_loan}, state}
  end

  def handle_call({:get_loan, id}, _from, state) do
    reply =
      case :dets.lookup(@loans_table, id) do
        [{^id, loan}] -> {:ok, loan}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_loans, _from, state) do
    loans =
      :dets.foldl(fn {_id, loan}, acc -> [loan | acc] end, [], @loans_table)
      |> Enum.sort_by(& &1.id)

    {:reply, loans, state}
  end

  def handle_call({:update_loan, id, attrs}, _from, state) when is_map(attrs) do
    case :dets.lookup(@loans_table, id) do
      [{^id, loan}] ->
        if valid_update?(attrs) do
          updated =
            loan
            |> Map.merge(attrs)
            |> Map.put(:updated_at, System.system_time(:second))

          case :dets.insert(@loans_table, {id, updated}) do
            :ok ->
              event = %{type: :updated, loan: updated}

              case append_history(state, id, event) do
                {:ok, new_state} -> {:reply, {:ok, updated}, new_state}
                {:error, reason, new_state} -> {:reply, {:error, reason}, new_state}
              end

            {:error, reason} ->
              {:reply, {:error, reason}, state}
          end
        else
          {:reply, {:error, :invalid_loan}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:update_loan, _id, _attrs}, _from, state) do
    {:reply, {:error, :invalid_loan}, state}
  end

  def handle_call({:delete_loan, id}, _from, state) do
    case :dets.lookup(@loans_table, id) do
      [{^id, loan}] ->
        case :dets.delete(@loans_table, id) do
          :ok ->
            event = %{type: :deleted, loan: loan}

            case append_history(state, id, event) do
              {:ok, new_state} -> {:reply, :ok, new_state}
              {:error, reason, new_state} -> {:reply, {:error, reason}, new_state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:history, _from, state) do
    events =
      :dets.foldl(fn {_id, event}, acc -> [event | acc] end, [], @history_table)
      |> Enum.sort_by(& &1.id)

    {:reply, events, state}
  end

  @impl true
  def handle_info(:periodic_sweep, state) do
    now = System.system_time(:second)

    loans =
      :dets.foldl(
        fn {_id, loan}, acc ->
          if loan.status == :active and loan.due_at <= now, do: [loan | acc], else: acc
        end,
        [],
        @loans_table
      )

    state =
      Enum.reduce(loans, state, fn loan, acc ->
        updated = %{loan | status: :overdue, updated_at: now}

        case :dets.insert(@loans_table, {loan.id, updated}) do
          :ok ->
            case append_history(acc, loan.id, %{type: :marked_overdue, loan: updated}) do
              {:ok, new_state} -> new_state
              {:error, _reason, new_state} -> new_state
            end

          {:error, _reason} ->
            acc
        end
      end)

    schedule_sweep(state.interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@loans_table)
    :dets.close(@history_table)
    :ok
  end

  defp valid_attrs?(attrs) do
    Map.keys(attrs) |> Enum.all?(&(&1 in @loan_fields)) and
      valid_loan_fields?(attrs)
  end

  defp valid_update?(attrs) do
    Map.keys(attrs) |> Enum.all?(&(&1 in @loan_fields))
  end

  defp valid_loan_fields?(attrs) do
    is_binary(Map.get(attrs, :book)) and Map.get(attrs, :book) != "" and
      is_binary(Map.get(attrs, :borrower)) and Map.get(attrs, :borrower) != "" and
      is_integer(Map.get(attrs, :due_at))
  end

  defp append_history(state, loan_id, details) do
    id = state.next_history_id

    event = %{
      id: id,
      loan_id: loan_id,
      at: System.system_time(:second),
      details: details
    }

    case :dets.insert(@history_table, {id, event}) do
      :ok -> {:ok, %{state | next_history_id: id + 1}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp next_key(table) do
    :dets.foldl(fn {id, _value}, highest -> max(id, highest) end, 0, table) + 1
  end

  defp schedule_sweep(interval) do
    Process.send_after(self(), :periodic_sweep, interval)
  end
end
