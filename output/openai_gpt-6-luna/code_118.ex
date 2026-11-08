# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LoanManager do
  use GenServer

  @default_file "loans.db"
  @default_interval 60_000
  @statuses ["active", "overdue", "returned"]
  @fields [:book, :borrower, :due_date, :status]

  # Public API

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def create(attrs, server \\ __MODULE__) do
    with {:ok, attrs} <- normalize_attributes(attrs, [:book, :borrower, :due_date]),
         :ok <- validate_required(attrs, [:book, :borrower, :due_date]) do
      GenServer.call(server, {:create, attrs})
    end
  end

  def get(id, server \\ __MODULE__) do
    with :ok <- validate_id(id) do
      GenServer.call(server, {:get, id})
    end
  end

  def list(server \\ __MODULE__), do: GenServer.call(server, :list)

  def update(id, attrs, server \\ __MODULE__) do
    with :ok <- validate_id(id),
         {:ok, attrs} <- normalize_attributes(attrs, @fields),
         :ok <- validate_partial(attrs) do
      GenServer.call(server, {:update, id, attrs})
    end
  end

  def delete(id, server \\ __MODULE__) do
    with :ok <- validate_id(id) do
      GenServer.call(server, {:delete, id})
    end
  end

  def stop(server \\ __MODULE__), do: GenServer.stop(server)

  # GenServer callbacks

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_file)
    interval = Keyword.get(opts, :interval, @default_interval)

    if is_binary(path) and is_integer(interval) and interval > 0 do
      case load_data(path) do
        {:ok, loans, next_id} ->
          state = %{loans: loans, next_id: next_id, path: path, interval: interval}

          case persist(state) do
            :ok ->
              schedule_periodic(interval)
              {:ok, state}

            {:error, reason} ->
              {:stop, {:persistence_error, reason}}
          end

        {:error, reason} ->
          {:stop, {:load_error, reason}}
      end
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    record = Map.merge(attrs, %{id: state.next_id, status: "active"})
    new_state = %{state | loans: Map.put(state.loans, record.id, record), next_id: state.next_id + 1}

    case persist(new_state) do
      :ok -> {:reply, {:ok, record}, new_state}
      {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.loans, id) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    records = state.loans |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, records}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    case Map.fetch(state.loans, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, record} ->
        updated = Map.merge(record, attrs)

        case validate_record(updated) do
          :ok ->
            new_state = %{state | loans: Map.put(state.loans, id, updated)}

            case persist(new_state) do
              :ok -> {:reply, {:ok, updated}, new_state}
              {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.fetch(state.loans, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, record} ->
        new_state = %{state | loans: Map.delete(state.loans, id)}

        case persist(new_state) do
          :ok -> {:reply, {:ok, record}, new_state}
          {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
        end
    end
  end

  @impl true
  def handle_info(:periodic, state) do
    schedule_periodic(state.interval)

    state =
      if Map.get(state, :task_ref) do
        state
      else
        task = Task.async(fn -> find_overdue_ids(state.loans, Date.utc_today()) end)
        Map.put(state, :task_ref, task.ref)
      end

    {:noreply, state}
  end

  def handle_info({ref, ids}, %{task_ref: ref} = state) do
    Process.demonitor(ref, [:flush])

    loans =
      Enum.reduce(ids, state.loans, fn id, acc ->
        case Map.fetch(acc, id) do
          {:ok, %{status: "active"} = record} ->
            Map.put(acc, id, Map.put(record, :status, "overdue"))

          _ ->
            acc
        end
      end)

    new_state = Map.delete(state, :task_ref)

    if loans == state.loans do
      {:noreply, new_state}
    else
      updated_state = %{new_state | loans: loans}

      case persist(updated_state) do
        :ok -> {:noreply, updated_state}
        {:error, _reason} -> {:noreply, new_state}
      end
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{task_ref: ref} = state) do
    {:noreply, Map.delete(state, :task_ref)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  # Validation

  defp normalize_attributes(attrs, allowed) when is_map(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case normalize_key(key) do
        field when field in allowed ->
          if Map.has_key?(acc, field) do
            {:halt, {:error, :duplicate_attribute}}
          else
            {:cont, {:ok, Map.put(acc, field, value)}}
          end

        _ ->
          {:halt, {:error, :unknown_attribute}}
      end
    end)
  end

  defp normalize_attributes(_attrs, _allowed), do: {:error, :invalid_attributes}

  defp normalize_key(key) when key in @fields, do: key
  defp normalize_key("book"), do: :book
  defp normalize_key("borrower"), do: :borrower
  defp normalize_key("due_date"), do: :due_date
  defp normalize_key("status"), do: :status
  defp normalize_key(_), do: nil

  defp validate_required(attrs, required) do
    if Enum.all?(required, &Map.has_key?(attrs, &1)) do
      validate_partial(attrs)
    else
      {:error, :missing_attributes}
    end
  end

  defp validate_partial(attrs) do
    valid =
      Enum.all?(attrs, fn
        {:book, value} -> valid_text?(value)
        {:borrower, value} -> valid_text?(value)
        {:due_date, value} -> valid_date?(value)
        {:status, value} -> value in @statuses
        _ -> false
      end)

    if valid, do: :ok, else: {:error, :invalid_attributes}
  end

  defp validate_record(record) do
    if is_map(record) and Map.keys(record) |> Enum.sort() == Enum.sort([:id | @fields]) and
         validate_id(record.id) == :ok and validate_partial(Map.take(record, @fields)) == :ok do
      :ok
    else
      {:error, :invalid_record}
    end
  end

  defp validate_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_id(_), do: {:error, :invalid_id}

  defp valid_text?(value), do: is_binary(value) and String.trim(value) != ""

  defp valid_date?(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, _date} -> true
      _ -> false
    end
  end

  defp valid_date?(_), do: false

  # Persistence

  defp load_data(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            %{version: 1, loans: loans, next_id: next_id}
            when is_map(loans) and is_integer(next_id) and next_id > 0 ->
              if valid_loaded_data?(loans, next_id) do
                {:ok, loans, next_id}
              else
                {:error, :invalid_data}
              end

            _ ->
              {:error, :invalid_data}
          end
        rescue
          _ -> {:error, :invalid_data}
        end

      {:error, :enoent} ->
        {:ok, %{}, 1}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp valid_loaded_data?(loans, next_id) do
    Enum.all?(loans, fn
      {id, %{id: ^id} = record} -> validate_record(record) == :ok
      _ -> false
    end) and
      next_id > (loans |> Map.keys() |> Enum.max(fn -> 0 end))
  end

  defp persist(state) do
    data = %{version: 1, loans: state.loans, next_id: state.next_id}
    binary = :erlang.term_to_binary(data, [:compressed])
    temporary_path = state.path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(state.path)),
         :ok <- File.write(temporary_path, binary, [:binary]),
         :ok <- File.rename(temporary_path, state.path) do
      :ok
    end
  end

  # Periodic processing

  defp schedule_periodic(interval), do: Process.send_after(self(), :periodic, interval)

  defp find_overdue_ids(loans, today) do
    Enum.flat_map(loans, fn
      {id, %{status: "active", due_date: due_date}} ->
        case Date.from_iso8601(due_date) do
          {:ok, date} when Date.compare(date, today) == :lt -> [id]
          _ -> []
        end

      _ ->
        []
    end)
  end
end
