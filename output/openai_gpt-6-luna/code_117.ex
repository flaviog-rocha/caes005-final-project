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

defmodule BookLoanManager do
  use GenServer

  @moduledoc """
  Gerencia empréstimos de livros, persistindo os registros em um arquivo local.
  Empréstimos ativos com data de devolução anterior ao dia atual são marcados
  periodicamente como atrasados.
  """

  @mutable_fields [:book_title, :borrower, :due_date, :status]
  @create_fields [:book_title, :borrower, :due_date]
  @statuses [:active, :overdue, :returned]

  # Inicia o servidor. Opções: :file, :interval e :name.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def create(attrs, server \\ __MODULE__) do
    GenServer.call(server, {:create, attrs})
  end

  def get(id, server \\ __MODULE__) do
    GenServer.call(server, {:get, id})
  end

  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  def update(id, attrs, server \\ __MODULE__) do
    GenServer.call(server, {:update, id, attrs})
  end

  def delete(id, server \\ __MODULE__) do
    GenServer.call(server, {:delete, id})
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :file, "book_loans.term")
    interval = Keyword.get(opts, :interval, 60_000)

    with true <- is_binary(path) and path != "",
         true <- is_integer(interval) and interval > 0,
         {:ok, records} <- load_records(path) do
      schedule_periodic_check(interval)

      {:ok, %{records: records, file: path, interval: interval}}
    else
      false -> {:stop, :invalid_options}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with :ok <- validate_create_attrs(attrs) do
      id = new_id(state.records)

      record =
        attrs
        |> Map.put(:id, id)
        |> Map.put(:status, :active)

      records = Map.put(state.records, id, record)

      case persist(records, state.file) do
        :ok -> {:reply, {:ok, record}, %{state | records: records}}
        {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      if valid_id?(id) do
        case Map.fetch(state.records, id) do
          {:ok, record} -> {:ok, record}
          :error -> {:error, :not_found}
        end
      else
        {:error, :invalid_id}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    records = state.records |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, records, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with true <- valid_id?(id),
         {:ok, current} <- fetch_record(state.records, id),
         :ok <- validate_update_attrs(attrs),
         updated <- Map.merge(current, attrs),
         :ok <- validate_record(updated) do
      records = Map.put(state.records, id, updated)

      case persist(records, state.file) do
        :ok -> {:reply, {:ok, updated}, %{state | records: records}}
        {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
      end
    else
      false -> {:reply, {:error, :invalid_id}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    cond do
      not valid_id?(id) ->
        {:reply, {:error, :invalid_id}, state}

      not Map.has_key?(state.records, id) ->
        {:reply, {:error, :not_found}, state}

      true ->
        records = Map.delete(state.records, id)

        case persist(records, state.file) do
          :ok -> {:reply, :ok, %{state | records: records}}
          {:error, reason} -> {:reply, {:error, {:persistence, reason}}, state}
        end
    end
  end

  @impl true
  def handle_info(:periodic_check, state) do
    server = self()

    # A Task agenda a atualização periódica no processo que mantém o estado.
    Task.start(fn -> GenServer.cast(server, :mark_overdue) end)

    schedule_periodic_check(state.interval)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:mark_overdue, state) do
    today = Date.utc_today()

    records =
      Map.new(state.records, fn {id, record} ->
        updated =
          if record.status == :active and Date.compare(record.due_date, today) == :lt do
            %{record | status: :overdue}
          else
            record
          end

        {id, updated}
      end)

    if records == state.records do
      {:noreply, state}
    else
      case persist(records, state.file) do
        :ok -> {:noreply, %{state | records: records}}
        {:error, _reason} -> {:noreply, state}
      end
    end
  end

  defp validate_create_attrs(attrs) when is_map(attrs) do
    cond do
      not Enum.all?(Map.keys(attrs), &(&1 in @create_fields)) ->
        {:error, :invalid_fields}

      not Enum.all?(@create_fields, &Map.has_key?(attrs, &1)) ->
        {:error, :missing_fields}

      not valid_text?(Map.get(attrs, :book_title)) ->
        {:error, :invalid_book_title}

      not valid_text?(Map.get(attrs, :borrower)) ->
        {:error, :invalid_borrower}

      not valid_date?(Map.get(attrs, :due_date)) ->
        {:error, :invalid_due_date}

      true ->
        :ok
    end
  end

  defp validate_create_attrs(_), do: {:error, :invalid_attributes}

  defp validate_update_attrs(attrs) when is_map(attrs) do
    cond do
      map_size(attrs) == 0 ->
        {:error, :empty_update}

      not Enum.all?(Map.keys(attrs), &(&1 in @mutable_fields)) ->
        {:error, :invalid_fields}

      Map.has_key?(attrs, :book_title) and not valid_text?(attrs.book_title) ->
        {:error, :invalid_book_title}

      Map.has_key?(attrs, :borrower) and not valid_text?(attrs.borrower) ->
        {:error, :invalid_borrower}

      Map.has_key?(attrs, :due_date) and not valid_date?(attrs.due_date) ->
        {:error, :invalid_due_date}

      Map.has_key?(attrs, :status) and attrs.status not in @statuses ->
        {:error, :invalid_status}

      true ->
        :ok
    end
  end

  defp validate_update_attrs(_), do: {:error, :invalid_attributes}

  defp validate_record(%{
         id: id,
         book_title: title,
         borrower: borrower,
         due_date: due_date,
         status: status
       }) do
    cond do
      not valid_id?(id) -> {:error, :invalid_id}
      not valid_text?(title) -> {:error, :invalid_book_title}
      not valid_text?(borrower) -> {:error, :invalid_borrower}
      not valid_date?(due_date) -> {:error, :invalid_due_date}
      status not in @statuses -> {:error, :invalid_status}
      true -> :ok
    end
  end

  defp validate_record(_), do: {:error, :invalid_record}

  defp valid_text?(value), do: is_binary(value) and String.trim(value) != ""

  defp valid_date?(%Date{} = date), do: Date.valid?(date)
  defp valid_date?(_), do: false

  defp valid_id?(id), do: is_integer(id) and id > 0

  defp fetch_record(records, id) do
    case Map.fetch(records, id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  defp new_id(records) do
    id = System.unique_integer([:positive, :monotonic])
    if Map.has_key?(records, id), do: new_id(records), else: id
  end

  defp schedule_periodic_check(interval) do
    Process.send_after(self(), :periodic_check, interval)
  end

  defp load_records(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          records = :erlang.binary_to_term(binary, [:safe])

          if valid_records?(records) do
            {:ok, records}
          else
            {:error, :invalid_persisted_data}
          end
        rescue
          ArgumentError -> {:error, :invalid_persisted_data}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, {:file_read, reason}}
    end
  end

  defp valid_records?(records) when is_map(records) do
    Enum.all?(records, fn {id, record} ->
      valid_id?(id) and Map.get(record, :id) == id and validate_record(record) == :ok
    end)
  end

  defp valid_records?(_), do: false

  defp persist(records, path) do
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp.#{System.unique_integer([:positive])}"
    binary = :erlang.term_to_binary(records)

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, binary, [:binary, :sync]),
         :ok <- File.rename(temporary_path, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end
end
