# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvStore do
  use GenServer

  @id_key "id"

  # Inicia um armazenamento CSV. O processo pode ser monitorado com monitor/1.
  def start_link(path) when is_binary(path) do
    GenServer.start_link(__MODULE__, path)
  end

  def start_link({path, opts}) when is_binary(path) and is_list(opts) do
    GenServer.start_link(__MODULE__, path, opts)
  end

  def create(server, record), do: GenServer.call(server, {:create, record})
  def get(server, id), do: GenServer.call(server, {:get, id})
  def update(server, id, changes), do: GenServer.call(server, {:update, id, changes})
  def delete(server, id), do: GenServer.call(server, {:delete, id})
  def all(server), do: GenServer.call(server, :all)

  # O processo monitorado envia uma mensagem :DOWN ao chamador se terminar.
  def monitor(server), do: Process.monitor(server)

  def await_termination(reference, timeout \\ :infinity) do
    receive do
      {:DOWN, ^reference, :process, pid, reason} ->
        {:ok, pid, reason}
    after
      timeout ->
        :timeout
    end
  end

  # Executa a função para cada registro CSV, limitando a quantidade de tarefas
  # simultâneas. Uma tarefa que termina inesperadamente produz um erro explícito.
  def process_csv(path, fun, opts \\ [])
      when is_binary(path) and is_function(fun, 1) and is_list(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if is_integer(max_concurrency) and max_concurrency > 0 do
      with {:ok, records} <- read_csv_records(path) do
        records
        |> Task.async_stream(fun,
          max_concurrency: max_concurrency,
          ordered: true,
          timeout: :infinity,
          on_timeout: :kill_task
        )
        |> Enum.reduce_while({:ok, []}, fn
          {:ok, result}, {:ok, results} ->
            {:cont, {:ok, [result | results]}}

          {:exit, reason}, _acc ->
            {:halt, {:error, {:worker_terminated, reason}}}
        end)
        |> case do
          {:ok, results} -> {:ok, Enum.reverse(results)}
          error -> error
        end
      end
    else
      {:error, :invalid_max_concurrency}
    end
  end

  def process_csv(_path, _fun, _opts), do: {:error, :invalid_arguments}

  @impl true
  def init(path) do
    case load_store(path) do
      {:ok, columns, records} ->
        {:ok, %{path: path, columns: columns, records: records}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, input}, _from, state) do
    with {:ok, record} <- normalize_record(input),
         id when is_binary(id) and id != "" <- Map.get(record, @id_key),
         false <- Map.has_key?(state.records, id) do
      records = Map.put(state.records, id, record)
      columns = Enum.uniq(state.columns ++ Map.keys(record))

      commit(state, columns, records, {:ok, record})
    else
      true -> {:reply, {:error, :already_exists}, state}
      nil -> {:reply, {:error, :missing_id}, state}
      {:error, _} = error -> {:reply, error, state}
      _ -> {:reply, {:error, :invalid_id}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case Map.fetch(state.records, to_string(id)) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:update, id, changes}, _from, state) do
    id = to_string(id)

    with {:ok, current} <- fetch_record(state.records, id),
         {:ok, normalized_changes} <- normalize_record(changes),
         false <- Map.has_key?(normalized_changes, @id_key) do
      updated = Map.merge(current, normalized_changes)
      records = Map.put(state.records, id, updated)
      columns = Enum.uniq(state.columns ++ Map.keys(normalized_changes))

      commit(state, columns, records, {:ok, updated})
    else
      true -> {:reply, {:error, :cannot_change_id}, state}
      {:error, _} = error -> {:reply, error, state}
      _ -> {:reply, {:error, :invalid_changes}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    id = to_string(id)

    case Map.fetch(state.records, id) do
      {:ok, record} ->
        records = Map.delete(state.records, id)
        commit(state, state.columns, records, {:ok, record})

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:all, _from, state) do
    records =
      state.records
      |> Map.values()
      |> Enum.sort_by(&Map.get(&1, @id_key))

    {:reply, {:ok, records}, state}
  end

  defp fetch_record(records, id) do
    case Map.fetch(records, id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  # Persiste primeiro em um arquivo temporário e troca o arquivo de destino
  # atomicamente; o estado em memória só é atualizado após a troca bem-sucedida.
  defp commit(state, columns, records, reply) do
    case write_store(state.path, columns, records) do
      :ok ->
        {:reply, reply, %{state | columns: columns, records: records}}

      {:error, reason} ->
        {:reply, {:error, {:persistence_failed, reason}}, state}
    end
  end

  defp load_store(path) do
    case File.read(path) do
      {:ok, contents} ->
        with {:ok, [header | rows]} <- parse_csv(contents),
             :ok <- validate_header(header),
             {:ok, records} <- records_from_rows(header, rows) do
          {:ok, header, records}
        else
          {:ok, []} -> {:error, :missing_header}
          {:error, _} = error -> error
          _ -> {:error, :invalid_csv}
        end

      {:error, :enoent} ->
        case write_store(path, [@id_key], %{}) do
          :ok -> {:ok, [@id_key], %{}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp records_from_rows(header, rows) do
    Enum.reduce_while(rows, {:ok, %{}}, fn row, {:ok, records} ->
      if length(row) != length(header) do
        {:halt, {:error, :invalid_row_length}}
      else
        record = header |> Enum.zip(row) |> Map.new()
        id = Map.get(record, @id_key)

        cond do
          is_nil(id) or id == "" ->
            {:halt, {:error, :missing_id}}

          Map.has_key?(records, id) ->
            {:halt, {:error, :duplicate_id}}

          true ->
            {:cont, {:ok, Map.put(records, id, record)}}
        end
      end
    end)
  end

  defp validate_header(header) do
    cond do
      header == [] -> {:error, :missing_header}
      not Enum.member?(header, @id_key) -> {:error, :missing_id_column}
      length(Enum.uniq(header)) != length(header) -> {:error, :duplicate_column}
      true -> :ok
    end
  end

  defp normalize_record(record) when is_map(record) do
    Enum.reduce_while(record, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      with {:ok, normalized_key} <- normalize_key(key),
           {:ok, normalized_value} <- normalize_value(value),
           false <- Map.has_key?(acc, normalized_key) do
        {:cont, {:ok, Map.put(acc, normalized_key, normalized_value)}}
      else
        true -> {:halt, {:error, :duplicate_column}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_record(_), do: {:error, :invalid_record}

  defp normalize_key(key) when is_binary(key), do: {:ok, key}
  defp normalize_key(key) when is_atom(key), do: {:ok, Atom.to_string(key)}
  defp normalize_key(_), do: {:error, :invalid_column}

  defp normalize_value(nil), do: {:ok, ""}

  defp normalize_value(value)
       when is_binary(value) or is_integer(value) or is_float(value) or is_atom(value) do
    {:ok, to_string(value)}
  end

  defp normalize_value(_), do: {:error, :invalid_value}

  defp write_store(path, columns, records) do
    columns = if Enum.member?(columns, @id_key), do: columns, else: [@id_key | columns]

    rows =
      records
      |> Map.values()
      |> Enum.sort_by(&Map.get(&1, @id_key))
      |> Enum.map(fn record -> Enum.map(columns, &Map.get(record, &1, "")) end)

    contents =
      [columns | rows]
      |> Enum.map_join("\n", fn row ->
        Enum.map_join(row, ",", &encode_csv_field/1)
      end)
      |> Kernel.<>("\n")

    directory = Path.dirname(path)
    temporary = path <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary, contents, [:binary]),
         :ok <- File.rename(temporary, path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary)
        {:error, reason}
    end
  end

  defp encode_csv_field(value) do
    value = to_string(value)

    if Enum.any?([",", "\"", "\n", "\r"], &String.contains?(value, &1)) do
      "\"" <> String.replace(value, "\"", "\"\"") <> "\""
    else
      value
    end
  end

  defp read_csv_records(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, [header | rows]} <- parse_csv(contents),
         :ok <- validate_header_for_processing(header),
         {:ok, records} <- processing_records(header, rows) do
      {:ok, records}
    else
      {:ok, []} -> {:error, :missing_header}
      {:error, _} = error -> error
      _ -> {:error, :invalid_csv}
    end
  end

  defp validate_header_for_processing(header) do
    cond do
      header == [] -> {:error, :missing_header}
      length(Enum.uniq(header)) != length(header) -> {:error, :duplicate_column}
      true -> :ok
    end
  end

  defp processing_records(header, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, records} ->
      if length(row) == length(header) do
        {:cont, {:ok, [Map.new(Enum.zip(header, row)) | records]}}
      else
        {:halt, {:error, :invalid_row_length}}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      error -> error
    end
  end

  defp parse_csv(contents) when is_binary(contents) do
    parse_csv_chars(String.to_charlist(contents), [], [], [], false, false)
  end

  defp parse_csv_chars([], fields, field, rows, true, _started),
    do: {:error, :unterminated_quoted_field}

  defp parse_csv_chars([], fields, field, rows, false, started) do
    if started do
      row = Enum.reverse([field |> Enum.reverse() |> List.to_string() | fields])
      {:ok, Enum.reverse([row | rows])}
    else
      {:ok, Enum.reverse(rows)}
    end
  end

  defp parse_csv_chars([?", ?" | rest], fields, field, rows, true, _started) do
    parse_csv_chars(rest, fields, [?" | field], rows, true, true)
  end

  defp parse_csv_chars([?" | rest], fields, field, rows, true, started) do
    parse_csv_chars(rest, fields, field, rows, false, started)
  end

  defp parse_csv_chars([char | rest], fields, field, rows, true, started) do
    parse_csv_chars(rest, fields, [char | field], rows, true, started)
  end

  defp parse_csv_chars([?, | rest], fields, field, rows, false, _started) do
    value = field |> Enum.reverse() |> List.to_string()
    parse_csv_chars(rest, [value | fields], [], rows, false, true)
  end

  defp parse_csv_chars([?\r, ?\n | rest], fields, field, rows, false, started) do
    row = Enum.reverse([field |> Enum.reverse() |> List.to_string() | fields])
    parse_csv_chars(rest, [], [], [row | rows], false, false)
  end

  defp parse_csv_chars([?\n | rest], fields, field, rows, false, started) do
    row = Enum.reverse([field |> Enum.reverse() |> List.to_string() | fields])
    parse_csv_chars(rest, [], [], [row | rows], false, false)
  end

  defp parse_csv_chars([?" | rest], fields, [], rows, false, _started) do
    parse_csv_chars(rest, fields, [], rows, true, true)
  end

  defp parse_csv_chars([?" | _rest], _fields, _field, _rows, false, _started),
    do: {:error, :unexpected_quote}

  defp parse_csv_chars([char | rest], fields, field, rows, false, _started) do
    parse_csv_chars(rest, fields, [char | field], rows, false, true)
  end
end
