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

  @default_header ["id"]

  def start_link(path, opts \\ []) do
    name = Keyword.get(opts, :name)
    GenServer.start_link(__MODULE__, {path, opts}, name: name)
  end

  def create(server, record), do: safe_call(server, {:create, record})
  def get(server, id), do: safe_call(server, {:get, id})
  def update(server, id, attrs), do: safe_call(server, {:update, id, attrs})
  def delete(server, id), do: safe_call(server, {:delete, id})
  def max_concurrency(server), do: safe_call(server, :max_concurrency)

  # Process rows concurrently up to the configured limit. Worker exits are returned
  # as errors rather than being silently discarded.
  def process_rows(server, rows, fun) when is_list(rows) and is_function(fun, 1) do
    with {:ok, limit} <- max_concurrency(server) do
      rows
      |> Enum.with_index()
      |> Task.async_stream(
        fn {row, index} -> {index, fun.(row)} end,
        max_concurrency: limit,
        ordered: true,
        timeout: :infinity,
        on_timeout: :kill_task
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {_index, result}}, {:ok, acc} ->
          {:cont, {:ok, [result | acc]}}

        {:exit, reason}, _acc ->
          {:halt, {:error, {:worker_terminated, reason}}}
      end)
      |> case do
        {:ok, results} -> {:ok, Enum.reverse(results)}
        error -> error
      end
    end
  end

  def process_rows(_server, _rows, _fun), do: {:error, :invalid_arguments}

  @impl true
  def init({path, opts}) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      path = Path.expand(path)

      with :ok <- File.mkdir_p(Path.dirname(path)),
           :ok <- initialize_file(path) do
        {:ok, %{path: path, max_concurrency: max_concurrency}}
      else
        {:error, reason} -> {:stop, reason}
      end
    end
  end

  @impl true
  def handle_call(:max_concurrency, _from, state) do
    {:reply, {:ok, state.max_concurrency}, state}
  end

  def handle_call({:create, record}, _from, state) do
    reply = mutate(state.path, fn {headers, records} ->
      record = normalize_record(record)

      if not is_map(record) do
        {:error, :invalid_record}
      else
        id = Map.get(record, "id") || next_id(records)
        record = Map.put(record, "id", to_string(id))

        if Enum.any?(records, &(same_id?(Map.get(&1, "id"), id))) do
          {:error, :already_exists}
        else
          headers = append_missing_headers(headers, Map.keys(record))
          {:ok, record, {headers, records ++ [record]}}
        end
      end
    end)

    {:reply, reply, state}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      with {:ok, {_, records}} <- read_csv(state.path) do
        case Enum.find(records, &same_id?(Map.get(&1, "id"), id)) do
          nil -> {:error, :not_found}
          record -> {:ok, record}
        end
      end

    {:reply, reply, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    reply = mutate(state.path, fn {headers, records} ->
      attrs = normalize_record(attrs)

      if not is_map(attrs) do
        {:error, :invalid_record}
      else
        case Enum.find_index(records, &same_id?(Map.get(&1, "id"), id)) do
          nil ->
            {:error, :not_found}

          index ->
            current = Enum.at(records, index)
            updated = current |> Map.merge(Map.delete(attrs, "id")) |> Map.put("id", current["id"])
            updated_records = List.replace_at(records, index, updated)
            headers = append_missing_headers(headers, Map.keys(updated))
            {:ok, updated, {headers, updated_records}}
        end
      end
    end)

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply = mutate(state.path, fn {headers, records} ->
      case Enum.find_index(records, &same_id?(Map.get(&1, "id"), id)) do
        nil ->
          {:error, :not_found}

        index ->
          deleted = Enum.at(records, index)
          {:ok, deleted, {headers, List.delete_at(records, index)}}
      end
    end)

    {:reply, reply, state}
  end

  defp safe_call(server, message) do
    try do
      GenServer.call(server, message, :infinity)
    catch
      :exit, reason -> {:error, {:server_terminated, reason}}
    end
  end

  defp initialize_file(path) do
    case File.read(path) do
      {:ok, _contents} -> :ok
      {:error, :enoent} -> File.write(path, encode_csv([@default_header]))
      {:error, reason} -> {:error, reason}
    end
  end

  # Every mutation is committed by writing a complete temporary file and renaming it.
  defp mutate(path, fun) do
    with {:ok, {headers, records}} <- read_csv(path),
         result when is_tuple(result) <- fun.({headers, records}) do
      case result do
        {:error, reason} ->
          {:error, reason}

        {:ok, value, {new_headers, new_records}} ->
          case atomic_write(path, new_headers, new_records) do
            :ok -> {:ok, value}
            {:error, reason} -> {:error, {:write_failed, reason}}
          end
      end
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_operation}
    end
  end

  defp read_csv(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, rows} <- parse_csv(contents) do
      case rows do
        [] ->
          {:ok, {@default_header, []}}

        [header | data_rows] ->
          header = Enum.map(header, &String.trim/1)

          records =
            Enum.map(data_rows, fn row ->
              row = row ++ List.duplicate("", max(length(header) - length(row), 0))
              row = Enum.take(row, length(header))
              Map.new(Enum.zip(header, row))
            end)

          {:ok, {header, records}}
      end
    end
  end

  defp atomic_write(path, headers, records) do
    content =
      [headers | Enum.map(records, fn record ->
        Enum.map(headers, &Map.get(record, &1, ""))
      end)]
      |> encode_csv()

    temp_path = path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive]))

    case File.write(temp_path, content, [:binary, :sync]) do
      :ok ->
        case File.rename(temp_path, path) do
          :ok -> :ok
          {:error, reason} ->
            File.rm(temp_path)
            {:error, reason}
        end

      {:error, reason} ->
        File.rm(temp_path)
        {:error, reason}
    end
  end

  defp normalize_record(record) when is_map(record) do
    Map.new(record, fn {key, value} ->
      {if(is_atom(key), do: Atom.to_string(key), else: to_string(key)),
       if(is_nil(value), do: "", else: to_string(value))}
    end)
  end

  defp normalize_record(_), do: nil

  defp append_missing_headers(headers, keys) do
    headers ++ Enum.reject(keys, &(&1 in headers))
  end

  defp next_id(records) do
    records
    |> Enum.map(&parse_integer(Map.get(&1, "id")))
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> 1
      ids -> Enum.max(ids) + 1
    end
  end

  defp parse_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {number, ""} -> number
      _ -> nil
    end
  end

  defp parse_integer(_), do: nil

  defp same_id?(stored, requested), do: not is_nil(stored) and to_string(stored) == to_string(requested)

  defp encode_csv(rows) do
    rows
    |> Enum.map(fn row ->
      row
      |> Enum.map(&encode_field/1)
      |> Enum.join(",")
    end)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp encode_field(value) do
    value = to_string(value)

    if String.contains?(value, [",", "\"", "\n", "\r"]) do
      "\"" <> String.replace(value, "\"", "\"\"") <> "\""
    else
      value
    end
  end

  defp parse_csv(contents) do
    parse_chars(String.to_charlist(contents), [], [], [], false, false, [])
  end

  defp parse_chars([], _field, _row, _rows, true, _started, _acc),
    do: {:error, :unterminated_quoted_field}

  defp parse_chars([], field, row, rows, false, started, _acc) do
    if started or field != [] or row != [] do
      {:ok, Enum.reverse([Enum.reverse([to_string(Enum.reverse(field)) | row]) | rows])}
    else
      {:ok, Enum.reverse(rows)}
    end
  end

  defp parse_chars([?", ?" | rest], field, row, rows, true, _started, acc) do
    parse_chars(rest, [?" | field], row, rows, true, true, acc)
  end

  defp parse_chars([?" | rest], field, row, rows, true, started, acc) do
    parse_chars(rest, field, row, rows, false, started, acc)
  end

  defp parse_chars([char | rest], field, row, rows, true, _started, acc) do
    parse_chars(rest, [char | field], row, rows, true, true, acc)
  end

  defp parse_chars([?, | rest], field, row, rows, false, _started, acc) do
    parse_chars(rest, [], [to_string(Enum.reverse(field)) | row], rows, false, true, acc)
  end

  defp parse_chars([?\r, ?\n | rest], field, row, rows, false, _started, acc) do
    finish_csv_row(rest, field, row, rows, acc)
  end

  defp parse_chars([?\n | rest], field, row, rows, false, _started, acc) do
    finish_csv_row(rest, field, row, rows, acc)
  end

  defp parse_chars([?\r | rest], field, row, rows, false, _started, acc) do
    finish_csv_row(rest, field, row, rows, acc)
  end

  defp parse_chars([?" | rest], [], row, rows, false, _started, acc) do
    parse_chars(rest, [], row, rows, true, true, acc)
  end

  defp parse_chars([char | rest], field, row, rows, false, _started, acc) do
    parse_chars(rest, [char | field], row, rows, false, true, acc)
  end

  defp finish_csv_row(rest, field, row, rows, acc) do
    completed = Enum.reverse([to_string(Enum.reverse(field)) | row])
    parse_chars(rest, [], [], [completed | rows], false, false, acc)
  end
end
