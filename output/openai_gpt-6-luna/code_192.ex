# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use GenServer

  @moduledoc """
  Processes CSV files using a durable, recoverable transaction journal.

  Start with a persistent state file:

      {:ok, pid} = CsvProcessor.start_link(state_file: "data/csv_processor.state")

  Process a CSV file, optionally transforming its rows:

      CsvProcessor.process(pid, "input.csv", "output.csv")
      CsvProcessor.process(pid, "input.csv", "output.csv", fn rows -> rows end)

  Operations are serialized per server. A pending transaction is completed during
  startup or before the next processing request.
  """

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def process(server, input_path, output_path) do
    process(server, input_path, output_path, &Function.identity/1)
  end

  def process(server, input_path, output_path, transform) when is_function(transform, 1) do
    GenServer.call(server, {:process, input_path, output_path, transform}, :infinity)
  end

  def history(server) do
    GenServer.call(server, :history)
  end

  @impl true
  def init(opts) do
    with {:ok, state_file} <- Keyword.fetch(opts, :state_file),
         true <- is_binary(state_file) and state_file != "",
         state_file <- Path.expand(state_file),
         {:ok, state} <- load_state(state_file),
         {:ok, state} <- recover_pending(state) do
      {:ok, Map.put(state, :state_file, state_file)}
    else
      :error -> {:stop, :state_file_required}
      false -> {:stop, :invalid_state_file}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  def handle_call({:process, input_path, output_path, transform}, _from, state) do
    case recover_pending(state) do
      {:ok, recovered_state} ->
        case run_operation(input_path, output_path, transform, recovered_state) do
          {:ok, entry, new_state} ->
            {:reply, {:ok, entry}, new_state}

          {:error, reason, new_state} ->
            {:reply, {:error, reason}, new_state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp load_state(state_file) do
    case File.read(state_file) do
      {:ok, contents} ->
        try do
          state = :erlang.binary_to_term(contents, [:safe])

          if is_map(state) and is_list(Map.get(state, :history)) do
            {:ok, Map.put_new(state, :pending, nil)}
          else
            {:error, :invalid_state}
          end
        rescue
          _ -> {:error, :invalid_state}
        end

      {:error, :enoent} ->
        state = %{history: [], pending: nil}

        case persist_state(state_file, state) do
          :ok -> {:ok, state}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_operation(input_path, output_path, transform, state) do
    input_path = Path.expand(input_path)
    output_path = Path.expand(output_path)

    cond do
      output_path == state.state_file ->
        {:error, :output_cannot_be_state_file, state}

      true ->
        do_run_operation(input_path, output_path, transform, state)
    end
  end

  defp do_run_operation(input_path, output_path, transform, state) do
    with {:ok, csv} <- File.read(input_path),
         {:ok, rows} <- parse_csv(csv),
         transformed <- transform.(rows),
         :ok <- validate_rows(transformed),
         output <- encode_csv(transformed),
         :ok <- File.mkdir_p(Path.dirname(output_path)),
         temp_path <- unique_path(output_path),
         :ok <- write_synced(temp_path, output) do
      id = unique_id()

      entry = %{
        id: id,
        input: input_path,
        output: output_path,
        rows_read: length(rows),
        rows_written: length(transformed),
        completed_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      pending = %{
        entry: entry,
        temp_path: temp_path,
        output_path: output_path,
        digest: :crypto.hash(:sha256, output)
      }

      pending_state = %{state | pending: pending}

      case persist_state(state.state_file, pending_state) do
        :ok ->
          case recover_pending(pending_state) do
            {:ok, committed_state} -> {:ok, entry, committed_state}
            {:error, reason} -> {:error, reason, pending_state}
          end

        {:error, reason} ->
          File.rm(temp_path)
          {:error, reason, state}
      end
    else
      {:error, reason} ->
        {:error, reason, state}

      other ->
        {:error, {:invalid_transform_result, other}, state}
    end
  rescue
    error ->
      {:error, {:processing_failed, Exception.message(error)}, state}
  catch
    kind, reason ->
      {:error, {:processing_failed, kind, reason}, state}
  end

  defp recover_pending(%{pending: nil} = state), do: {:ok, state}

  defp recover_pending(%{pending: pending} = state) do
    temp_path = pending.temp_path
    output_path = pending.output_path
    digest = pending.digest

    result =
      case File.read(temp_path) do
        {:ok, contents} ->
          if :crypto.hash(:sha256, contents) == digest do
            with :ok <- File.mkdir_p(Path.dirname(output_path)),
                 :ok <- File.rename(temp_path, output_path) do
              :ok
            end
          else
            {:error, :transaction_temp_corrupt}
          end

        {:error, :enoent} ->
          case File.read(output_path) do
            {:ok, contents} ->
              if :crypto.hash(:sha256, contents) == digest do
                :ok
              else
                {:error, :transaction_output_mismatch}
              end

            {:error, reason} ->
              {:error, {:transaction_files_missing, reason}}
          end

        {:error, reason} ->
          {:error, reason}
      end

    case result do
      :ok ->
        committed_state = %{
          state
          | history: [pending.entry | state.history],
            pending: nil
        }

        case persist_state(state.state_file, committed_state) do
          :ok -> {:ok, committed_state}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  rescue
    error -> {:error, {:recovery_failed, Exception.message(error)}}
  end

  defp persist_state(state_file, state) do
    with :ok <- File.mkdir_p(Path.dirname(state_file)),
         temp_path <- unique_path(state_file),
         data <- :erlang.term_to_binary(Map.drop(state, [:state_file]), [:compressed]),
         :ok <- write_synced(temp_path, data),
         :ok <- File.rename(temp_path, state_file) do
      :ok
    else
      {:error, reason} = error ->
        error

      other ->
        {:error, other}
    end
  end

  defp write_synced(path, data) do
    with {:ok, file} <- File.open(path, [:write, :binary]) do
      try do
        with :ok <- IO.binwrite(file, data),
             :ok <- :file.sync(file) do
          :ok
        end
      after
        File.close(file)
      end
    end
  end

  defp unique_path(path) do
    path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  defp unique_id do
    Integer.to_string(System.system_time(:microsecond)) <>
      "-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  defp validate_rows(rows) when is_list(rows) do
    if Enum.all?(rows, fn row ->
         is_list(row) and Enum.all?(row, &csv_value?/1)
       end) do
      :ok
    else
      {:error, :invalid_transform_result}
    end
  end

  defp validate_rows(_), do: {:error, :invalid_transform_result}

  defp csv_value?(value) do
    is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or is_nil(value)
  end

  defp encode_csv(rows) do
    rows
    |> Enum.map_join("\r\n", fn row ->
      Enum.map_join(row, ",", &encode_field/1)
    end)
    |> append_final_newline(rows)
  end

  defp append_final_newline(csv, []), do: csv
  defp append_final_newline(csv, _rows), do: csv <> "\r\n"

  defp encode_field(value) do
    field = if is_nil(value), do: "", else: to_string(value)

    if String.contains?(field, [",", "\"", "\r", "\n"]) do
      "\"" <> String.replace(field, "\"", "\"\"") <> "\""
    else
      field
    end
  end

  defp parse_csv(csv) when is_binary(csv) do
    parse_csv_bytes(csv, [], [], [], false, false, false)
  end

  defp parse_csv_bytes(<<>>, rows, row, field, quoted, closed, touched) do
    cond do
      quoted ->
        {:error, :malformed_csv}

      touched or row != [] or field != [] or closed ->
        final_row = Enum.reverse([field_value(field) | row])
        {:ok, Enum.reverse([final_row | rows])}

      true ->
        {:ok, Enum.reverse(rows)}
    end
  end

  defp parse_csv_bytes(<<34, 34, rest::binary>>, rows, row, field, true, _closed, _touched) do
    parse_csv_bytes(rest, rows, row, [34 | field], true, false, true)
  end

  defp parse_csv_bytes(<<34, rest::binary>>, rows, row, field, true, _closed, touched) do
    parse_csv_bytes(rest, rows, row, field, false, true, touched)
  end

  defp parse_csv_bytes(<<byte, rest::binary>>, rows, row, field, true, _closed, _touched) do
    parse_csv_bytes(rest, rows, row, [byte | field], true, false, true)
  end

  defp parse_csv_bytes(<<44, rest::binary>>, rows, row, field, false, _closed, _touched) do
    parse_csv_bytes(rest, rows, [field_value(field) | row], [], false, false, true)
  end

  defp parse_csv_bytes(<<13, 10, rest::binary>>, rows, row, field, false, _closed, _touched) do
    next_row = Enum.reverse([field_value(field) | row])
    parse_csv_bytes(rest, [next_row | rows], [], [], false, false, false)
  end

  defp parse_csv_bytes(<<10, rest::binary>>, rows, row, field, false, _closed, _touched) do
    next_row = Enum.reverse([field_value(field) | row])
    parse_csv_bytes(rest, [next_row | rows], [], [], false, false, false)
  end

  defp parse_csv_bytes(<<13, rest::binary>>, rows, row, field, false, _closed, _touched) do
    next_row = Enum.reverse([field_value(field) | row])
    parse_csv_bytes(rest, [next_row | rows], [], [], false, false, false)
  end

  defp parse_csv_bytes(<<34, rest::binary>>, rows, [], [], false, _closed, _touched) do
    parse_csv_bytes(rest, rows, [], [], true, false, true)
  end

  defp parse_csv_bytes(<<34, _rest::binary>>, _rows, _row, _field, false, _closed, _touched) do
    {:error, :malformed_csv}
  end

  defp parse_csv_bytes(<<_byte, _rest::binary>>, _rows, _row, _field, false, true, _touched) do
    {:error, :malformed_csv}
  end

  defp parse_csv_bytes(<<byte, rest::binary>>, rows, row, field, false, false, _touched) do
    parse_csv_bytes(rest, rows, row, [byte | field], false, false, true)
  end

  defp field_value(field), do: field |> Enum.reverse() |> :erlang.list_to_binary()
end
