# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Permitir comunicação entre diferentes processos.
# 4. Diferenciar erros de validação de falhas operacionais.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use GenServer

  def start_link(path, interval \\ 60_000) do
    GenServer.start_link(__MODULE__, {path, interval})
  end

  def run_now(server) do
    GenServer.call(server, :run_now)
  end

  def status(server) do
    GenServer.call(server, :status)
  end

  @impl true
  def init({path, interval}) when is_binary(path) and is_integer(interval) and interval > 0 do
    schedule_tick(interval)

    {:ok,
     %{
       path: path,
       interval: interval,
       worker: nil,
       last_result: nil
     }}
  end

  def init({_path, _interval}), do: {:stop, {:validation, :invalid_path_or_interval}}

  @impl true
  def handle_call(:run_now, _from, state) do
    send(self(), :tick)
    {:reply, :ok, state}
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       worker: worker_pid(state.worker),
       last_result: state.last_result
     }, state}
  end

  @impl true
  def handle_info(:tick, state) do
    schedule_tick(state.interval)

    case state.worker do
      nil ->
        parent = self()
        path = state.path

        {pid, ref} =
          spawn_monitor(fn ->
            result = safely_process_file(path)
            send(parent, {:worker_result, self(), result})
          end)

        {:noreply, %{state | worker: {pid, ref}}}

      _worker ->
        {:noreply, state}
    end
  end

  def handle_info({:worker_result, pid, result}, %{worker: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | worker: nil, last_result: result}}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %{worker: {pid, ref}} = state) do
    result = {:error, {:operational, {:worker_exit, reason}}}
    {:noreply, %{state | worker: nil, last_result: result}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp schedule_tick(interval) do
    Process.send_after(self(), :tick, interval)
  end

  defp worker_pid(nil), do: nil
  defp worker_pid({pid, _ref}), do: pid

  defp safely_process_file(path) do
    try do
      process_file(path)
    rescue
      exception ->
        {:error,
         {:operational, {:exception, exception.__struct__, Exception.message(exception)}}}
    catch
      kind, reason ->
        {:error, {:operational, {:caught, kind, reason}}}
    end
  end

  defp process_file(path) do
    case File.read(path) do
      {:ok, contents} ->
        case parse_csv(contents) do
          {:ok, rows} ->
            {:ok,
             %{
               row_count: length(rows),
               data_row_count: max(length(rows) - 1, 0),
               column_count: case rows do
                 [header | _] -> length(header)
                 [] -> 0
               end
             }}

          {:error, reason} ->
            {:error, {:validation, reason}}
        end

      {:error, reason} ->
        {:error, {:operational, {:file_read, reason}}}
    end
  end

  defp parse_csv(contents) do
    chars = :binary.bin_to_list(contents)

    case parse_chars(chars, :plain, [], [], [], false) do
      {:ok, rows} -> validate_row_widths(rows)
      error -> error
    end
  end

  defp parse_chars([], :quoted, _field, _row, _rows, _touched) do
    {:error, :unterminated_quoted_field}
  end

  defp parse_chars([], _mode, field, row, rows, touched) do
    if touched or field != [] or row != [] do
      {:ok, [Enum.reverse([Enum.reverse(field) | row]) | rows] |> Enum.reverse()}
    else
      {:ok, Enum.reverse(rows)}
    end
  end

  defp parse_chars([?\r, ?\n | rest], mode, field, row, rows, _touched)
       when mode != :quoted do
    parse_chars(rest, :plain, [], [], [Enum.reverse([Enum.reverse(field) | row]) | rows], false)
  end

  defp parse_chars([newline | rest], mode, field, row, rows, _touched)
       when mode != :quoted and newline in [?\r, ?\n] do
    parse_chars(rest, :plain, [], [], [Enum.reverse([Enum.reverse(field) | row]) | rows], false)
  end

  defp parse_chars([?, | rest], :plain, field, row, rows, _touched) do
    parse_chars(rest, :plain, [], [Enum.reverse(field) | row], rows, true)
  end

  defp parse_chars([?, | rest], :quote_end, field, row, rows, _touched) do
    parse_chars(rest, :plain, [], [Enum.reverse(field) | row], rows, true)
  end

  defp parse_chars([?" | rest], :plain, [], row, rows, _touched) do
    parse_chars(rest, :quoted, [], row, rows, true)
  end

  defp parse_chars([?" | rest], :quoted, field, row, rows, touched) do
    parse_chars(rest, :quote_end, field, row, rows, touched)
  end

  defp parse_chars([?" | rest], :quote_end, field, row, rows, touched) do
    parse_chars(rest, :quoted, [?" | field], row, rows, touched)
  end

  defp parse_chars([_char | _rest], :quote_end, _field, _row, _rows, _touched) do
    {:error, :unexpected_character_after_closing_quote}
  end

  defp parse_chars([?" | _rest], :plain, _field, _row, _rows, _touched) do
    {:error, :unexpected_quote}
  end

  defp parse_chars([char | rest], :quoted, field, row, rows, touched) do
    parse_chars(rest, :quoted, [char | field], row, rows, touched)
  end

  defp parse_chars([char | rest], :plain, field, row, rows, _touched) do
    parse_chars(rest, :plain, [char | field], row, rows, true)
  end

  defp validate_row_widths([]), do: {:ok, []}

  defp validate_row_widths([header | data_rows] = rows) do
    expected = length(header)

    case Enum.with_index(data_rows, 2)
         |> Enum.find(fn {row, _line} -> length(row) != expected end) do
      nil ->
        {:ok, rows}

      {row, line} ->
        {:error, {:inconsistent_columns, line, expected, length(row)}}
    end
  end
end
