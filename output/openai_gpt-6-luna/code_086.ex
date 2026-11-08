# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvImporter.Store do
  @moduledoc false

  use Agent

  def start_link(opts \\ []) do
    Agent.start_link(fn ->
      %{
        records: [],
        failures_remaining: Keyword.get(opts, :failures_remaining, 0)
      }
    end)
  end

  def records(store), do: Agent.get(store, & &1.records)

  def commit(store, records) do
    Agent.get_and_update(store, fn
      %{failures_remaining: remaining} = state when remaining > 0 ->
        {{:error, {:transient, :simulated_failure}},
         %{state | failures_remaining: remaining - 1}}

      state ->
        {{:ok, length(records)}, %{state | records: state.records ++ records}}
    end)
  end
end

defmodule CsvImporter do
  @moduledoc false
  require Logger

  def import_file(path, store, opts \\ []) do
    with {:ok, contents} <- File.read(path),
         {:ok, rows} <- parse_csv(contents),
         {:ok, records} <- build_records(rows),
         {:ok, processed} <- process_records(records, opts),
         {:ok, count} <- commit_with_retry(store, processed, opts) do
      Logger.info("CSV import completed: #{count} records from #{path}")
      {:ok, count}
    else
      {:error, reason} = error ->
        Logger.error("CSV import failed for #{path}: #{inspect(reason)}")
        error
    end
  end

  defp build_records([]), do: {:error, :empty_csv}

  defp build_records([headers | rows]) do
    if Enum.any?(headers, &(&1 == "")) or
         length(Enum.uniq(headers)) != length(headers) do
      {:error, :invalid_headers}
    else
      rows
      |> Enum.with_index(2)
      |> Enum.reduce_while({:ok, []}, fn {row, line}, {:ok, acc} ->
        if length(row) == length(headers) do
          record = headers |> Enum.zip(row) |> Map.new()
          {:cont, {:ok, [record | acc]}}
        else
          {:halt, {:error, {:invalid_row_length, line}}}
        end
      end)
      |> case do
        {:ok, records} -> {:ok, Enum.reverse(records)}
        error -> error
      end
    end
  end

  defp process_records(records, opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    transform = Keyword.get(opts, :transform, & &1)

    if not is_integer(max_concurrency) or max_concurrency < 1 do
      {:error, :invalid_max_concurrency}
    else
      records
      |> Task.async_stream(
        fn record ->
          case transform.(record) do
            {:ok, value} -> {:ok, value}
            {:error, reason} -> {:error, reason}
            value -> {:ok, value}
          end
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, record}}, {:ok, acc} ->
          {:cont, {:ok, [record | acc]}}

        {:ok, {:error, reason}}, _acc ->
          {:halt, {:error, {:transform_failed, reason}}}

        {:exit, reason}, _acc ->
          {:halt, {:error, {:transform_task_failed, reason}}}
      end)
      |> case do
        {:ok, processed} -> {:ok, Enum.reverse(processed)}
        error -> error
      end
    end
  end

  defp commit_with_retry(store, records, opts) do
    retries = Keyword.get(opts, :max_retries, 3)
    backoff_ms = Keyword.get(opts, :backoff_ms, 50)

    if not is_integer(retries) or retries < 0 or
         not is_integer(backoff_ms) or backoff_ms < 0 do
      {:error, :invalid_retry_options}
    else
      do_commit(store, records, retries, backoff_ms)
    end
  end

  defp do_commit(store, records, retries_left, backoff_ms) do
    case CsvImporter.Store.commit(store, records) do
      {:ok, count} ->
        {:ok, count}

      {:error, {:transient, reason}} when retries_left > 0 ->
        Logger.warning(
          "Transient commit failure; retrying (#{retries_left} retries left): #{inspect(reason)}"
        )

        if backoff_ms > 0, do: Process.sleep(backoff_ms)
        do_commit(store, records, retries_left - 1, backoff_ms)

      {:error, {:transient, reason}} ->
        {:error, {:retries_exhausted, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_csv(contents) do
    contents
    |> String.replace("\r\n", "\n")
    |> String.replace("\r", "\n")
    |> String.to_charlist()
    |> parse_chars([], [], [], false)
  end

  defp parse_chars([], _fields, _field, _rows, true),
    do: {:error, :unterminated_quoted_field}

  defp parse_chars([], fields, field, rows, false) do
    if fields == [] and field == [] do
      {:ok, Enum.reverse(rows)}
    else
      row = finish_field(fields, field)
      {:ok, Enum.reverse([row | rows])}
    end
  end

  defp parse_chars([?", ?" | rest], fields, field, rows, true),
    do: parse_chars(rest, fields, [?" | field], rows, true)

  defp parse_chars([?" | rest], fields, field, rows, quoted) when quoted,
    do: parse_chars(rest, fields, field, rows, false)

  defp parse_chars([?" | rest], fields, [], rows, false),
    do: parse_chars(rest, fields, [], rows, true)

  defp parse_chars([?, | rest], fields, field, rows, false) do
    parse_chars(rest, [field_to_string(field) | fields], [], rows, false)
  end

  defp parse_chars([?\n | rest], fields, field, rows, false) do
    row = finish_field(fields, field)
    parse_chars(rest, [], [], [row | rows], false)
  end

  defp parse_chars([?" | _rest], _fields, _field, _rows, false),
    do: {:error, :quote_inside_unquoted_field}

  defp parse_chars([char | rest], fields, field, rows, quoted) do
    parse_chars(rest, fields, [char | field], rows, quoted)
  end

  defp finish_field(fields, field),
    do: Enum.reverse([field_to_string(field) | fields])

  defp field_to_string(field),
    do: field |> Enum.reverse() |> List.to_string()
end You must verify the output against the user’s instructions and the question. The user asked for a single self-contained Elixir file, only code/comments, using Logger, CSV processing, atomic updates, retries on temporary failures, concurrency limit, optional fake DB/Ecto. Output does this mostly. Need detect compile issue char pattern `defp parse_chars([?", ?" | rest]...` Elixir char syntax for quote: `?"` yes literal " char. List syntax `[?", ?" | rest]` parsed? In Elixir char literal `?"` ends after quote, so yes `?",` etc. Also `parse_chars` errors return `{:error,...}` while parser calls okay.

Critical CSV quote escaped logic: when quoted quote followed doubled handles; on closing quote, toggles. Fine.

Atomic commit records append all in one Agent call. But transaction failure decrement internal failures state only, no records mutation. Good.

Potential compile issue Agent.start_link opts? call Agent.start_link(fun) valid. Good.

`String.to_charlist` and `List.to_string` Unicode okay.

CSV parsing of blank file empty rows returns []; parse `"\n"` yields one [""] -> invalid header. fine.

Task async stream reduce behavior: if first error, Enum.reduce_while halts. Task.async_stream stream may have spawned tasks with side effect in transform, but no commit yet. Partial updates concern destination atomic, external transform side effects not covered. Fine.

But no validation transform function type; raises in process_records (Keyword transform nonfn causing error at async task -> exit and returns error). Fine.

Could mention no extra. Output only code. We need likely flag Elixir syntax at pattern `defp parse_chars([?", ?" | rest],...` let's reason parser: char literal can be `?"` syntax is `?"` where opening ? then literal double quote. In source `?",` is `?"` then comma. `[?", ?" | rest]` means first char quoted double, second too. valid.

Issue `parse_chars` `parse_csv(contents)` when contents ends newline, rows reversed and no extra final row correct. If file contents empty, build error.

Atomicity in in-memory Agent store only. Good.

But user requested a unique self-contained file; two modules same code file yes.

Potential API `Logger.warning/1` available Elixir version (Logger.warning introduced? likely yes). Fine.

One bug: `Agent.get_and_update` callback second return state new. In failure branch state map uses `%{state | failures_remaining: remaining - 1}` valid. Success records concat.

One missing handling CsvImporter.Store.commit Agent down exit uncaught, not requirement perhaps errors are temporary? retries only explicit transient. Fine.

Need final only output code was already emitted. No need revise? User asks code; output was code only. We should ensure no issue with quote handling pattern syntax. One notable Elixir compile: function clause `defp parse_chars([?", ?" | rest], fields, field, rows, true),` valid char literal syntax likely lexical: `?"` represents double quote. In Elixir docs char literals `?\n`, `?a`, `?"` yes.

One concern `System.schedulers_online()` may return 0? no.

The conditional `if backoff_ms > 0, do: Process.sleep(backoff_ms)` returns nil if false okay.

Atomic prevention partially updates: staged all rows and commit final. good.

We should not add.
