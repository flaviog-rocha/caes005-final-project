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

defmodule CSVProcessor.CSV do
  @moduledoc false

  def parse(binary) when is_binary(binary) do
    scan(String.to_charlist(binary), [], [], [], false, false)
  end

  defp scan([], _field, _row, _records, true, _started),
    do: {:error, :unterminated_quoted_field}

  defp scan([], field, row, records, false, true) do
    record = Enum.reverse([Enum.reverse(field) |> to_string() | row])
    {:ok, Enum.reverse([record | records])}
  end

  defp scan([], _field, _row, records, false, false),
    do: {:ok, Enum.reverse(records)}

  defp scan([?", ?" | rest], field, row, records, true, started),
    do: scan(rest, [?" | field], row, records, true, started)

  defp scan([?" | rest], field, row, records, true, started),
    do: scan(rest, field, row, records, false, started)

  defp scan([char | rest], field, row, records, true, started),
    do: scan(rest, [char | field], row, records, true, started)

  defp scan([?\r, ?\n | rest], field, row, records, false, _started) do
    finish_record(rest, field, row, records)
  end

  defp scan([?\n | rest], field, row, records, false, _started) do
    finish_record(rest, field, row, records)
  end

  defp scan([?, | rest], field, row, records, false, _started),
    do: scan(rest, [], [Enum.reverse(field) |> to_string() | row], records, false, true)

  defp scan([?" | rest], [], row, records, false, _started),
    do: scan(rest, [], row, records, true, true)

  defp scan([?" | _rest], _field, _row, _records, false, _started),
    do: {:error, :quote_in_unquoted_field}

  defp scan([char | rest], field, row, records, false, _started),
    do: scan(rest, [char | field], row, records, false, true)

  defp finish_record(rest, field, row, records) do
    record = Enum.reverse([Enum.reverse(field) |> to_string() | row])
    scan(rest, [], [], [record | records], false, false)
  end
end

defmodule CSVProcessor.FakeDB do
  @moduledoc false
  use GenServer

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def commit(server, rows, id_field) do
    GenServer.call(server, {:commit, rows, id_field}, :infinity)
  end

  def all(server) do
    GenServer.call(server, :all)
  end

  @impl true
  def init(opts) do
    {:ok, %{rows: %{}, fail_next: Keyword.get(opts, :fail_next, 0)}}
  end

  @impl true
  def handle_call(:all, _from, state), do: {:reply, state.rows, state}

  def handle_call({:commit, rows, id_field}, _from, state) do
    cond do
      state.fail_next > 0 ->
        {:reply, {:error, {:temporary, :injected_failure}},
         %{state | fail_next: state.fail_next - 1}}

      invalid_row = Enum.find(rows, &invalid_row?(&1, id_field)) ->
        {:reply, {:error, {:invalid_row, invalid_row}}, state}

      true ->
        # A única alteração de estado ocorre depois da validação do lote inteiro.
        updated =
          Enum.reduce(rows, state.rows, fn row, acc ->
            Map.put(acc, Map.fetch!(row, id_field), row)
          end)

        {:reply, {:ok, length(rows)}, %{state | rows: updated}}
    end
  end

  defp invalid_row?(row, id_field) do
    not is_map(row) or not Map.has_key?(row, id_field) or
      Map.get(row, id_field) in [nil, ""]
  end
end

defmodule CSVProcessor do
  @moduledoc """
  Processa arquivos CSV em lotes atômicos usando `CSVProcessor.FakeDB`.
  """

  require Logger

  def process_file(path, db, opts \\ []) do
    id_field = Keyword.get(opts, :id_field, "id")

    with {:ok, contents} <- File.read(path),
         {:ok, records} <- CSVProcessor.CSV.parse(contents),
         {:ok, rows} <- records_to_maps(records, id_field),
         {:ok, count} <- commit_with_retries(db, rows, id_field, opts) do
      Logger.info("CSV processado: #{path} (#{count} registros)")
      {:ok, %{path: path, rows: count}}
    else
      {:error, reason} = error ->
        Logger.error("Falha ao processar CSV #{path}: #{inspect(reason)}")
        error
    end
  end

  def process_files(paths, db, opts \\ []) do
    max_concurrency =
      opts
      |> Keyword.get(:max_concurrency, System.schedulers_online())
      |> max(1)

    paths
    |> Task.async_stream(
      fn path -> process_file(path, db, opts) end,
      max_concurrency: max_concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
  end

  defp records_to_maps([], _id_field), do: {:error, :empty_csv}

  defp records_to_maps([headers | records], id_field) do
    cond do
      headers == [] or Enum.any?(headers, &(&1 == "")) ->
        {:error, :invalid_headers}

      length(Enum.uniq(headers)) != length(headers) ->
        {:error, :duplicate_headers}

      id_field not in headers ->
        {:error, {:missing_id_column, id_field}}

      true ->
        records
        |> Enum.with_index(2)
        |> Enum.reduce_while({:ok, []}, fn {record, line}, {:ok, acc} ->
          if length(record) != length(headers) do
            {:halt, {:error, {:invalid_column_count, line}}}
          else
            row = headers |> Enum.zip(record) |> Map.new()

            if Map.get(row, id_field) in [nil, ""] do
              {:halt, {:error, {:missing_id, line}}}
            else
              {:cont, {:ok, [row | acc]}}
            end
          end
        end)
        |> case do
          {:ok, rows} -> {:ok, Enum.reverse(rows)}
          error -> error
        end
    end
  end

  defp commit_with_retries(db, rows, id_field, opts) do
    retries = max(Keyword.get(opts, :max_retries, 3), 0)
    delay = max(Keyword.get(opts, :retry_delay, 100), 0)
    max_delay = max(Keyword.get(opts, :max_retry_delay, 5_000), 0)

    do_commit(db, rows, id_field, retries, delay, max_delay)
  end

  defp do_commit(db, rows, id_field, retries_left, delay, max_delay) do
    case CSVProcessor.FakeDB.commit(db, rows, id_field) do
      {:ok, count} ->
        {:ok, count}

      {:error, {:temporary, reason}} when retries_left > 0 ->
        Logger.warning(
          "Falha temporária no banco (#{inspect(reason)}); " <>
            "nova tentativa em #{delay} ms"
        )

        Process.sleep(delay)
        do_commit(db, rows, id_field, retries_left - 1, min(delay * 2, max_delay), max_delay)

      {:error, {:temporary, reason}} ->
        {:error, {:retries_exhausted, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
