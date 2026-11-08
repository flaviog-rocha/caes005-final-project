# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Manter um histórico das operações realizadas.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CsvStore do
  @moduledoc """
  Armazenamento persistente de registros com importação CSV incremental e histórico.
  """

  defstruct [:records, :history]

  @type t :: %__MODULE__{records: reference(), history: reference()}

  def open(directory) when is_binary(directory) do
    with :ok <- File.mkdir_p(directory),
         {:ok, records} <-
           :dets.open_file(:undefined,
             file: Path.join(directory, "records.dets") |> String.to_charlist(),
             type: :set
           ),
         {:ok, history} <-
           :dets.open_file(:undefined,
             file: Path.join(directory, "history.dets") |> String.to_charlist(),
             type: :set
           ) do
      {:ok, %__MODULE__{records: records, history: history}}
    else
      {:error, reason} = error ->
        if is_reference(records = Process.get(:csv_store_records)), do: :dets.close(records)
        error

      other ->
        other
    end
  rescue
    error -> {:error, error}
  end

  def close(%__MODULE__{} = store) do
    record_result = :dets.close(store.records)
    history_result = :dets.close(store.history)

    if record_result == :ok and history_result == :ok do
      :ok
    else
      {:error, {record_result, history_result}}
    end
  end

  def insert(%__MODULE__{} = store, id, attributes) when is_map(attributes) do
    case :dets.lookup(store.records, id) do
      [] ->
        persist_record(store, id, attributes, :insert)

      [_] ->
        {:error, :already_exists}
    end
  end

  def get(%__MODULE__{} = store, id) do
    case :dets.lookup(store.records, id) do
      [{^id, attributes}] -> {:ok, attributes}
      [] -> {:error, :not_found}
    end
  end

  def update(%__MODULE__{} = store, id, attributes) when is_map(attributes) do
    case get(store, id) do
      {:ok, existing} -> persist_record(store, id, Map.merge(existing, attributes), :update)
      error -> error
    end
  end

  def delete(%__MODULE__{} = store, id) do
    case get(store, id) do
      {:ok, _attributes} ->
        :ok = :dets.delete(store.records, id)
        :ok = :dets.sync(store.records)
        record_history(store, :delete, id)
        :ok

      error ->
        error
    end
  end

  def each(%__MODULE__{} = store, callback) when is_function(callback, 2) do
    :dets.foldl(fn {id, attributes}, acc -> callback.(id, attributes); acc end, nil, store.records)
    :ok
  end

  def history_each(%__MODULE__{} = store, callback) when is_function(callback, 1) do
    :dets.foldl(
      fn
        {{:event, _sequence}, event}, acc -> callback.(event); acc
        _, acc -> acc
      end,
      nil,
      store.history
    )

    :ok
  end

  def history(%__MODULE__{} = store) do
    events =
      :dets.foldl(
        fn
          {{:event, sequence}, event}, acc -> [{sequence, event} | acc]
          _, acc -> acc
        end,
        [],
        store.history
      )

    events
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  def import_csv(%__MODULE__{} = store, path, options \\ []) do
    delimiter = Keyword.get(options, :delimiter, ?,)
    header? = Keyword.get(options, :header, true)
    id_column = Keyword.get(options, :id_column)

    if not is_integer(delimiter) or delimiter < 0 or delimiter > 255 do
      {:error, :invalid_delimiter}
    else
      parser = %{
        store: store,
        delimiter: delimiter,
        headers: if(header?, do: :pending, else: :generated),
        id_column: id_column,
        field: [],
        fields: [],
        mode: :plain,
        touched: false,
        count: 0,
        error: nil
      }

      try do
        parser =
          path
          |> File.stream!([], 64 * 1024)
          |> Enum.reduce_while(parser, fn chunk, current ->
            next = feed(current, chunk)
            if next.error, do: {:halt, next}, else: {:cont, next}
          end)

        parser = if parser.error, do: parser, else: finish(parser)

        case parser.error do
          nil -> {:ok, parser.count}
          reason -> {:error, reason, parser.count}
        end
      rescue
        error -> {:error, error}
      end
    end
  end

  defp persist_record(store, id, attributes, action) do
    :ok = :dets.insert(store.records, {id, attributes})
    :ok = :dets.sync(store.records)
    record_history(store, action, id)
    {:ok, attributes}
  end

  defp record_history(store, action, id) do
    sequence =
      case :dets.lookup(store.history, :counter) do
        [{:counter, value}] -> value + 1
        [] -> 1
      end

    event = %{
      sequence: sequence,
      action: action,
      id: id,
      timestamp: DateTime.utc_now()
    }

    :ok = :dets.insert(store.history, [{:counter, sequence}, {{:event, sequence}, event}])
    :ok = :dets.sync(store.history)
    :ok
  end

  defp feed(parser, <<>>), do: parser

  defp feed(%{error: error} = parser, _bytes) when not is_nil(error), do: parser

  defp feed(parser, <<byte, rest::binary>>) do
    parser
    |> step(byte)
    |> feed(rest)
  end

  defp step(%{mode: :skip_lf} = parser, ?\n), do: %{parser | mode: :plain}

  defp step(%{mode: :skip_lf} = parser, byte) do
    parser
    |> Map.put(:mode, :plain)
    |> step(byte)
  end

  defp step(%{mode: :quoted} = parser, ?"), do: %{parser | mode: :quote_end}

  defp step(%{mode: :quoted} = parser, byte), do: append_byte(parser, byte)

  defp step(%{mode: :quote_end} = parser, ?") do
    parser
    |> append_byte(?")
    |> Map.put(:mode, :quoted)
  end

  defp step(%{mode: :quote_end} = parser, byte) when byte == ?,, do: end_field(parser)

  defp step(%{mode: :quote_end} = parser, ?\n), do: end_row(parser)

  defp step(%{mode: :quote_end} = parser, ?\r) do
    parser |> end_row() |> Map.put(:mode, :skip_lf)
  end

  defp step(%{mode: :quote_end} = parser, byte) do
    parser
    |> append_byte(?")
    |> append_byte(byte)
    |> Map.put(:mode, :plain)
  end

  defp step(%{mode: :plain, delimiter: delimiter} = parser, delimiter), do: end_field(parser)

  defp step(%{mode: :plain} = parser, ?\n), do: end_row(parser)

  defp step(%{mode: :plain} = parser, ?\r) do
    parser |> end_row() |> Map.put(:mode, :skip_lf)
  end

  defp step(%{mode: :plain, field: []} = parser, ?") do
    %{parser | mode: :quoted, touched: true}
  end

  defp step(%{mode: :plain} = parser, byte), do: append_byte(parser, byte)

  defp append_byte(parser, byte) do
    %{parser | field: [byte | parser.field], touched: true}
  end

  defp end_field(parser) do
    field = parser.field |> Enum.reverse() |> :erlang.list_to_binary()
    %{parser | field: [], fields: [field | parser.fields], touched: true}
  end

  defp end_row(parser) do
    row =
      [parser.field |> Enum.reverse() |> :erlang.list_to_binary() | parser.fields]
      |> Enum.reverse()

    parser
    |> Map.put(:field, [])
    |> Map.put(:fields, [])
    |> Map.put(:mode, :plain)
    |> Map.put(:touched, false)
    |> consume_row(row)
  end

  defp finish(%{mode: :quoted} = parser), do: %{parser | error: :unterminated_quoted_field}
  defp finish(%{touched: false} = parser), do: parser

  defp finish(parser) do
    row =
      [parser.field |> Enum.reverse() |> :erlang.list_to_binary() | parser.fields]
      |> Enum.reverse()

    parser
    |> Map.put(:field, [])
    |> Map.put(:fields, [])
    |> Map.put(:touched, false)
    |> consume_row(row)
  end

  defp consume_row(%{headers: :pending} = parser, headers) do
    if Enum.any?(headers, &(&1 == "")) do
      %{parser | error: :empty_header}
    else
      %{parser | headers: headers}
    end
  end

  defp consume_row(%{headers: :generated} = parser, row) do
    headers = Enum.map(0..(length(row) - 1), &Integer.to_string/1)
    consume_data_row(%{parser | headers: headers}, row)
  end

  defp consume_row(parser, row), do: consume_data_row(parser, row)

  defp consume_data_row(parser, row) do
    if length(row) != length(parser.headers) do
      %{parser | error: {:column_count_mismatch, length(parser.headers), length(row)}}
    else
      attributes = parser.headers |> Enum.zip(row) |> Map.new()
      id_column = parser.id_column || hd(parser.headers)

      case Map.fetch(attributes, id_column) do
        :error ->
          %{parser | error: {:id_column_not_found, id_column}}

        {:ok, id} ->
          result =
            case get(parser.store, id) do
              {:ok, _existing} -> update(parser.store, id, attributes)
              {:error, :not_found} -> insert(parser.store, id, attributes)
            end

          case result do
            {:ok, _} -> %{parser | count: parser.count + 1}
            error -> %{parser | error: {:record_error, id, error}}
          end
      end
    end
  end
end
