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
  use GenServer

  @records_table :csv_store_records
  @history_table :csv_store_history

  def start_link(path) when is_binary(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def stop do
    GenServer.stop(__MODULE__)
  end

  def create(data) when is_map(data) do
    GenServer.call(__MODULE__, {:create, data})
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get, id})
  end

  def update(id, changes) when is_map(changes) do
    GenServer.call(__MODULE__, {:update, id, changes})
  end

  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id})
  end

  def list do
    GenServer.call(__MODULE__, :list)
  end

  def history do
    GenServer.call(__MODULE__, :history)
  end

  def import_csv(path, opts \\ []) when is_binary(path) do
    GenServer.call(__MODULE__, {:import_csv, path, opts}, :infinity)
  end

  @impl true
  def init(path) do
    with {:ok, _} <-
           :dets.open_file(@records_table,
             file: String.to_charlist(path <> ".records"),
             type: :set
           ),
         {:ok, _} <-
           :dets.open_file(@history_table,
             file: String.to_charlist(path <> ".history"),
             type: :set
           ) do
      unless :dets.lookup(@records_table, :next_id) != [] do
        :dets.insert(@records_table, {:next_id, 1})
      end

      unless :dets.lookup(@history_table, :next_event) != [] do
        :dets.insert(@history_table, {:next_event, 1})
      end

      {:ok, %{}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, data}, _from, state) do
    {id, state} = insert_record(data, :create, state)
    {:reply, {:ok, id}, state}
  end

  def handle_call({:get, id}, _from, state) do
    result =
      case :dets.lookup(@records_table, {:record, id}) do
        [{{:record, ^id}, data}] -> {:ok, data}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:update, id, changes}, _from, state) do
    case :dets.lookup(@records_table, {:record, id}) do
      [{{:record, ^id}, data}] ->
        updated = Map.merge(data, changes)
        :dets.insert(@records_table, {{:record, id}, updated})
        state = log_event(:update, id, state)
        :dets.sync(@records_table)
        :dets.sync(@history_table)
        {:reply, {:ok, updated}, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case :dets.lookup(@records_table, {:record, id}) do
      [{{:record, ^id}, _data}] ->
        :dets.delete(@records_table, {:record, id})
        state = log_event(:delete, id, state)
        :dets.sync(@records_table)
        :dets.sync(@history_table)
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    records =
      :dets.foldl(
        fn
          {{:record, id}, data}, acc -> [{id, data} | acc]
          _, acc -> acc
        end,
        [],
        @records_table
      )
      |> Enum.sort_by(&elem(&1, 0))

    {:reply, records, state}
  end

  def handle_call(:history, _from, state) do
    events =
      :dets.foldl(
        fn
          {{:event, id}, event}, acc -> [{id, event} | acc]
          _, acc -> acc
        end,
        [],
        @history_table
      )
      |> Enum.sort_by(&elem(&1, 0))

    {:reply, events, state}
  end

  def handle_call({:import_csv, path, opts}, _from, state) do
    case import_file(path, Keyword.get(opts, :headers, true), state) do
      {:ok, count, state} -> {:reply, {:ok, count}, state}
      {:error, reason, count, state} -> {:reply, {:error, reason, count}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@records_table)
    :dets.close(@history_table)
    :ok
  end

  defp insert_record(data, action, state) do
    [{:next_id, id}] = :dets.lookup(@records_table, :next_id)
    :dets.insert(@records_table, [{:next_id, id + 1}, {{:record, id}, data}])
    state = log_event(action, id, state)
    :dets.sync(@records_table)
    :dets.sync(@history_table)
    {id, state}
  end

  defp log_event(action, id, state) do
    [{:next_event, event_id}] = :dets.lookup(@history_table, :next_event)

    event = %{
      action: action,
      record_id: id,
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    :dets.insert(@history_table, [
      {:next_event, event_id + 1},
      {{:event, event_id}, event}
    ])

    state
  end

  defp import_file(path, headers?, state) do
    case File.open(path, [:read, :binary]) do
      {:ok, file} ->
        result =
          try do
            import_chunks(file, initial_parser_state(), %{headers?: headers?, headers: nil, count: 0}, state)
          after
            File.close(file)
          end

        result

      {:error, reason} ->
        {:error, reason, 0, state}
    end
  end

  defp import_chunks(file, parser, import_state, store_state) do
    case IO.binread(file, 65_536) do
      {:error, reason} ->
        {:error, reason, import_state.count, store_state}

      :eof ->
        with {:ok, parser, rows} <- finish_parser(parser),
             {:ok, import_state, store_state} <-
               consume_rows(rows, import_state, store_state) do
          {:ok, import_state.count, store_state}
        else
          {:error, reason, import_state, store_state} ->
            {:error, reason, import_state.count, store_state}

          {:error, reason} ->
            {:error, reason, import_state.count, store_state}
        end

      chunk when is_binary(chunk) ->
        case parse_chunk(chunk, parser) do
          {:ok, parser, rows} ->
            case consume_rows(rows, import_state, store_state) do
              {:ok, import_state, store_state} ->
                import_chunks(file, parser, import_state, store_state)

              {:error, reason, import_state, store_state} ->
                {:error, reason, import_state.count, store_state}
            end

          {:error, reason} ->
            {:error, reason, import_state.count, store_state}
        end
    end
  end

  defp consume_rows(rows, import_state, store_state) do
    Enum.reduce_while(rows, {:ok, import_state, store_state}, fn row,
                                                                 {:ok, import_state, store_state} ->
      cond do
        import_state.headers? and is_nil(import_state.headers) ->
          {:cont, {:ok, %{import_state | headers: row}, store_state}}

        true ->
          data = row_to_map(row, import_state.headers)
          {_id, store_state} = insert_record(data, :import, store_state)
          {:cont, {:ok, %{import_state | count: import_state.count + 1}, store_state}}
      end
    end)
  end

  defp row_to_map(row, nil) do
    row
    |> Enum.with_index(1)
    |> Map.new(fn {value, index} -> {"column_#{index}", value} end)
  end

  defp row_to_map(row, headers) do
    headers
    |> Enum.zip(row)
    |> Map.new()
  end

  defp initial_parser_state do
    %{fields: [], field: [], quoted: false, after_quote: false, touched: false}
  end

  defp parse_chunk(binary, parser), do: parse_bytes(binary, parser, [])

  defp parse_bytes(<<>>, parser, rows), do: {:ok, parser, Enum.reverse(rows)}

  defp parse_bytes(<<byte, rest::binary>>, %{after_quote: true} = parser, rows) do
    cond do
      byte == ?" ->
        parser = %{parser | field: [?" | parser.field], quoted: true, after_quote: false}
        parse_bytes(rest, parser, rows)

      byte == ?, ->
        parser = parser |> finish_field() |> Map.merge(%{after_quote: false, touched: true})
        parse_bytes(rest, parser, rows)

      byte == 10 ->
        {row, parser} = finish_row(%{parser | after_quote: false, touched: true})
        parse_bytes(rest, parser, [row | rows])

      byte == 13 ->
        parse_bytes(rest, parser, rows)

      true ->
        {:error, :invalid_csv}
    end
  end

  defp parse_bytes(<<byte, rest::binary>>, %{quoted: true} = parser, rows) do
    if byte == ?" do
      parse_bytes(rest, %{parser | quoted: false, after_quote: true}, rows)
    else
      parse_bytes(rest, %{parser | field: [byte | parser.field], touched: true}, rows)
    end
  end

  defp parse_bytes(<<byte, rest::binary>>, parser, rows) do
    cond do
      byte == ?, ->
        parser = parser |> finish_field() |> Map.put(:touched, true)
        parse_bytes(rest, parser, rows)

      byte == 10 ->
        {row, parser} = finish_row(%{parser | touched: true})
        parse_bytes(rest, parser, [row | rows])

      byte == 13 ->
        parse_bytes(rest, %{parser | touched: true}, rows)

      byte == ?" and parser.field == [] ->
        parse_bytes(rest, %{parser | quoted: true, touched: true}, rows)

      byte == ?" ->
        {:error, :invalid_csv}

      true ->
        parse_bytes(rest, %{parser | field: [byte | parser.field], touched: true}, rows)
    end
  end

  defp finish_field(parser) do
    value = parser.field |> Enum.reverse() |> IO.iodata_to_binary()
    %{parser | fields: [value | parser.fields], field: []}
  end

  defp finish_row(parser) do
    parser = finish_field(parser)
    row = Enum.reverse(parser.fields)
    {row, initial_parser_state()}
  end

  defp finish_parser(%{quoted: true}) do
    {:error, :unterminated_quoted_field}
  end

  defp finish_parser(parser) do
    parser = %{parser | quoted: false, after_quote: false}

    if parser.touched or parser.fields != [] or parser.field != [] do
      {row, _parser} = finish_row(parser)
      {:ok, initial_parser_state(), [row]}
    else
      {:ok, initial_parser_state(), []}
    end
  end
end
