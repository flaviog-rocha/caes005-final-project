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
  Armazenamento de registros CSV com:

  * persistência em log append-only (`records.csv`), recuperada por reprodução do log;
  * histórico de operações em `history.csv`;
  * índice em memória apenas com `id -> {offset, tamanho}`; os valores são lidos
    do disco sob demanda com `:file.pread/3`, e importações/consultas são feitas
    por streaming;
  * operações: insert, get, update, delete, put (upsert), reduce (consulta), import_csv e compact.

  Uso básico:

      {:ok, _} = CsvStore.start_link(dir: "dados")
      :ok = CsvStore.insert("1", ["Ana", "ana@x.com"])
      {:ok, ["Ana", "ana@x.com"]} = CsvStore.get("1")
      :ok = CsvStore.update("1", ["Ana Maria", "ana@x.com"])
      :ok = CsvStore.delete("1")
      CsvStore.history(10)
      CsvStore.import_csv("grande.csv")
      CsvStore.reduce(0, fn _id, [nome | _], acc -> acc + byte_size(nome) end)
  """

  use GenServer

  @data_file "records.csv"
  @history_file "history.csv"

  # ===================================================================
  # API pública
  # ===================================================================

  @doc "Inicia o servidor. Opções: `:dir` (diretório de dados) e `:name`."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Cadastra um novo registro. Falha se o id já existir."
  def insert(server \\ __MODULE__, id, fields) do
    GenServer.call(server, {:insert, norm(id), norm_fields(fields)})
  end

  @doc "Consulta um registro pelo id."
  def get(server \\ __MODULE__, id) do
    GenServer.call(server, {:get, norm(id)})
  end

  @doc "Atualiza um registro existente. Falha se o id não existir."
  def update(server \\ __MODULE__, id, fields) do
    GenServer.call(server, {:update, norm(id), norm_fields(fields)})
  end

  @doc "Cadastra ou atualiza (upsert) um registro."
  def put(server \\ __MODULE__, id, fields) do
    GenServer.call(server, {:put, norm(id), norm_fields(fields)})
  end

  @doc "Remove um registro. Falha se o id não existir."
  def delete(server \\ __MODULE__, id) do
    GenServer.call(server, {:delete, norm(id)})
  end

  @doc "Retorna as últimas `limit` entradas do histórico de operações."
  def history(server \\ __MODULE__, limit) when is_integer(limit) and limit > 0 do
    GenServer.call(server, {:history, limit})
  end

  @doc """
  Percorre todos os registros vivos, um a um, acumulando o resultado de
  `fun.(id, fields, acc)`. Nunca materializa a lista completa de registros.
  """
  def reduce(server \\ __MODULE__, acc, fun) when is_function(fun, 3) do
    GenServer.call(server, {:reduce, acc, fun}, :infinity)
  end

  @doc """
  Importa um CSV em streaming: a primeira coluna é o id, as demais são os campos.
  Cada linha é gravada como upsert. Retorna `{:ok, quantidade_importada}`.
  """
  def import_csv(server \\ __MODULE__, path) do
    count =
      path
      |> csv_rows()
      |> Enum.reduce(0, fn
        [id | fields], count when id != "" ->
          :ok = put(server, id, fields)
          count + 1

        _row, count ->
          count
      end)

    {:ok, count}
  end

  @doc "Reescreve o arquivo de dados contendo somente os registros vivos."
  def compact(server \\ __MODULE__) do
    GenServer.call(server, :compact, :infinity)
  end

  # ===================================================================
  # Callbacks do GenServer
  # ===================================================================

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :dir, "csv_store")
    File.mkdir_p!(dir)

    data_path = Path.join(dir, @data_file)
    hist_path = Path.join(dir, @history_file)

    # Recuperação: reconstrói o índice a partir do log e descarta
    # eventual registro parcialmente escrito no fim do arquivo.
    {index, valid} = rebuild_index(data_path)
    truncate_torn_tail(data_path, valid)

    {:ok, append} = :file.open(data_path, [:append, :binary, :raw])
    {:ok, read} = :file.open(data_path, [:read, :binary, :raw])
    {:ok, hist} = :file.open(hist_path, [:append, :binary, :raw])

    {:ok,
     %{
       data_path: data_path,
       hist_path: hist_path,
       index: index,
       size: valid,
       append: append,
       read: read,
       hist: hist
     }}
  end

  @impl true
  def handle_call({:insert, id, fields}, _from, st) do
    if Map.has_key?(st.index, id) do
      {:reply, {:error, :already_exists}, st}
    else
      st = st |> write_record("P", id, fields) |> log("insert", id)
      {:reply, :ok, st}
    end
  end

  def handle_call({:update, id, fields}, _from, st) do
    if Map.has_key?(st.index, id) do
      st = st |> write_record("P", id, fields) |> log("update", id)
      {:reply, :ok, st}
    else
      {:reply, {:error, :not_found}, st}
    end
  end

  def handle_call({:put, id, fields}, _from, st) do
    st = st |> write_record("P", id, fields) |> log("put", id)
    {:reply, :ok, st}
  end

  def handle_call({:delete, id}, _from, st) do
    if Map.has_key?(st.index, id) do
      st = st |> write_record("D", id, []) |> log("delete", id)
      {:reply, :ok, st}
    else
      {:reply, {:error, :not_found}, st}
    end
  end

  def handle_call({:get, id}, _from, st) do
    reply =
      case Map.fetch(st.index, id) do
        {:ok, loc} -> {:ok, read_fields(st, loc)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, st}
  end

  def handle_call({:reduce, acc, fun}, _from, st) do
    # Lê um registro por vez a partir do disco.
    result =
      Enum.reduce(st.index, acc, fn {id, loc}, acc ->
        fun.(id, read_fields(st, loc), acc)
      end)

    {:reply, result, st}
  end

  def handle_call({:history, limit}, _from, st) do
    # Mantém apenas as últimas `limit` linhas durante a leitura em streaming.
    queue =
      st.hist_path
      |> File.stream!()
      |> Enum.reduce(:queue.new(), fn line, q ->
        q = if :queue.len(q) >= limit, do: :queue.drop(q), else: q
        :queue.in(decode_line(line), q)
      end)

    {:reply, :queue.to_list(queue), st}
  end

  def handle_call(:compact, _from, st) do
    tmp = st.data_path <> ".compact"
    {:ok, out} = :file.open(tmp, [:write, :binary, :raw])

    # Copia apenas os registros vivos, recalculando os offsets.
    {index, size} =
      Enum.reduce(st.index, {%{}, 0}, fn {id, {off, len}}, {idx, pos} ->
        {:ok, bin} = :file.pread(st.read, off, len)
        :ok = :file.write(out, bin)
        {Map.put(idx, id, {pos, len}), pos + len}
      end)

    :ok = :file.sync(out)
    :ok = :file.close(out)
    :ok = :file.close(st.append)
    :ok = :file.close(st.read)
    :ok = File.rename(tmp, st.data_path)

    {:ok, append} = :file.open(st.data_path, [:append, :binary, :raw])
    {:ok, read} = :file.open(st.data_path, [:read, :binary, :raw])

    st =
      %{st | index: index, size: size, append: append, read: read}
      |> log("compact", "-")

    {:reply, :ok, st}
  end

  # ===================================================================
  # Escrita e histórico
  # ===================================================================

  # Acrescenta um registro ao log e atualiza o índice.
  # "P" = put (inserção/atualização), "D" = tombstone (remoção).
  defp write_record(st, flag, id, fields) do
    bin = encode_row([flag, id | fields])
    :ok = :file.write(st.append, bin)
    :ok = :file.sync(st.append)

    index =
      if flag == "D",
        do: Map.delete(st.index, id),
        else: Map.put(st.index, id, {st.size, byte_size(bin)})

    %{st | index: index, size: st.size + byte_size(bin)}
  end

  # Registra a operação no histórico com timestamp UTC.
  defp log(st, op, id) do
    ts = DateTime.utc_now() |> DateTime.to_iso8601()
    line = encode_row([ts, op, id, "ok"])
    :ok = :file.write(st.hist, line)
    :ok = :file.sync(st.hist)
    st
  end

  defp read_fields(st, {offset, len}) do
    {:ok, bin} = :file.pread(st.read, offset, len)
    ["P", _id | fields] = decode_line(bin)
    fields
  end

  # ===================================================================
  # Recuperação
  # ===================================================================

  # Reproduz o log em streaming. Retorna o índice e o tamanho válido do arquivo.
  defp rebuild_index(path) do
    if File.exists?(path) do
      {index, valid, _pending} =
        path
        |> File.stream!()
        |> Enum.reduce({%{}, 0, ""}, fn line, {idx, valid, buf} ->
          buf = buf <> line

          if complete?(buf) do
            {apply_record(idx, valid, buf), valid + byte_size(buf), ""}
          else
            {idx, valid, buf}
          end
        end)

      {index, valid}
    else
      {%{}, 0}
    end
  end

  defp apply_record(idx, offset, bin) do
    case decode_line(bin) do
      ["P", id | _] -> Map.put(idx, id, {offset, byte_size(bin)})
      ["D", id | _] -> Map.delete(idx, id)
      _ -> idx
    end
  end

  defp truncate_torn_tail(path, valid) do
    if File.exists?(path) and File.stat!(path).size > valid do
      {:ok, fd} = :file.open(path, [:read, :write, :binary])
      {:ok, ^valid} = :file.position(fd, valid)
      :ok = :file.truncate(fd)
      :ok = :file.close(fd)
    end

    :ok
  end

  # ===================================================================
  # CSV: streaming e codificação (RFC 4180 simplificada)
  # ===================================================================

  # Produz linhas CSV em streaming, tratando campos com quebras de linha entre aspas.
  defp csv_rows(path) do
    path
    |> File.stream!()
    |> Stream.transform(
      fn -> "" end,
      fn line, buf ->
        buf = buf <> line

        if balanced?(buf),
          do: {[decode_line(buf)], ""},
          else: {[], buf}
      end,
      fn buf ->
        if buf != "", do: raise(ArgumentError, "CSV com aspas não fechadas")
      end
    )
  end

  defp complete?(buf), do: String.ends_with?(buf, "\n") and balanced?(buf)

  defp balanced?(buf), do: rem(length(:binary.matches(buf, "\"")), 2) == 0

  defp encode_row(fields) do
    Enum.map_join(fields, ",", &escape_field/1) <> "\n"
  end

  defp escape_field(field) do
    if String.contains?(field, [",", "\"", "\n", "\r"]) do
      "\"" <> String.replace(field, "\"", "\"\"") <> "\""
    else
      field
    end
  end

  defp decode_line(line) do
    line
    |> String.trim_trailing("\n")
    |> String.trim_trailing("\r")
    |> parse_fields([])
  end

  defp parse_fields(bin, acc) do
    {field, rest} = parse_field(bin)
    acc = [field | acc]

    case rest do
      :eof -> Enum.reverse(acc)
      <<",", more::binary>> -> parse_fields(more, acc)
      _ -> raise ArgumentError, "linha CSV malformada"
    end
  end

  defp parse_field(<<?", rest::binary>>), do: parse_quoted(rest, [])
  defp parse_field(bin), do: parse_plain(bin, [])

  defp parse_plain(<<",", _::binary>> = bin, acc), do: {bytes(acc), bin}
  defp parse_plain(<<c, rest::binary>>, acc), do: parse_plain(rest, [c | acc])
  defp parse_plain(<<>>, acc), do: {bytes(acc), :eof}

  defp parse_quoted(<<?", ?", rest::binary>>, acc), do: parse_quoted(rest, [?" | acc])

  defp parse_quoted(<<?", rest::binary>>, acc) do
    {bytes(acc), if(rest == "", do: :eof, else: rest)}
  end

  defp parse_quoted(<<c, rest::binary>>, acc), do: parse_quoted(rest, [c | acc])
  defp parse_quoted(<<>>, _acc), do: raise(ArgumentError, "campo entre aspas não fechado")

  defp bytes(acc), do: acc |> Enum.reverse() |> :erlang.list_to_binary()

  # ===================================================================
  # Utilidades
  # ===================================================================

  defp norm(value), do: to_string(value)
  defp norm_fields(fields), do: Enum.map(fields, &to_string/1)
end
