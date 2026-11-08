# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: processamento de CSV com histórico, atomicidade, recuperação e persistência.
#
# Uso (exemplo em iex):
#   {:ok, _} = CSVPipeline.start_link(data_dir: "dados_csv")
#   CSVPipeline.process("clientes", {:file, "clientes.csv"}, retries: 2)
#   CSVPipeline.process("vendas", {:content, "id,valor\n1,10\n2,20"},
#     transform: fn row -> Map.update!(row, "valor", &String.to_integer/1) end)
#   CSVPipeline.fetch("clientes")
#   CSVPipeline.history()
#   CSVPipeline.delete("vendas")

defmodule CSVPipeline do
  @moduledoc """
  Pipeline de processamento de CSV com:

    * histórico de operações (sucessos e falhas);
    * atualizações atômicas (tudo ou nada, via registro único com checksum);
    * recuperação de processos (supervisor, retentativas e replay do diário);
    * persistência em disco (diário append-only + snapshots atômicos).
  """
  use Supervisor

  @doc "Inicia a árvore de supervisão (Task.Supervisor + Store)."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: CSVPipeline.TaskSupervisor},
      {CSVPipeline.Store, opts}
    ]

    # Se o Store cair, é reiniciado e reconstrói o estado a partir do disco.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 30)
  end

  @doc """
  Processa um CSV e, somente se tudo estiver válido, substitui o dataset de forma atômica.

  `source`: `{:file, caminho}` ou `{:content, binario}`.
  Opções:
    * `:transform` - função aplicada a cada linha (mapa). Deve retornar um mapa.
    * `:retries`   - novas tentativas em caso de crash do job (padrão 2).
    * `:timeout`   - tempo máximo por tentativa em ms (padrão 30_000).
  """
  def process(dataset, source, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)
    attempts = Keyword.get(opts, :retries, 2) + 1
    label = source_label(source)

    case run_with_retries(fn -> run_job(source, opts) end, timeout, attempts) do
      {:ok, header, rows} -> call({:import, dataset, header, rows, label})
      {:error, reason} -> call({:import_failed, dataset, label, reason})
    end
  end

  @doc "Retorna as linhas de um dataset."
  def fetch(dataset), do: call({:fetch, dataset})

  @doc "Retorna o histórico de operações em ordem cronológica."
  def history, do: call(:history)

  @doc "Lista os datasets existentes."
  def datasets, do: call(:datasets)

  @doc "Remove um dataset (operação registrada no histórico)."
  def delete(dataset), do: call({:delete, dataset})

  # -- Internos --------------------------------------------------------------

  defp call(msg) do
    GenServer.call(CSVPipeline.Store, msg, :infinity)
  catch
    :exit, _reason -> {:error, :store_unavailable}
  end

  defp source_label({:file, path}), do: "file:" <> to_string(path)
  defp source_label({:content, bin}), do: "content:#{byte_size(bin)}B"

  # Executa o job em um processo isolado. Crash do job não derruba o chamador.
  defp run_with_retries(job, timeout, attempts_left) do
    task = Task.Supervisor.async_nolink(CSVPipeline.TaskSupervisor, job)

    result =
      case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
        {:ok, res} -> res
        {:exit, reason} -> {:error, {:crashed, reason}}
        nil -> {:error, :timeout}
      end

    case result do
      {:error, {:crashed, _}} when attempts_left > 1 ->
        run_with_retries(job, timeout, attempts_left - 1)

      other ->
        other
    end
  end

  defp run_job(source, opts) do
    transform = Keyword.get(opts, :transform, &Function.identity/1)

    with {:ok, content} <- read_source(source),
         {:ok, rows} <- CSVPipeline.CSV.parse(content),
         {:ok, header, body} <- split_header(rows),
         {:ok, maps} <- to_maps(header, body, transform) do
      {:ok, header, maps}
    end
  end

  defp read_source({:file, path}), do: File.read(path)
  defp read_source({:content, bin}) when is_binary(bin), do: {:ok, bin}

  defp split_header([]), do: {:error, :empty_csv}
  defp split_header([header | body]), do: {:ok, header, body}

  defp to_maps(header, body, transform) do
    n = length(header)

    result =
      body
      |> Enum.with_index(2)
      |> Enum.reduce_while({:ok, []}, fn {row, line}, {:ok, acc} ->
        if length(row) == n do
          {:cont, {:ok, [transform.(Map.new(Enum.zip(header, row))) | acc]}}
        else
          {:halt, {:error, {:invalid_row, line}}}
        end
      end)

    case result do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end
end

defmodule CSVPipeline.CSV do
  @moduledoc false
  # Parser CSV (RFC 4180 simplificado): separador ",", campos entre aspas,
  # aspas escapadas por "" e quebras de linha LF ou CRLF.

  @doc "Converte o conteúdo em lista de linhas (listas de strings)."
  def parse(content) when is_binary(content) do
    tokenize(content, :plain, "", [], [])
  end

  # Fim da entrada
  defp tokenize(<<>>, :quoted, _field, _row, _rows), do: {:error, :unterminated_quote}

  defp tokenize(<<>>, :plain, field, row, rows) do
    {:ok, Enum.reverse(finish_row(field, row, rows))}
  end

  # Estado :quoted (dentro de aspas)
  defp tokenize(<<?", ?", rest::binary>>, :quoted, field, row, rows),
    do: tokenize(rest, :quoted, <<field::binary, ?">>, row, rows)

  defp tokenize(<<?", rest::binary>>, :quoted, field, row, rows),
    do: tokenize(rest, :plain, field, row, rows)

  defp tokenize(<<c, rest::binary>>, :quoted, field, row, rows),
    do: tokenize(rest, :quoted, <<field::binary, c>>, row, rows)

  # Estado :plain (fora de aspas)
  defp tokenize(<<?", rest::binary>>, :plain, "", row, rows),
    do: tokenize(rest, :quoted, "", row, rows)

  defp tokenize(<<?,, rest::binary>>, :plain, field, row, rows),
    do: tokenize(rest, :plain, "", [field | row], rows)

  defp tokenize(<<?\r, ?\n, rest::binary>>, :plain, field, row, rows),
    do: tokenize(rest, :plain, "", [], finish_row(field, row, rows))

  defp tokenize(<<?\n, rest::binary>>, :plain, field, row, rows),
    do: tokenize(rest, :plain, "", [], finish_row(field, row, rows))

  defp tokenize(<<c, rest::binary>>, :plain, field, row, rows),
    do: tokenize(rest, :plain, <<field::binary, c>>, row, rows)

  # Linhas vazias são ignoradas
  defp finish_row(field, [], rows) when field == "", do: rows
  defp finish_row(field, row, rows), do: [Enum.reverse([field | row]) | rows]
end

defmodule CSVPipeline.Journal do
  @moduledoc false
  # Persistência em disco.
  #
  # - Diário (journal.log): registros append-only, cada um com tamanho e CRC32.
  #   Um registro é escrito de uma vez; registro parcial/corrompido é detectado
  #   na leitura e descartado (atomicidade da operação).
  # - Snapshot (snapshot.bin): estado completo, escrito via arquivo temporário + rename.

  def read_snapshot(dir) do
    case File.read(snapshot_path(dir)) do
      {:ok, bin} -> {:ok, :erlang.binary_to_term(bin, [:safe])}
      {:error, :enoent} -> :none
    end
  end

  def write_snapshot(dir, snapshot) do
    atomic_write(snapshot_path(dir), :erlang.term_to_binary(snapshot))
  end

  def append(dir, entry) do
    write_sync(log_path(dir), :append, encode(entry))
  end

  def replace_log(dir, entries) do
    atomic_write(log_path(dir), IO.iodata_to_binary(Enum.map(entries, &encode/1)))
  end

  def read_log(dir) do
    case File.read(log_path(dir)) do
      {:ok, bin} -> decode(bin, [])
      {:error, :enoent} -> {[], :ok}
    end
  end

  # -- Internos --------------------------------------------------------------

  defp log_path(dir), do: Path.join(dir, "journal.log")
  defp snapshot_path(dir), do: Path.join(dir, "snapshot.bin")

  defp encode(entry) do
    payload = :erlang.term_to_binary(entry)
    <<byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>
  end

  defp decode(<<len::32, crc::32, payload::binary-size(len), rest::binary>>, acc) do
    if :erlang.crc32(payload) == crc do
      decode(rest, [:erlang.binary_to_term(payload, [:safe]) | acc])
    else
      {Enum.reverse(acc), :damaged}
    end
  end

  defp decode(<<>>, acc), do: {Enum.reverse(acc), :ok}
  defp decode(_partial_tail, acc), do: {Enum.reverse(acc), :damaged}

  # Escrita temporária + fsync + rename: nunca deixa arquivo final pela metade.
  defp atomic_write(path, data) do
    tmp = path <> ".tmp"
    with :ok <- write_sync(tmp, :write, data), do: :file.rename(tmp, path)
  end

  defp write_sync(path, mode, data) do
    case :file.open(path, [mode, :binary, :raw]) do
      {:ok, io} ->
        try do
          with :ok <- :file.write(io, data), do: :file.datasync(io)
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end

defmodule CSVPipeline.Store do
  @moduledoc false
  # Estado central. Toda mudança é primeiro gravada no diário e só depois
  # aplicada em memória. Ao iniciar, reconstrói o estado a partir do disco.

  use GenServer
  alias CSVPipeline.Journal

  @checkpoint_every 50

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :data_dir, "csv_data")
    File.mkdir_p!(dir)
    {:ok, recover(dir)}
  end

  @impl true
  def handle_call({:import, name, header, rows, source}, _from, st) do
    entry = new_entry(st, :import, name, source, :committed, [{:put, name, header, rows}], nil)
    respond(entry, st, {:ok, entry.id})
  end

  def handle_call({:import_failed, name, source, reason}, _from, st) do
    entry = new_entry(st, :import, name, source, :failed, [], inspect(reason))
    respond(entry, st, {:error, reason})
  end

  def handle_call({:delete, name}, _from, st) do
    if Map.has_key?(st.datasets, name) do
      entry = new_entry(st, :delete, name, nil, :committed, [{:delete, name}], nil)
      respond(entry, st, :ok)
    else
      entry = new_entry(st, :delete, name, nil, :failed, [], "not_found")
      respond(entry, st, {:error, :not_found})
    end
  end

  def handle_call({:fetch, name}, _from, st) do
    reply =
      case Map.fetch(st.datasets, name) do
        {:ok, %{rows: rows}} -> {:ok, rows}
        :error -> {:error, :not_found}
      end

    {:reply, reply, st}
  end

  def handle_call(:history, _from, st), do: {:reply, Enum.reverse(st.history), st}
  def handle_call(:datasets, _from, st), do: {:reply, Map.keys(st.datasets), st}

  # -- Commit ----------------------------------------------------------------

  # Persiste a entrada; só aplica em memória se a persistência for bem-sucedida.
  defp respond(entry, st, ok_reply) do
    case Journal.append(st.dir, entry) do
      :ok ->
        st = st |> apply_entry(entry) |> maybe_checkpoint()
        {:reply, ok_reply, st}

      {:error, reason} ->
        # Possível escrita parcial no diário: regrava o log a partir do estado consistente.
        case checkpoint(st) do
          :ok ->
            {:reply, {:error, {:persist_failed, reason}}, st}

          {:error, cause} ->
            # Sem garantia de integridade do diário: derruba o processo para
            # que o supervisor reinicie e reconstrua o estado a partir do disco.
            {:stop, {:persist_failed, cause}, {:error, {:persist_failed, reason}}, st}
        end
    end
  end

  defp new_entry(st, kind, name, source, status, ops, reason) do
    %{
      id: st.last_id + 1,
      at: DateTime.utc_now(),
      kind: kind,
      dataset: name,
      source: source,
      status: status,
      ops: ops,
      reason: reason
    }
  end

  defp apply_entry(st, %{status: :committed, ops: ops} = entry) do
    datasets = Enum.reduce(ops, st.datasets, &apply_op/2)
    push(%{st | datasets: datasets}, entry)
  end

  defp apply_entry(st, entry), do: push(st, entry)

  defp apply_op({:put, name, header, rows}, ds),
    do: Map.put(ds, name, %{header: header, rows: rows})

  defp apply_op({:delete, name}, ds), do: Map.delete(ds, name)

  defp push(st, entry) do
    summary = %{entry | ops: Enum.map(entry.ops, &summarize/1)}
    %{st | last_id: entry.id, history: [summary | st.history], since: st.since + 1}
  end

  # O histórico guarda apenas um resumo (sem as linhas inteiras).
  defp summarize({:put, name, _header, rows}), do: {:put, name, length(rows)}
  defp summarize({:delete, name}), do: {:delete, name}

  # -- Checkpoint e recuperação ----------------------------------------------

  defp maybe_checkpoint(%{since: n} = st) when n >= @checkpoint_every do
    case checkpoint(st) do
      :ok -> %{st | since: 0}
      # O diário continua íntegro; nova tentativa na próxima operação.
      {:error, _} -> st
    end
  end

  defp maybe_checkpoint(st), do: st

  # Grava snapshot e só então esvazia o diário. Entradas com id <= last_id
  # são ignoradas no replay, então uma queda entre as duas etapas é segura.
  defp checkpoint(st) do
    snapshot = %{last_id: st.last_id, datasets: st.datasets, history: st.history}

    with :ok <- Journal.write_snapshot(st.dir, snapshot) do
      Journal.replace_log(st.dir, [])
    end
  end

  defp recover(dir) do
    base =
      case Journal.read_snapshot(dir) do
        {:ok, snap} -> Map.merge(snap, %{dir: dir, since: 0})
        :none -> %{dir: dir, since: 0, last_id: 0, datasets: %{}, history: []}
      end

    {entries, status} = Journal.read_log(dir)

    # Remove cauda parcial/corrompida deixada por queda durante escrita.
    if status == :damaged, do: :ok = Journal.replace_log(dir, entries)

    entries
    |> Enum.filter(&(&1.id > base.last_id))
    |> Enum.reduce(base, &apply_entry(&2, &1))
  end
end
