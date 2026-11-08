# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único e autocontido: processamento de CSV com histórico de operações,
# atualizações atômicas, recuperação após falhas (supervisão OTP) e persistência
# em disco (escrita em arquivo temporário + fsync + rename atômico).
#
# Uso (exemplo em IEx):
#   {:ok, _} = CsvProcessor.start_link(path: "estado.bin")
#   CsvProcessor.import_csv("vendas", "nome,valor\nAna,10\nBruno,20")
#   CsvProcessor.transform("vendas", [{:filter_eq, "nome", "Ana"}, {:rename, "valor", "total"}])
#   CsvProcessor.export_csv("vendas")
#   CsvProcessor.history()

defmodule CsvProcessor do
  @moduledoc """
  API pública do processador de CSV.

  Garantias:
    * Histórico: toda operação (sucesso ou rejeição) gera uma entrada no histórico.
    * Atomicidade: uma operação (ou um pipeline de transformações) só altera o estado
      se TODAS as etapas forem válidas e a persistência em disco for concluída.
    * Recuperação: o processo de estado roda sob um Supervisor; em caso de falha
      ele é reiniciado e recarrega o último estado persistido.
    * Persistência: o estado é gravado de forma atômica (tmp + fsync + rename),
      de modo que o arquivo em disco nunca fique parcialmente escrito.
  """

  def start_link(opts \\ []), do: CsvProcessor.Supervisor.start_link(opts)

  def import_csv(name, csv_text), do: CsvProcessor.Store.import_csv(name, csv_text)

  def transform(name, ops), do: CsvProcessor.Store.transform(name, ops)

  def fetch(name), do: CsvProcessor.Store.fetch(name)

  def export_csv(name), do: CsvProcessor.Store.export_csv(name)

  def history, do: CsvProcessor.Store.history()
end

defmodule CsvProcessor.Supervisor do
  @moduledoc false
  # Supervisor responsável por reiniciar o Store após falhas inesperadas.
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {CsvProcessor.Store, opts}
    ]

    # :one_for_one: apenas o processo que falhou é reiniciado.
    # Ao reiniciar, o Store recarrega o estado persistido em disco.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule CsvProcessor.Parser do
  @moduledoc false
  # Parser de CSV próprio (sem dependências externas), com suporte a campos
  # entre aspas, aspas escapadas ("") e quebras de linha dentro de campos.

  @spec parse(binary()) :: {:ok, [[binary()]]} | {:error, atom()}
  def parse(text) when is_binary(text) do
    scan(text, "", [], [], false)
  end

  # Fim da entrada dentro de aspas: CSV malformado.
  defp scan(<<>>, _field, _row, _rows, true), do: {:error, :unclosed_quote}

  # Fim da entrada fora de aspas: fecha a última linha pendente.
  defp scan(<<>>, field, row, rows, false), do: {:ok, Enum.reverse(close_row(field, row, rows))}

  # Dentro de aspas
  defp scan(<<?", ?", rest::binary>>, field, row, rows, true),
    do: scan(rest, field <> "\"", row, rows, true)

  defp scan(<<?", rest::binary>>, field, row, rows, true),
    do: scan(rest, field, row, rows, false)

  defp scan(<<c, rest::binary>>, field, row, rows, true),
    do: scan(rest, <<field::binary, c>>, row, rows, true)

  # Fora de aspas
  defp scan(<<?", rest::binary>>, field, row, rows, false),
    do: scan(rest, field, row, rows, true)

  defp scan(<<?,, rest::binary>>, field, row, rows, false),
    do: scan(rest, "", [field | row], rows, false)

  defp scan(<<?\n, rest::binary>>, field, row, rows, false),
    do: scan(rest, "", [], close_row(field, row, rows), false)

  defp scan(<<?\r, rest::binary>>, field, row, rows, false),
    do: scan(rest, field, row, rows, false)

  defp scan(<<c, rest::binary>>, field, row, rows, false),
    do: scan(rest, <<field::binary, c>>, row, rows, false)

  # Ignora linhas totalmente vazias.
  defp close_row("", [], rows), do: rows
  defp close_row(field, row, rows), do: [Enum.reverse([field | row]) | rows]
end

defmodule CsvProcessor.Store do
  @moduledoc false
  # Processo de estado. Todas as alterações passam por `run/4`, que:
  #   1. calcula o novo estado em memória (função pura);
  #   2. registra a entrada de histórico;
  #   3. persiste o novo estado de forma atômica;
  #   4. só então substitui o estado em memória.
  # Se qualquer etapa falhar, o estado anterior é mantido intacto.

  use GenServer
  require Logger

  @format_version 1
  @default_path "csv_processor_state.bin"

  # ---------------------------------------------------------------------------
  # API cliente
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def import_csv(server \\ __MODULE__, name, csv_text),
    do: GenServer.call(server, {:import, name, csv_text})

  def transform(server \\ __MODULE__, name, ops),
    do: GenServer.call(server, {:transform, name, ops})

  def fetch(server \\ __MODULE__, name),
    do: GenServer.call(server, {:fetch, name})

  def export_csv(server \\ __MODULE__, name),
    do: GenServer.call(server, {:export, name})

  def history(server \\ __MODULE__),
    do: GenServer.call(server, :history)

  # ---------------------------------------------------------------------------
  # Callbacks GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)

    case load(path) do
      {:ok, snapshot} -> {:ok, Map.put(snapshot, :path, path)}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:import, name, csv}, _from, state) do
    {reply, new_state} = run(state, :import, name, &do_import(&1, name, csv))
    {:reply, reply, new_state}
  end

  def handle_call({:transform, name, ops}, _from, state) do
    {reply, new_state} = run(state, :transform, name, &do_transform(&1, name, ops))
    {:reply, reply, new_state}
  end

  def handle_call({:fetch, name}, _from, state) do
    {:reply, fetch_ds(state.datasets, name), state}
  end

  def handle_call({:export, name}, _from, state) do
    reply =
      case fetch_ds(state.datasets, name) do
        {:ok, ds} -> {:ok, to_csv(ds)}
        error -> error
      end

    {:reply, reply, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, {:ok, Enum.reverse(state.history)}, state}
  end

  # ---------------------------------------------------------------------------
  # Núcleo transacional
  # ---------------------------------------------------------------------------

  # Executa uma operação de forma atômica e registra no histórico.
  defp run(state, action, name, fun) do
    {status, datasets, reply} =
      case fun.(state.datasets) do
        {:ok, ds, info} -> {:committed, ds, {:ok, info}}
        {:error, reason} -> {:rejected, state.datasets, {:error, reason}}
      end

    detail = elem(reply, 1)

    entry = %{
      seq: state.seq,
      at: DateTime.utc_now(),
      action: action,
      dataset: name,
      status: status,
      detail: detail
    }

    next = %{
      state
      | datasets: datasets,
        history: [entry | state.history],
        seq: state.seq + 1
    }

    # Ponto de commit: só substitui o estado em memória se a persistência funcionar.
    case persist(next) do
      :ok -> {reply, next}
      {:error, reason} -> {{:error, {:persist_failed, reason}}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Operações sobre datasets (funções puras)
  # ---------------------------------------------------------------------------

  defp do_import(datasets, name, csv) do
    with :ok <- check_encoding(csv),
         {:ok, rows} <- CsvProcessor.Parser.parse(csv),
         {:ok, header, body} <- split_header(rows),
         :ok <- validate_header(header),
         :ok <- validate_width(header, body) do
      ds = %{header: header, rows: body}
      {:ok, Map.put(datasets, name, ds), %{columns: length(header), rows: length(body)}}
    end
  end

  defp do_transform(datasets, name, ops) when is_list(ops) do
    with {:ok, ds} <- fetch_ds(datasets, name),
         {:ok, new_ds} <- apply_ops(ds, ops) do
      info = %{
        columns: length(new_ds.header),
        rows: length(new_ds.rows),
        operations: length(ops)
      }

      {:ok, Map.put(datasets, name, new_ds), info}
    end
  end

  defp do_transform(_datasets, _name, _ops), do: {:error, :invalid_operations}

  # Pipeline all-or-nothing: qualquer erro interrompe e descarta tudo.
  defp apply_ops(ds, ops) do
    Enum.reduce_while(ops, {:ok, ds}, fn op, {:ok, acc} ->
      case apply_op(acc, op) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp apply_op(ds, {:select, cols}) when is_list(cols) do
    with {:ok, idxs} <- column_indexes(ds.header, cols) do
      rows = Enum.map(ds.rows, fn row -> Enum.map(idxs, &Enum.at(row, &1)) end)
      {:ok, %{header: cols, rows: rows}}
    end
  end

  defp apply_op(ds, {:filter_eq, col, value}) do
    with {:ok, [i]} <- column_indexes(ds.header, [col]) do
      {:ok, %{ds | rows: Enum.filter(ds.rows, &(Enum.at(&1, i) == value))}}
    end
  end

  defp apply_op(ds, {:sort, col}) do
    with {:ok, [i]} <- column_indexes(ds.header, [col]) do
      {:ok, %{ds | rows: Enum.sort_by(ds.rows, &Enum.at(&1, &1 |> then(fn _ -> i end)))}}
    end
  end

  defp apply_op(ds, {:rename, old, new}) do
    cond do
      old not in ds.header -> {:error, {:unknown_column, old}}
      new in ds.header -> {:error, {:duplicate_column, new}}
      true -> {:ok, %{ds | header: Enum.map(ds.header, &if(&1 == old, do: new, else: &1))}}
    end
  end

  defp apply_op(_ds, op), do: {:error, {:unknown_op, op}}

  defp column_indexes(header, cols) do
    result =
      Enum.reduce_while(cols, {:ok, []}, fn col, {:ok, acc} ->
        case Enum.find_index(header, &(&1 == col)) do
          nil -> {:halt, {:error, {:unknown_column, col}}}
          i -> {:cont, {:ok, [i | acc]}}
        end
      end)

    case result do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp fetch_ds(datasets, name) do
    case Map.fetch(datasets, name) do
      {:ok, ds} -> {:ok, ds}
      :error -> {:error, :unknown_dataset}
    end
  end

  defp check_encoding(csv) do
    if String.valid?(csv), do: :ok, else: {:error, :invalid_encoding}
  end

  defp split_header([]), do: {:error, :empty_csv}
  defp split_header([header | body]), do: {:ok, header, body}

  defp validate_header(header) do
    if length(Enum.uniq(header)) == length(header),
      do: :ok,
      else: {:error, :duplicate_columns}
  end

  defp validate_width(header, body) do
    width = length(header)

    case Enum.find_index(body, &(length(&1) != width)) do
      nil -> :ok
      # Índice + 2 considera que o cabeçalho está na linha 1.
      i -> {:error, {:invalid_row, i + 2}}
    end
  end

  defp to_csv(%{header: header, rows: rows}) do
    [header | rows]
    |> Enum.map(fn row -> Enum.map_join(row, ",", &encode_field/1) end)
    |> Enum.join("\n")
  end

  defp encode_field(field) do
    if String.contains?(field, [",", "\"", "\n", "\r"]) do
      "\"" <> String.replace(field, "\"", "\"\"") <> "\""
    else
      field
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência
  # ---------------------------------------------------------------------------

  defp empty, do: %{datasets: %{}, history: [], seq: 1}

  defp load(path) do
    # Um arquivo .tmp remanescente indica escrita interrompida por falha;
    # o arquivo principal continua válido, então o temporário é descartado.
    _ = File.rm(path <> ".tmp")

    case File.read(path) do
      {:ok, bin} ->
        case decode(bin) do
          {:ok, snapshot} ->
            {:ok, snapshot}

          {:error, reason} ->
            backup = path <> ".corrupt"
            Logger.error("Estado corrompido (#{inspect(reason)}); movendo para #{backup}")
            _ = File.rename(path, backup)
            {:ok, empty()}
        end

      {:error, :enoent} ->
        {:ok, empty()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decode(bin) do
    case :erlang.binary_to_term(bin, [:safe]) do
      %{version: @format_version, datasets: d, history: h, seq: s}
      when is_map(d) and is_list(h) and is_integer(s) ->
        {:ok, %{datasets: d, history: h, seq: s}}

      _ ->
        {:error, :invalid_format}
    end
  rescue
    ArgumentError -> {:error, :invalid_binary}
  end

  defp persist(state) do
    snapshot = %{
      version: @format_version,
      datasets: state.datasets,
      history: state.history,
      seq: state.seq
    }

    bin = :erlang.term_to_binary(snapshot)
    tmp = state.path <> ".tmp"

    # Escrita atômica: grava e sincroniza o temporário, depois troca pelo rename,
    # que é atômico no sistema de arquivos. Nunca há arquivo parcialmente escrito.
    with :ok <- write_sync(tmp, bin),
         :ok <- File.rename(tmp, state.path) do
      :ok
    end
  end

  defp write_sync(path, bin) do
    case File.open(path, [:write, :binary], fn io ->
           with :ok <- IO.binwrite(io, bin), do: :file.sync(io)
         end) do
      {:ok, :ok} -> :ok
      {:ok, {:error, reason}} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end
end
