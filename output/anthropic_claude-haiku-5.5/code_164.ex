# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único e autocontido: supervisor, semáforo de concorrência,
# armazenamento em CSV com escrita atômica e codificação CSV mínima.
# Requer apenas a biblioteca padrão do Elixir/OTP.

defmodule CSVProcessor do
  @moduledoc """
  Processamento de registros persistidos em arquivo CSV.

  Requisitos atendidos:

    1. Limite de operações simultâneas via `CSVProcessor.Semaphore`.
    2. Cadastro, consulta, atualização e remoção de registros.
    3. Atualizações atômicas: um lote é aplicado por completo ou não é aplicado.
       A persistência usa arquivo temporário + `File.rename/2`, que é atômico.
    4. Detecção de encerramentos inesperados: o semáforo monitora os processos
       que ocupam slots e recupera o slot quando o processo morre sem liberá-lo.

  Exemplo de uso:

      {:ok, _pid} =
        CSVProcessor.start_link(
          path: "dados.csv",
          columns: ["nome", "email"],
          max_concurrency: 4
        )

      {:ok, id} = CSVProcessor.insert(%{"nome" => "Ana", "email" => "ana@x.com"})
      {:ok, registro} = CSVProcessor.get(id)
      :ok = CSVProcessor.update(id, %{"email" => "nova@x.com"})
      :ok = CSVProcessor.delete(id)
      {:ok, _ids} = CSVProcessor.import_file("entrada.csv")
  """

  use Supervisor

  alias CSVProcessor.{CSV, Semaphore, Store}

  @doc """
  Inicia o supervisor com as opções:

    * `:path` - caminho do arquivo CSV (obrigatório)
    * `:columns` - colunas usadas quando o arquivo ainda não existe
    * `:max_concurrency` - número máximo de operações simultâneas
  """
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Semaphore, Keyword.get(opts, :max_concurrency, System.schedulers_online())},
      {Store, opts}
    ]

    # Se o Store cair, o supervisor o reinicia e ele recarrega o estado do arquivo.
    # Como a escrita é atômica, o arquivo nunca fica com dados parciais.
    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cadastra um registro e retorna `{:ok, id}`."
  def insert(attrs) do
    case batch([{:insert, attrs}]) do
      {:ok, [id]} -> {:ok, id}
      error -> error
    end
  end

  @doc "Consulta um registro pelo id."
  def get(id) do
    run(fn -> GenServer.call(Store, {:get, to_string(id)}, :infinity) end)
  end

  @doc "Lista todos os registros ordenados pelo id."
  def list do
    run(fn -> GenServer.call(Store, :list, :infinity) end)
  end

  @doc "Atualiza campos de um registro existente."
  def update(id, changes) do
    case batch([{:update, to_string(id), changes}]) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc "Remove um registro pelo id."
  def delete(id) do
    case batch([{:delete, to_string(id)}]) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Aplica uma lista de operações de forma atômica.

  Operações aceitas:

    * `{:insert, attrs}`
    * `{:update, id, changes}`
    * `{:delete, id}`

  Se qualquer operação falhar, nenhuma é aplicada e o arquivo não é alterado.
  Retorna `{:ok, ids_inseridos}` ou `{:error, motivo}`.
  """
  def batch(ops) when is_list(ops) do
    run(fn -> GenServer.call(Store, {:batch, ops}, :infinity) end)
  end

  @doc """
  Importa um arquivo CSV com cabeçalho. Cada linha vira um cadastro.
  A validação das linhas é feita em paralelo, sob controle do semáforo,
  e a inserção é atômica: se uma linha for inválida, nada é importado.
  """
  def import_file(path) do
    with {:ok, content} <- File.read(path),
         [header | rows] <- CSV.parse(content),
         {:ok, ops} <- build_ops(header, rows) do
      batch(ops)
    else
      [] -> {:error, :empty_file}
      error -> error
    end
  end

  @doc "Retorna estatísticas do semáforo (máximo, ativos, em espera)."
  def stats, do: Semaphore.stats()

  # ---------------------------------------------------------------------------
  # Internos
  # ---------------------------------------------------------------------------

  # Toda operação passa pelo semáforo, limitando a concorrência.
  defp run(fun) do
    case Semaphore.run(fun) do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_ops(header, rows) do
    rows
    |> Task.async_stream(
      fn row -> run(fn -> row_to_attrs(header, row) end) end,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, attrs}}, {:ok, acc} ->
        {:cont, {:ok, [{:insert, attrs} | acc]}}

      {:ok, {:error, reason}}, _acc ->
        {:halt, {:error, reason}}

      {:exit, reason}, _acc ->
        {:halt, {:error, {:task_crashed, reason}}}
    end)
    |> case do
      {:ok, ops} -> {:ok, Enum.reverse(ops)}
      error -> error
    end
  end

  defp row_to_attrs(header, row) do
    if length(header) == length(row) do
      {:ok, Map.new(Enum.zip(header, row))}
    else
      {:error, {:invalid_row, row}}
    end
  end
end

# -----------------------------------------------------------------------------
# Semáforo de contagem com detecção de encerramentos inesperados
# -----------------------------------------------------------------------------
defmodule CSVProcessor.Semaphore do
  @moduledoc false
  # Limita o número de operações simultâneas.
  # Cada processo que adquire um slot é monitorado. Se ele morrer sem liberar
  # o slot, o DOWN é recebido, o slot é recuperado e o evento é registrado.

  use GenServer

  def start_link(max) when is_integer(max) and max > 0 do
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc """
  Executa `fun` ocupando um slot. Retorna `{:ok, resultado}` ou `{:error, motivo}`
  se a função lançar exceção, lançar throw ou sair.
  """
  def run(fun) when is_function(fun, 0) do
    token = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      {:ok, fun.()}
    rescue
      e -> {:error, {:exception, e}}
    catch
      kind, reason -> {:error, {kind, reason}}
    after
      GenServer.call(__MODULE__, {:release, token}, :infinity)
    end
  end

  def stats do
    GenServer.call(__MODULE__, :stats, :infinity)
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    # Monitora o solicitante para detectar encerramento inesperado.
    mref = Process.monitor(pid)

    if map_size(state.active) < state.max do
      {:reply, mref, %{state | active: Map.put(state.active, mref, pid)}}
    else
      # Resposta adiada: o cliente permanece bloqueado até obter um slot.
      {:noreply, %{state | waiting: :queue.in({from, mref}, state.waiting)}}
    end
  end

  def handle_call({:release, mref}, _from, state) do
    # Remove o monitor para não receber DOWN de um processo que liberou corretamente.
    Process.demonitor(mref, [:flush])
    state = %{state | active: Map.delete(state.active, mref)}
    {:reply, :ok, promote(state)}
  end

  def handle_call(:stats, _from, state) do
    stats = %{
      max: state.max,
      active: map_size(state.active),
      waiting: :queue.len(state.waiting)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, pid, reason}, state) do
    cond do
      Map.has_key?(state.active, mref) ->
        log(
          "processo #{inspect(pid)} encerrou sem liberar o slot " <>
            "(motivo: #{inspect(reason)}); slot recuperado"
        )

        state = %{state | active: Map.delete(state.active, mref)}
        {:noreply, promote(state)}

      true ->
        log(
          "processo #{inspect(pid)} encerrou enquanto aguardava slot " <>
            "(motivo: #{inspect(reason)})"
        )

        waiting = :queue.filter(fn {_from, m} -> m != mref end, state.waiting)
        {:noreply, %{state | waiting: waiting}}
    end
  end

  # Concede slots a processos em espera enquanto houver capacidade.
  defp promote(%{active: active, max: max} = state) when map_size(active) < max do
    case :queue.out(state.waiting) do
      {{:value, {{pid, _tag} = from, mref}}, rest} ->
        GenServer.reply(from, mref)
        promote(%{state | waiting: rest, active: Map.put(active, mref, pid)})

      {:empty, _} ->
        state
    end
  end

  defp promote(state), do: state

  defp log(message), do: IO.puts(:stderr, "[CSVProcessor.Semaphore] " <> message)
end

# -----------------------------------------------------------------------------
# Armazenamento: estado em memória + persistência atômica em CSV
# -----------------------------------------------------------------------------
defmodule CSVProcessor.Store do
  @moduledoc false
  # Único processo que escreve no arquivo. Como as operações são serializadas
  # aqui e a persistência é feita por arquivo temporário + rename, um lote
  # nunca fica parcialmente aplicado, nem em memória nem no disco.

  use GenServer

  alias CSVProcessor.CSV

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    columns = ensure_id(Keyword.get(opts, :columns, ["nome", "email"]))
    state = %{path: path, header: columns, records: %{}, next_id: 1}

    case File.read(path) do
      {:ok, content} ->
        case CSV.parse(content) do
          [header | rows] -> load(%{state | header: header}, rows)
          [] -> init_file(state)
        end

      {:error, :enoent} ->
        init_file(state)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:batch, ops}, _from, state) do
    case apply_ops(state, ops) do
      {:ok, records, next_id, ids} ->
        case persist(state, records) do
          :ok ->
            {:reply, {:ok, ids}, %{state | records: records, next_id: next_id}}

          {:error, reason} ->
            # Estado em memória permanece o anterior: nada foi aplicado.
            {:reply, {:error, {:persist_failed, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.records, id) do
        {:ok, rec} -> {:ok, rec}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    {:reply, {:ok, sorted(state.records)}, state}
  end

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  defp init_file(state) do
    case persist(state, %{}) do
      :ok -> {:ok, state}
      {:error, reason} -> {:stop, {:persist_failed, reason}}
    end
  end

  defp load(state, rows) do
    if "id" in state.header do
      records =
        Map.new(rows, fn row ->
          rec = Map.new(Enum.zip(state.header, row))
          {rec["id"], rec}
        end)

      next_id =
        Enum.reduce(Map.keys(records), 1, fn id, m ->
          case Integer.parse(id) do
            {n, ""} -> max(n + 1, m)
            _ -> m
          end
        end)

      {:ok, %{state | records: records, next_id: next_id}}
    else
      {:stop, {:invalid_header, "coluna id ausente"}}
    end
  end

  defp ensure_id(columns) do
    if "id" in columns, do: columns, else: ["id" | columns]
  end

  # ---------------------------------------------------------------------------
  # Aplicação das operações (puras, sobre uma cópia dos registros)
  # ---------------------------------------------------------------------------

  defp apply_ops(state, ops) do
    apply_ops(ops, state.header, state.records, state.next_id, [])
  end

  defp apply_ops([], _header, records, next_id, ids) do
    {:ok, records, next_id, Enum.reverse(ids)}
  end

  defp apply_ops([op | rest], header, records, next_id, ids) do
    case apply_op(op, header, records, next_id) do
      {:ok, records, next_id, new_ids} ->
        apply_ops(rest, header, records, next_id, Enum.reverse(new_ids, ids))

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_op({:insert, attrs}, header, records, next_id) do
    with {:ok, attrs} <- normalize(attrs, header) do
      id = Integer.to_string(next_id)
      {:ok, Map.put(records, id, Map.put(attrs, "id", id)), next_id + 1, [id]}
    end
  end

  defp apply_op({:update, id, changes}, header, records, next_id) do
    with {:ok, old} <- fetch(records, id),
         {:ok, changes} <- normalize(changes, header) do
      updated = Map.merge(old, Map.delete(changes, "id"))
      {:ok, Map.put(records, id, updated), next_id, []}
    end
  end

  defp apply_op({:delete, id}, _header, records, next_id) do
    with {:ok, _old} <- fetch(records, id) do
      {:ok, Map.delete(records, id), next_id, []}
    end
  end

  defp apply_op(op, _header, _records, _next_id) do
    {:error, {:invalid_operation, op}}
  end

  defp fetch(records, id) do
    case Map.fetch(records, id) do
      {:ok, rec} -> {:ok, rec}
      :error -> {:error, {:not_found, id}}
    end
  end

  defp normalize(attrs, header) do
    map = Map.new(attrs, fn {k, v} -> {to_string(k), to_string(v)} end)

    case Map.keys(map) -- ["id" | header] do
      [] -> {:ok, map}
      unknown -> {:error, {:unknown_columns, unknown}}
    end
  end

  # ---------------------------------------------------------------------------
  # Persistência atômica
  # ---------------------------------------------------------------------------

  defp persist(state, records) do
    tmp = state.path <> ".tmp"
    data = to_csv(state.header, records)

    result =
      with {:ok, io} <- File.open(tmp, [:write, :binary]),
           :ok <- IO.binwrite(io, data),
           :ok <- :file.sync(io),
           :ok <- File.close(io),
           :ok <- File.rename(tmp, state.path) do
        :ok
      end

    if result != :ok, do: File.rm(tmp)
    result
  end

  defp to_csv(header, records) do
    rows =
      for rec <- sorted(records) do
        CSV.encode_row(Enum.map(header, &Map.get(rec, &1, "")))
      end

    [CSV.encode_row(header) | rows]
  end

  defp sorted(records) do
    records
    |> Map.values()
    |> Enum.sort_by(&sort_key(&1["id"]))
  end

  # Ordena ids numéricos pelo valor e os demais pelo texto.
  defp sort_key(id) do
    case Integer.parse(id) do
      {n, ""} -> {0, n, id}
      _ -> {1, 0, id}
    end
  end
end

# -----------------------------------------------------------------------------
# Codificação e decodificação CSV (RFC 4180, sem campos com quebra de linha)
# -----------------------------------------------------------------------------
defmodule CSVProcessor.CSV do
  @moduledoc false

  @doc "Converte o conteúdo de um CSV em uma lista de linhas (listas de strings)."
  def parse(content) do
    content
    |> String.split(["\r\n", "\n"], trim: true)
    |> Enum.map(&parse_line/1)
  end

  @doc "Codifica uma linha como iodata terminada em quebra de linha."
  def encode_row(fields) do
    [Enum.map_join(fields, ",", &encode_field/1), "\n"]
  end

  defp encode_field(value) do
    text = to_string(value)

    if String.contains?(text, [",", "\"", "\n", "\r"]) do
      "\"" <> String.replace(text, "\"", "\"\"") <> "\""
    else
      text
    end
  end

  defp parse_line(line), do: parse_line(line, "", [], false)

  defp parse_line(<<>>, acc, fields, _quoted) do
    Enum.reverse([acc | fields])
  end

  # Aspas duplas dentro de campo entre aspas representam uma aspas literal.
  defp parse_line(<<?", ?", rest::binary>>, acc, fields, true) do
    parse_line(rest, acc <> "\"", fields, true)
  end

  # Aspas simples alternam o modo "entre aspas".
  defp parse_line(<<?", rest::binary>>, acc, fields, quoted) do
    parse_line(rest, acc, fields, not quoted)
  end

  # Vírgula fora de aspas separa campos.
  defp parse_line(<<?,, rest::binary>>, acc, fields, false) do
    parse_line(rest, "", [acc | fields], false)
  end

  defp parse_line(<<char, rest::binary>>, acc, fields, quoted) do
    parse_line(rest, acc <> <<char>>, fields, quoted)
  end
end
