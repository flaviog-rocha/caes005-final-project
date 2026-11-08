# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Processamento de CSV com controle de concorrência, transações atômicas e
# detecção de encerramentos inesperados de processos.
#
# Requisitos atendidos:
#   1. Limite de operações simultâneas: CsvProcessor.Semaphore
#   2. CRUD de registros: CsvProcessor.Store (create/read/update/delete/list)
#   3. Sem atualizações parcialmente concluídas: transações "tudo ou nada"
#      (validação completa antes de aplicar qualquer alteração)
#   4. Detecção de encerramentos inesperados: monitores em todos os processos
#      que seguram recursos ou transações (rollback automático e log)
#
# Uso básico (em código Elixir):
#   {:ok, store} = CsvProcessor.Store.start_link(name: :store)
#   {:ok, sem}   = CsvProcessor.Semaphore.start_link(4)
#   CsvProcessor.import_csv(store, sem, "dados.csv")

defmodule CsvProcessor do
  @moduledoc """
  Importação de arquivos CSV para o Store, respeitando o limite de
  operações simultâneas e garantindo atomicidade por arquivo.
  """

  alias CsvProcessor.{Semaphore, Store}

  @default_timeout 30_000

  @doc """
  Importa um arquivo CSV (primeira linha = cabeçalho, coluna "id" obrigatória).

  A importação ocorre sob controle do semáforo, em um processo isolado e
  monitorado. Se o processo de importação encerrar de forma inesperada ou
  exceder o timeout, nenhuma linha é gravada (a transação é descartada).

  Retorna `{:ok, quantidade_importada}` ou `{:error, motivo}`.
  """
  def import_csv(store, sem, path, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    Semaphore.run(sem, fn -> isolated(fn -> load(store, path) end, timeout) end)
  end

  # Executa a função em um processo separado, monitorado pelo chamador.
  defp isolated(fun, timeout) do
    parent = self()

    {pid, mref} =
      spawn_monitor(fn ->
        send(parent, {:result, self(), fun.()})
      end)

    receive do
      {:result, ^pid, result} ->
        Process.demonitor(mref, [:flush])
        result

      {:DOWN, ^mref, :process, ^pid, reason} ->
        {:error, {:unexpected_exit, reason}}
    after
      timeout ->
        Process.demonitor(mref, [:flush])
        Process.exit(pid, :kill)
        {:error, :timeout}
    end
  end

  defp load(store, path) do
    lines =
      path
      |> File.stream!()
      |> Stream.map(&String.trim_trailing(&1, "\n"))
      |> Stream.map(&String.trim_trailing(&1, "\r"))
      |> Stream.reject(&(&1 == ""))
      |> Enum.to_list()

    with [header | body] <- lines,
         keys = parse_line(header),
         {:ok, rows} <- build_rows(keys, body) do
      # Todas as linhas entram em uma única transação: ou tudo é gravado, ou nada.
      Store.transaction(store, fn tx ->
        Enum.each(rows, fn {id, attrs} -> :ok = Store.tx_insert(tx, id, attrs) end)
        length(rows)
      end)
    else
      [] -> {:error, :empty_file}
      {:error, _} = error -> error
    end
  end

  defp build_rows(keys, lines) do
    lines
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, []}, fn {line, lineno}, {:ok, acc} ->
      record = keys |> Enum.zip(parse_line(line)) |> Map.new()

      case Map.pop(record, "id") do
        {nil, _} -> {:halt, {:error, {:missing_id, lineno}}}
        {id, attrs} -> {:cont, {:ok, [{id, attrs} | acc]}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  # Parser CSV simples: suporta campos entre aspas, aspas escapadas ("")
  # e vírgulas dentro de campos. Não suporta quebras de linha dentro de campos.
  @doc false
  def parse_line(line), do: parse_fields(line, [], "", false) |> Enum.reverse()

  # 34 = aspas duplas, 44 = vírgula
  defp parse_fields(<<>>, fields, cur, _in_quotes), do: [cur | fields]

  defp parse_fields(<<34, 34, rest::binary>>, fields, cur, true),
    do: parse_fields(rest, fields, cur <> "\"", true)

  defp parse_fields(<<34, rest::binary>>, fields, cur, in_quotes),
    do: parse_fields(rest, fields, cur, not in_quotes)

  defp parse_fields(<<44, rest::binary>>, fields, cur, false),
    do: parse_fields(rest, [cur | fields], "", false)

  defp parse_fields(<<c, rest::binary>>, fields, cur, in_quotes),
    do: parse_fields(rest, fields, cur <> <<c>>, in_quotes)
end

defmodule CsvProcessor.Semaphore do
  @moduledoc """
  Semáforo que limita o número de operações executadas simultaneamente.

  Cada permissão é associada a um monitor do processo que a detém. Se o
  processo encerrar sem liberar a permissão, o monitor (DOWN) a devolve
  automaticamente ao pool.
  """

  use GenServer
  require Logger

  @doc "Inicia o semáforo com `max` operações simultâneas."
  def start_link(max, opts \\ []) when is_integer(max) and max > 0 do
    GenServer.start_link(__MODULE__, max, opts)
  end

  @doc """
  Executa `fun` quando houver permissão disponível e libera a permissão
  ao final, mesmo em caso de exceção.
  """
  def run(sem, fun) when is_function(fun, 0) do
    {:ok, ref} = GenServer.call(sem, :acquire, :infinity)

    try do
      fun.()
    after
      :ok = GenServer.call(sem, {:release, ref}, :infinity)
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, st) do
    # Monitora o solicitante desde o início: se morrer esperando, é removido da fila.
    ref = Process.monitor(pid)

    if map_size(st.active) < st.max do
      {:reply, {:ok, ref}, %{st | active: Map.put(st.active, ref, pid)}}
    else
      {:noreply, %{st | waiting: :queue.in({from, ref}, st.waiting)}}
    end
  end

  def handle_call({:release, ref}, _from, st) do
    Process.demonitor(ref, [:flush])
    st = %{st | active: Map.delete(st.active, ref)}
    {:reply, :ok, grant_next(st)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, st) do
    if Map.has_key?(st.active, ref) do
      log_abnormal(reason, pid)
      st = %{st | active: Map.delete(st.active, ref)}
      {:noreply, grant_next(st)}
    else
      {:noreply, drop_waiting(st, ref)}
    end
  end

  def handle_info(_msg, st), do: {:noreply, st}

  # Concede permissões a quem está na fila enquanto houver vagas.
  defp grant_next(%{max: max, active: active} = st) when map_size(active) < max do
    case :queue.out(st.waiting) do
      {{:value, {from, ref}}, rest} ->
        GenServer.reply(from, {:ok, ref})
        st = %{st | waiting: rest, active: Map.put(active, ref, elem(from, 0))}
        grant_next(st)

      {:empty, _} ->
        st
    end
  end

  defp grant_next(st), do: st

  defp drop_waiting(st, ref) do
    %{st | waiting: :queue.filter(fn {_from, r} -> r != ref end, st.waiting)}
  end

  defp log_abnormal(:normal, _pid), do: :ok

  defp log_abnormal(reason, pid) do
    Logger.warning(
      "Semaphore: processo #{inspect(pid)} encerrou inesperadamente: #{inspect(reason)}. " <>
        "Permissão devolvida ao pool."
    )
  end
end

defmodule CsvProcessor.Store do
  @moduledoc """
  Armazenamento em memória de registros (mapas identificados por "id").

  - Operações avulsas (create/update/delete) são atômicas.
  - Transações agrupam várias operações: são validadas e aplicadas por
    completo no commit, ou descartadas inteiramente (rollback).
  - Se o dono de uma transação encerrar antes do commit, a transação
    é descartada automaticamente (detectado via monitor).
  """

  use GenServer
  require Logger

  # ---------------------------------------------------------------------------
  # API pública - CRUD
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, :ok, opts)

  def create(s, id, attrs), do: GenServer.call(s, {:apply, [{:insert, id, attrs}]})
  def update(s, id, attrs), do: GenServer.call(s, {:apply, [{:update, id, attrs}]})
  def delete(s, id), do: GenServer.call(s, {:apply, [{:delete, id}]})
  def read(s, id), do: GenServer.call(s, {:read, id})
  def list(s), do: GenServer.call(s, :list)

  # ---------------------------------------------------------------------------
  # API pública - Transações
  # ---------------------------------------------------------------------------

  @doc """
  Executa `fun.(tx)` dentro de uma transação. Retorna `{:ok, valor}` se o
  commit for bem-sucedido, ou `{:error, motivo}` se a validação falhar
  (nesse caso, nenhuma alteração é aplicada). Exceções causam rollback
  e são propagadas.
  """
  def transaction(s, fun) when is_function(fun, 1) do
    {:ok, ref} = GenServer.call(s, :begin)
    tx = {s, ref}

    try do
      value = fun.(tx)

      case GenServer.call(s, {:commit, ref}, :infinity) do
        :ok -> {:ok, value}
        {:error, _} = error -> error
      end
    catch
      kind, reason ->
        GenServer.call(s, {:rollback, ref})
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  def tx_insert({s, ref}, id, attrs),
    do: GenServer.call(s, {:stage, ref, {:insert, id, attrs}})

  def tx_update({s, ref}, id, attrs),
    do: GenServer.call(s, {:stage, ref, {:update, id, attrs}})

  def tx_delete({s, ref}, id),
    do: GenServer.call(s, {:stage, ref, {:delete, id}})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(:ok), do: {:ok, %{records: %{}, txs: %{}}}

  @impl true
  def handle_call({:read, id}, _from, st) do
    reply =
      case Map.fetch(st.records, id) do
        {:ok, rec} -> {:ok, rec}
        :error -> {:error, :not_found}
      end

    {:reply, reply, st}
  end

  def handle_call(:list, _from, st) do
    {:reply, Map.to_list(st.records), st}
  end

  # Operação avulsa: validada e aplicada de forma atômica.
  def handle_call({:apply, ops}, _from, st) do
    case apply_ops(st.records, ops) do
      {:ok, records} -> {:reply, :ok, %{st | records: records}}
      {:error, _} = error -> {:reply, error, st}
    end
  end

  def handle_call(:begin, {pid, _}, st) do
    # O monitor serve como identificador da transação e detecta a morte do dono.
    ref = Process.monitor(pid)
    txs = Map.put(st.txs, ref, %{owner: pid, ops: []})
    {:reply, {:ok, ref}, %{st | txs: txs}}
  end

  def handle_call({:stage, ref, op}, {pid, _}, st) do
    case Map.fetch(st.txs, ref) do
      {:ok, %{owner: ^pid} = tx} ->
        txs = Map.put(st.txs, ref, %{tx | ops: [op | tx.ops]})
        {:reply, :ok, %{st | txs: txs}}

      _ ->
        {:reply, {:error, :invalid_tx}, st}
    end
  end

  def handle_call({:commit, ref}, {pid, _}, st) do
    case Map.fetch(st.txs, ref) do
      {:ok, %{owner: ^pid, ops: ops}} ->
        Process.demonitor(ref, [:flush])
        st = %{st | txs: Map.delete(st.txs, ref)}

        case apply_ops(st.records, Enum.reverse(ops)) do
          {:ok, records} -> {:reply, :ok, %{st | records: records}}
          {:error, _} = error -> {:reply, error, st}
        end

      _ ->
        {:reply, {:error, :invalid_tx}, st}
    end
  end

  def handle_call({:rollback, ref}, {pid, _}, st) do
    case Map.fetch(st.txs, ref) do
      {:ok, %{owner: ^pid}} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{st | txs: Map.delete(st.txs, ref)}}

      _ ->
        {:reply, {:error, :invalid_tx}, st}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, st) do
    case Map.pop(st.txs, ref) do
      {nil, _} ->
        {:noreply, st}

      {_tx, txs} ->
        if reason != :normal do
          Logger.warning(
            "Store: dono da transação #{inspect(pid)} encerrou inesperadamente " <>
              "(#{inspect(reason)}). Transação descartada."
          )
        end

        {:noreply, %{st | txs: txs}}
    end
  end

  def handle_info(_msg, st), do: {:noreply, st}

  # ---------------------------------------------------------------------------
  # Lógica interna
  # ---------------------------------------------------------------------------

  # Aplica todas as operações sobre uma cópia dos registros. Se qualquer uma
  # falhar, retorna erro e os registros originais permanecem intactos.
  defp apply_ops(records, ops) do
    Enum.reduce_while(ops, {:ok, records}, fn op, {:ok, acc} ->
      case apply_op(acc, op) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp apply_op(records, {:insert, id, attrs}) do
    if Map.has_key?(records, id),
      do: {:error, {:already_exists, id}},
      else: {:ok, Map.put(records, id, attrs)}
  end

  defp apply_op(records, {:update, id, attrs}) do
    case Map.fetch(records, id) do
      {:ok, old} -> {:ok, Map.put(records, id, Map.merge(old, attrs))}
      :error -> {:error, {:not_found, id}}
    end
  end

  defp apply_op(records, {:delete, id}) do
    if Map.has_key?(records, id),
      do: {:ok, Map.delete(records, id)},
      else: {:error, {:not_found, id}}
  end
end
