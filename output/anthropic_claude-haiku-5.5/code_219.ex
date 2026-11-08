# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de agendamento de compromissos em Elixir (arquivo único, apenas biblioteca padrão).
#
# Uso básico:
#   {:ok, _sup} = Agenda.start_link(path: "/tmp/agenda.dets")
#   {:ok, appt} = Agenda.create(%{title: "Reunião", starts_at: "2030-01-10T10:00:00Z", ends_at: "2030-01-10T11:00:00Z"})
#   {:ok, appt} = Agenda.fetch(appt.id)
#   {:ok, appt} = Agenda.update(appt.id, %{location: "Sala 2"})
#   {:ok, _}    = Agenda.delete(appt.id)
#   Agenda.operations()   # operações em andamento
#   Agenda.incidents()    # operações interrompidas, abortadas ou com falha

defmodule Agenda.Validator do
  @moduledoc """
  Validação pura dos dados antes de qualquer operação ser executada.
  """

  @fields [:title, :description, :location, :starts_at, :ends_at]
  @string_fields Map.new(@fields, &{Atom.to_string(&1), &1})

  @doc "Valida atributos para criação de um compromisso."
  def validate_create(attrs) do
    with {:ok, attrs} <- normalize(attrs),
         :ok <- require_keys(attrs, [:title, :starts_at, :ends_at]),
         {:ok, valid} <- validate_fields(attrs),
         :ok <- check_period(valid) do
      {:ok, valid}
    end
  end

  @doc "Valida um conjunto parcial de alterações (a consistência do período é checada após a mesclagem)."
  def validate_changes(changes) do
    with {:ok, changes} <- normalize(changes),
         :ok <- non_empty(changes),
         {:ok, valid} <- validate_fields(changes) do
      {:ok, valid}
    end
  end

  @doc "Valida identificadores de registros."
  def validate_id(id) when is_integer(id) and id > 0, do: :ok
  def validate_id(_), do: {:error, {:invalid, :id}}

  @doc "Garante que o término seja posterior ao início."
  def check_period(%{starts_at: starts, ends_at: ends}) do
    if DateTime.compare(starts, ends) == :lt, do: :ok, else: {:error, :invalid_period}
  end

  def check_period(_), do: :ok

  # -- Internos ---------------------------------------------------------------

  defp normalize(attrs) when is_map(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case field_key(key) do
        {:ok, field} -> {:cont, {:ok, Map.put(acc, field, value)}}
        :error -> {:halt, {:error, {:unknown_field, key}}}
      end
    end)
  end

  defp normalize(_), do: {:error, :invalid_attributes}

  defp field_key(key) when key in @fields, do: {:ok, key}
  defp field_key(key) when is_binary(key), do: Map.fetch(@string_fields, key)
  defp field_key(_), do: :error

  defp require_keys(attrs, keys) do
    case Enum.reject(keys, &Map.has_key?(attrs, &1)) do
      [] -> :ok
      missing -> {:error, {:missing_fields, missing}}
    end
  end

  defp non_empty(map) when map_size(map) > 0, do: :ok
  defp non_empty(_), do: {:error, :no_changes}

  defp validate_fields(attrs) do
    Enum.reduce_while(attrs, {:ok, %{}}, fn {field, value}, {:ok, acc} ->
      case validate_field(field, value) do
        {:ok, valid} -> {:cont, {:ok, Map.put(acc, field, valid)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_field(:title, value) when is_binary(value) do
    trimmed = String.trim(value)

    cond do
      trimmed == "" -> {:error, {:invalid, :title, :blank}}
      String.length(trimmed) > 200 -> {:error, {:invalid, :title, :too_long}}
      true -> {:ok, trimmed}
    end
  end

  defp validate_field(:title, _), do: {:error, {:invalid, :title, :not_text}}

  defp validate_field(field, nil) when field in [:description, :location], do: {:ok, nil}

  defp validate_field(field, value) when field in [:description, :location] and is_binary(value) do
    if String.length(value) <= 500,
      do: {:ok, value},
      else: {:error, {:invalid, field, :too_long}}
  end

  defp validate_field(field, _) when field in [:description, :location],
    do: {:error, {:invalid, field, :not_text}}

  defp validate_field(field, value) when field in [:starts_at, :ends_at],
    do: parse_datetime(field, value)

  defp parse_datetime(_field, %DateTime{} = dt), do: {:ok, dt}

  defp parse_datetime(field, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> {:ok, dt}
      {:error, _} -> {:error, {:invalid, field, :bad_format}}
    end
  end

  defp parse_datetime(field, _), do: {:error, {:invalid, field, :not_datetime}}
end

defmodule Agenda.Watchdog do
  @moduledoc """
  Rastreia operações em andamento, monitora os processos envolvidos e
  detecta encerramentos inesperados (do cliente que executa a operação
  e do `Agenda.Store`). A tabela ETS é pública para leitura, então o
  estado pode ser consultado mesmo com o servidor ocupado.
  """
  use GenServer
  require Logger

  @table :agenda_operations

  # -- API --------------------------------------------------------------------

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc false
  def track(action, record_id, owner),
    do: GenServer.call(__MODULE__, {:track, action, record_id, owner})

  @doc false
  def untrack(op_id, outcome), do: GenServer.call(__MODULE__, {:untrack, op_id, outcome})

  @doc false
  def watch_store(pid), do: GenServer.cast(__MODULE__, {:watch_store, pid})

  @doc "Lista as operações em andamento."
  def running, do: Enum.filter(all(), &(&1.status == :running))

  @doc "Lista operações que terminaram de forma anormal."
  def incidents, do: Enum.reject(all(), &(&1.status == :running))

  defp all, do: for({_id, op} <- :ets.tab2list(@table), do: Map.delete(op, :monitor))

  # -- GenServer --------------------------------------------------------------

  @impl true
  def init(_) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    {:ok, %{monitors: %{}, store_ref: nil}}
  end

  @impl true
  def handle_call({:track, action, record_id, owner}, _from, state) do
    op_id = System.unique_integer([:positive, :monotonic])
    ref = Process.monitor(owner)

    op = %{
      id: op_id,
      action: action,
      record_id: record_id,
      owner: owner,
      status: :running,
      reason: nil,
      started_at: DateTime.utc_now(),
      finished_at: nil,
      monitor: ref
    }

    :ets.insert(@table, {op_id, op})
    {:reply, op_id, %{state | monitors: Map.put(state.monitors, ref, op_id)}}
  end

  def handle_call({:untrack, op_id, outcome}, _from, state) do
    case :ets.lookup(@table, op_id) do
      [{^op_id, %{monitor: ref} = op}] ->
        Process.demonitor(ref, [:flush])
        state = %{state | monitors: Map.delete(state.monitors, ref)}

        case {op.status, outcome} do
          {:running, :completed} -> :ets.delete(@table, op_id)
          {:running, {:failed, reason}} -> :ets.insert(@table, {op_id, finish(op, :failed, reason)})
          _ -> :ok
        end

        {:reply, :ok, state}

      [] ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_cast({:watch_store, pid}, state) do
    {:noreply, %{state | store_ref: Process.monitor(pid)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{store_ref: ref} = state) do
    Logger.error("Agenda.Store encerrou inesperadamente: #{inspect(reason)}")
    interrupt_running(reason)
    {:noreply, %{state | store_ref: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {op_id, monitors} ->
        case :ets.lookup(@table, op_id) do
          [{^op_id, %{status: :running} = op}] ->
            Logger.warning(
              "Operação #{op.action} (#{op_id}) abortada: processo cliente encerrou com #{inspect(reason)}"
            )

            :ets.insert(@table, {op_id, finish(op, :aborted, reason)})

          _ ->
            :ok
        end

        {:noreply, %{state | monitors: monitors}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Internos ---------------------------------------------------------------

  defp finish(op, status, reason) do
    Map.merge(op, %{status: status, reason: reason, finished_at: DateTime.utc_now()})
  end

  defp interrupt_running(reason) do
    for {id, %{status: :running} = op} <- :ets.tab2list(@table) do
      :ets.insert(@table, {id, finish(op, :interrupted, reason)})
    end

    :ok
  end
end

defmodule Agenda.Store do
  @moduledoc """
  Armazenamento persistente em :dets com journal de escrita antecipada (WAL).
  Cada mutação é registrada no journal antes de ser aplicada; após uma falha,
  as entradas pendentes são reaplicadas na inicialização (operações idempotentes).
  """
  use GenServer
  require Logger

  alias Agenda.{Validator, Watchdog}

  @table :agenda_store
  @default_file "agenda_store.dets"

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), @default_file))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, table} ->
        replayed = recover(table)

        if replayed > 0,
          do: Logger.warning("#{replayed} operação(ões) pendente(s) reaplicada(s) após falha")

        Watchdog.watch_store(self())
        {:ok, %{table: table}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def terminate(_reason, %{table: table}) do
    :dets.close(table)
  end

  @impl true
  def handle_call({:create, attrs}, _from, %{table: t} = state) do
    id = next_id(t)
    now = DateTime.utc_now()

    record =
      %{description: nil, location: nil}
      |> Map.merge(attrs)
      |> Map.merge(%{id: id, inserted_at: now, updated_at: now})

    :ok = commit(t, {:put, id, record, id + 1})
    {:reply, {:ok, record}, state}
  end

  def handle_call({:fetch, id}, _from, %{table: t} = state) do
    {:reply, lookup(t, id), state}
  end

  def handle_call(:list, _from, %{table: t} = state) do
    records =
      t
      |> :dets.match_object({{:appt, :_}, :_})
      |> Enum.map(fn {_key, record} -> record end)
      |> Enum.sort_by(& &1.starts_at, DateTime)

    {:reply, {:ok, records}, state}
  end

  def handle_call({:update, id, changes}, _from, %{table: t} = state) do
    reply =
      with {:ok, record} <- lookup(t, id) do
        merged = record |> Map.merge(changes) |> Map.put(:updated_at, DateTime.utc_now())

        with :ok <- Validator.check_period(merged) do
          :ok = commit(t, {:put, id, merged, next_id(t)})
          {:ok, merged}
        end
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, %{table: t} = state) do
    reply =
      case lookup(t, id) do
        {:ok, _} -> commit(t, {:delete, id})
        {:error, _} = error -> error
      end

    {:reply, reply, state}
  end

  # -- Persistência e recuperação ---------------------------------------------

  defp commit(t, op) do
    key = {:journal, {System.os_time(:nanosecond), System.unique_integer([:positive, :monotonic])}}
    :ok = :dets.insert(t, {key, op})
    :ok = :dets.sync(t)
    :ok = apply_op(t, op)
    :ok = :dets.delete(t, key)
    :dets.sync(t)
  end

  defp recover(t) do
    pending =
      :dets.foldl(
        fn
          {{:journal, seq}, op}, acc -> [{seq, op} | acc]
          _other, acc -> acc
        end,
        [],
        t
      )
      |> Enum.sort()

    Enum.each(pending, fn {seq, op} ->
      :ok = apply_op(t, op)
      :ok = :dets.delete(t, {:journal, seq})
    end)

    :ok = :dets.sync(t)
    length(pending)
  end

  defp apply_op(t, {:put, id, record, next}) do
    :dets.insert(t, [{{:appt, id}, record}, {:next_id, next}])
  end

  defp apply_op(t, {:delete, id}), do: :dets.delete(t, {:appt, id})

  defp next_id(t) do
    case :dets.lookup(t, :next_id) do
      [{:next_id, n}] -> n
      [] -> 1
    end
  end

  defp lookup(t, id) do
    case :dets.lookup(t, {:appt, id}) do
      [{_, record}] -> {:ok, record}
      [] -> {:error, :not_found}
    end
  end
end

defmodule Agenda do
  @moduledoc """
  API pública do sistema de agendamento.

  Supervisão `:rest_for_one`: o `Agenda.Watchdog` é iniciado antes do
  `Agenda.Store`, de modo que uma falha no watchdog reinicie o store, e
  uma falha no store é reiniciada automaticamente com recuperação dos dados.
  """

  alias Agenda.{Store, Validator, Watchdog}

  @call_timeout 5_000
  @retry_delay 50

  @doc "Inicia a árvore de supervisão do sistema. Opções: `:path` (arquivo :dets)."
  def start_link(opts \\ []) do
    children = [
      {Watchdog, []},
      {Store, opts}
    ]

    Supervisor.start_link(children,
      strategy: :rest_for_one,
      name: Agenda.Supervisor,
      max_restarts: 10,
      max_seconds: 30
    )
  end

  @doc "Cadastra um compromisso."
  def create(attrs) do
    with {:ok, valid} <- Validator.validate_create(attrs) do
      run(:create, nil, {:create, valid})
    end
  end

  @doc "Consulta um compromisso pelo id."
  def fetch(id) do
    with :ok <- Validator.validate_id(id) do
      run(:fetch, id, {:fetch, id})
    end
  end

  @doc "Lista todos os compromissos ordenados pelo início."
  def list do
    run(:list, nil, :list)
  end

  @doc "Atualiza parcialmente um compromisso."
  def update(id, changes) do
    with :ok <- Validator.validate_id(id),
         {:ok, valid} <- Validator.validate_changes(changes) do
      run(:update, id, {:update, id, valid})
    end
  end

  @doc "Remove um compromisso."
  def delete(id) do
    with :ok <- Validator.validate_id(id) do
      run(:delete, id, {:delete, id})
    end
  end

  @doc "Retorna as operações atualmente em andamento."
  def operations, do: Watchdog.running()

  @doc "Retorna operações interrompidas, abortadas ou com falha."
  def incidents, do: Watchdog.incidents()

  # -- Execução rastreada -----------------------------------------------------

  defp run(action, record_id, msg) do
    op_id = Watchdog.track(action, record_id, self())
    result = call_store(msg)

    outcome =
      case result do
        {:error, {:unavailable, reason}} -> {:failed, reason}
        _ -> :completed
      end

    Watchdog.untrack(op_id, outcome)
    result
  end

  # Tenta novamente apenas quando o store ainda não está registrado (ex.: em
  # reinicialização pelo supervisor), pois nessa situação a mensagem não foi processada.
  defp call_store(msg, attempts \\ 3) do
    GenServer.call(Store, msg, @call_timeout)
  catch
    :exit, {:noproc, _} when attempts > 1 ->
      Process.sleep(@retry_delay)
      call_store(msg, attempts - 1)

    :exit, reason ->
      {:error, {:unavailable, reason}}
  end
end
