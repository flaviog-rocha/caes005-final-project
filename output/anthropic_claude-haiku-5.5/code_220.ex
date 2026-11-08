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

# Sistema de agendamento de compromissos em Elixir (arquivo único, apenas biblioteca padrão/OTP).
#
# Visão geral:
#   * Agenda               - API pública (fachada).
#   * Agenda.Supervisor    - Supervisor one_for_one que inicia o Task.Supervisor e o Scheduler.
#   * Agenda.Scheduler     - GenServer que valida, coordena e monitora as operações.
#   * Agenda.Validator     - Validação pura (funcional) dos dados recebidos.
#   * Agenda.Storage       - Persistência em DETS (armazenamento em disco da OTP) e journal de intenções.
#
# Garantias de confiabilidade:
#   1. Validação antes de qualquer operação de escrita ou leitura de identificador.
#   2. Cada escrita é registrada num journal (DETS) antes de ser executada e removida ao concluir.
#      Se o Scheduler cair no meio de uma operação, o journal é reaplicado na próxima inicialização
#      (operações de put/delete são idempotentes).
#   3. O Supervisor reinicia o Scheduler automaticamente após falhas; os dados persistem em DETS.
#   4. Cada escrita roda num worker supervisionado (Task.Supervisor). A falha do worker é detectada
#      via monitor (:DOWN) e registrada; um timer detecta workers travados (timeout).
#   5. É possível consultar as operações em andamento e o histórico recente.
#
# Exemplo de uso (não executado automaticamente):
#   {:ok, _} = Agenda.start_link(data_dir: "/tmp/agenda")
#   {:ok, appt} = Agenda.create(%{"title" => "Reunião", "starts_at" => "2030-01-10T10:00:00Z",
#                                 "ends_at" => "2030-01-10T11:00:00Z"})
#   {:ok, _} = Agenda.update(appt.id, %{title: "Reunião de planejamento"})
#   Agenda.operations()
#   Agenda.status()
#   :ok = Agenda.delete(appt.id)

defmodule Agenda do
  @moduledoc """
  API pública do sistema de agendamento.

  Todas as funções retornam tuplas `{:ok, valor}` ou `{:error, motivo}`.
  Se o serviço estiver indisponível, retorna `{:error, {:unavailable, razao}}`.
  """

  @call_timeout 15_000

  @doc "Inicia o sistema (supervisor + scheduler). Opções: `:data_dir`, `:op_timeout`."
  def start_link(opts \\ []) do
    Agenda.Supervisor.start_link(opts)
  end

  @doc "Cadastra um compromisso. Campos: title, description, location, starts_at, ends_at."
  def create(attrs), do: call({:create, attrs})

  @doc "Consulta um compromisso pelo id."
  def get(id), do: call({:get, id})

  @doc "Lista todos os compromissos ordenados por início."
  def list, do: call(:list)

  @doc "Atualiza campos de um compromisso existente."
  def update(id, attrs), do: call({:update, id, attrs})

  @doc "Remove um compromisso pelo id."
  def delete(id), do: call({:delete, id})

  @doc "Lista as operações de escrita atualmente em andamento."
  def operations, do: call(:operations)

  @doc "Retorna o estado geral: operações em andamento, recuperações na inicialização e histórico recente."
  def status, do: call(:status)

  defp call(message) do
    GenServer.call(Agenda.Scheduler, message, @call_timeout)
  catch
    :exit, reason -> {:error, {:unavailable, reason}}
  end
end

defmodule Agenda.Supervisor do
  @moduledoc false
  # Supervisor responsável pelo Task.Supervisor (workers) e pelo Scheduler.
  # A estratégia one_for_one isola falhas e reinicia apenas o processo que caiu.

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: Agenda.TaskSupervisor},
      {Agenda.Scheduler, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Agenda.Validator do
  @moduledoc false
  # Validação funcional e pura: recebe dados brutos (mapas com chaves atom ou string)
  # e devolve dados normalizados ou um erro descritivo. Nenhum efeito colateral.

  @fields_by_name %{
    "title" => :title,
    "description" => :description,
    "location" => :location,
    "starts_at" => :starts_at,
    "ends_at" => :ends_at
  }
  @fields [:title, :description, :location, :starts_at, :ends_at]
  @required [:title, :starts_at, :ends_at]
  @defaults %{description: nil, location: nil}
  @max_title 200
  @max_text 1_000
  @max_id 100

  @doc "Valida os dados para criação de um compromisso completo."
  def validate_create(attrs) do
    with {:ok, fields} <- cast(attrs),
         {:ok, record} <- finalize(Map.merge(@defaults, fields)) do
      {:ok, record}
    end
  end

  @doc "Valida a atualização parcial de um compromisso existente, verificando o registro resultante."
  def validate_update(existing, attrs) do
    with {:ok, fields} <- cast(attrs),
         :ok <- non_empty(fields),
         {:ok, record} <- finalize(Map.merge(existing, fields)) do
      {:ok, record}
    end
  end

  @doc "Valida um identificador."
  def validate_id(id) when is_binary(id) do
    case String.trim(id) do
      "" -> {:error, {:invalid, [{:id, "não pode ser vazio"}]}}
      trimmed when byte_size(trimmed) > @max_id -> {:error, {:invalid, [{:id, "muito longo"}]}}
      trimmed -> {:ok, trimmed}
    end
  end

  def validate_id(_), do: {:error, {:invalid, [{:id, "deve ser texto"}]}}

  # --- Normalização de chaves e campos ---------------------------------------

  defp cast(attrs) when is_map(attrs) do
    {fields, errors} =
      Enum.reduce(attrs, {%{}, []}, fn {key, value}, {acc, errs} ->
        case normalize_key(key) do
          nil ->
            {acc, [{inspect(key), "campo desconhecido"} | errs]}

          field ->
            case cast_field(field, value) do
              {:ok, casted} -> {Map.put(acc, field, casted), errs}
              {:error, msg} -> {acc, [{field, msg} | errs]}
            end
        end
      end)

    case errors do
      [] -> {:ok, fields}
      _ -> {:error, {:invalid, Enum.reverse(errors)}}
    end
  end

  defp cast(_), do: {:error, {:invalid, [{:attrs, "deve ser um mapa"}]}}

  defp normalize_key(key) when is_binary(key), do: Map.get(@fields_by_name, key)
  defp normalize_key(key) when is_atom(key) and key in @fields, do: key
  defp normalize_key(_), do: nil

  defp cast_field(:title, value) when is_binary(value) do
    trimmed = String.trim(value)

    cond do
      trimmed == "" -> {:error, "não pode ser vazio"}
      String.length(trimmed) > @max_title -> {:error, "excede #{@max_title} caracteres"}
      true -> {:ok, trimmed}
    end
  end

  defp cast_field(f, nil) when f in [:description, :location], do: {:ok, nil}

  defp cast_field(f, value) when f in [:description, :location] and is_binary(value) do
    trimmed = String.trim(value)

    if String.length(trimmed) > @max_text,
      do: {:error, "excede #{@max_text} caracteres"},
      else: {:ok, trimmed}
  end

  defp cast_field(f, value) when f in [:starts_at, :ends_at], do: cast_datetime(value)
  defp cast_field(_field, _value), do: {:error, "valor inválido"}

  defp cast_datetime(%DateTime{} = dt), do: {:ok, dt}

  defp cast_datetime(%NaiveDateTime{} = ndt),
    do: {:ok, DateTime.from_naive!(ndt, "Etc/UTC")}

  defp cast_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> {:ok, dt}
      {:error, _} -> {:error, "data ISO 8601 inválida"}
    end
  end

  defp cast_datetime(_), do: {:error, "data inválida"}

  # --- Regras de consistência do registro --------------------------------------

  defp non_empty(fields) when map_size(fields) == 0,
    do: {:error, {:invalid, [{:attrs, "nenhum campo para atualizar"}]}}

  defp non_empty(_), do: :ok

  defp finalize(record) do
    with :ok <- check_required(record),
         :ok <- check_range(record) do
      {:ok, record}
    end
  end

  defp check_required(record) do
    case Enum.reject(@required, &Map.has_key?(record, &1)) do
      [] -> :ok
      missing -> {:error, {:invalid, Enum.map(missing, &{&1, "obrigatório"})}}
    end
  end

  defp check_range(%{starts_at: starts, ends_at: ends}) do
    if DateTime.compare(ends, starts) == :gt,
      do: :ok,
      else: {:error, {:invalid, [{:ends_at, "deve ser posterior a starts_at"}]}}
  end

  defp check_range(_), do: :ok
end

defmodule Agenda.Storage do
  @moduledoc false
  # Persistência em DETS (stdlib/OTP). Duas tabelas:
  #   * appointments - registros de compromissos (id => registro)
  #   * journal      - intenções de escrita em andamento (op_id => intenção)
  # DETS é fechado automaticamente quando o processo dono (Scheduler) termina,
  # e reaberto na reinicialização, preservando os dados.

  @appointments :agenda_appointments
  @journal :agenda_journal

  @doc "Abre (ou cria) as tabelas no diretório informado."
  def open(dir) do
    with :ok <- File.mkdir_p(dir),
         {:ok, _} <- open_table(@appointments, dir, "appointments.dets"),
         {:ok, _} <- open_table(@journal, dir, "journal.dets") do
      :ok
    else
      {:error, reason} ->
        close()
        {:error, reason}
    end
  end

  defp open_table(name, dir, file) do
    path = dir |> Path.join(file) |> String.to_charlist()
    :dets.open_file(name, file: path, type: :set)
  end

  @doc "Fecha as tabelas."
  def close do
    :dets.close(@appointments)
    :dets.close(@journal)
    :ok
  end

  def get(id) do
    case :dets.lookup(@appointments, id) do
      [{^id, record}] -> {:ok, record}
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, {:storage, reason}}
    end
  end

  def put(id, record) do
    with :ok <- :dets.insert(@appointments, {id, record}), do: :dets.sync(@appointments)
  end

  def delete(id) do
    with :ok <- :dets.delete(@appointments, id), do: :dets.sync(@appointments)
  end

  def all do
    :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], @appointments)
  end

  def journal_put(op_id, entry) do
    with :ok <- :dets.insert(@journal, {op_id, entry}), do: :dets.sync(@journal)
  end

  def journal_delete(op_id) do
    with :ok <- :dets.delete(@journal, op_id), do: :dets.sync(@journal)
  end

  def journal_pending do
    :dets.foldl(fn pair, acc -> [pair | acc] end, [], @journal)
  end
end

defmodule Agenda.Scheduler do
  @moduledoc false
  # GenServer central.
  #   * Valida entradas antes de qualquer operação.
  #   * Escritas são executadas em workers supervisionados (Task.Supervisor), um por vez por compromisso.
  #   * Monitora workers: detecta encerramentos inesperados (:DOWN) e travamentos (timeout).
  #   * Na inicialização, reaplica operações interrompidas registradas no journal.

  use GenServer
  require Logger

  alias Agenda.{Storage, Validator}

  @name __MODULE__
  @task_sup Agenda.TaskSupervisor
  @default_timeout 5_000
  @history_limit 50

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: @name)
  end

  # --- Inicialização e recuperação ------------------------------------------------

  @impl true
  def init(opts) do
    dir = Keyword.get(opts, :data_dir, Path.join(System.tmp_dir!(), "agenda"))
    timeout = Keyword.get(opts, :op_timeout, @default_timeout)

    case Storage.open(dir) do
      :ok ->
        recovered = recover_interrupted()

        {:ok,
         %{
           ops: %{},
           busy: %{},
           history: [],
           recovered: recovered,
           op_timeout: timeout
         }}

      {:error, reason} ->
        {:stop, {:storage_unavailable, reason}}
    end
  end

  @impl true
  def terminate(_reason, _state), do: Storage.close()

  defp recover_interrupted do
    for {op_id, entry} <- Storage.journal_pending() do
      result = execute(entry)
      Storage.journal_delete(op_id)

      Logger.warning(
        "[Agenda] operação #{op_id} (#{entry.kind} #{entry.id}) reaplicada após falha: #{inspect(result)}"
      )

      op_id
    end
  end

  # --- Chamadas síncronas ---------------------------------------------------------

  @impl true
  def handle_call({:create, attrs}, from, state) do
    case Validator.validate_create(attrs) do
      {:ok, record} ->
        id = new_id()
        record = Map.put(record, :id, id)
        dispatch(state, from, :create, id, {:put, record}, {:ok, record})

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      with {:ok, id} <- Validator.validate_id(id), do: Storage.get(id)

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    records = Enum.sort_by(Storage.all(), & &1.starts_at, DateTime)
    {:reply, {:ok, records}, state}
  end

  def handle_call({:update, id, attrs}, from, state) do
    with {:ok, id} <- Validator.validate_id(id),
         {:ok, existing} <- Storage.get(id),
         {:ok, record} <- Validator.validate_update(existing, attrs) do
      dispatch(state, from, :update, id, {:put, record}, {:ok, record})
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:delete, id}, from, state) do
    with {:ok, id} <- Validator.validate_id(id),
         {:ok, _existing} <- Storage.get(id) do
      dispatch(state, from, :delete, id, :delete, :ok)
    else
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call(:operations, _from, state) do
    now = System.monotonic_time(:millisecond)

    ops =
      state.ops
      |> Map.values()
      |> Enum.map(fn op ->
        %{
          op_id: op.op_id,
          kind: op.kind,
          appointment_id: op.id,
          status: :running,
          started_at: op.started_at_utc,
          elapsed_ms: now - op.started_at
        }
      end)
      |> Enum.sort_by(& &1.started_at, DateTime)

    {:reply, ops, state}
  end

  def handle_call(:status, _from, state) do
    status = %{
      in_flight: map_size(state.ops),
      recovered_on_start: state.recovered,
      recent: state.history
    }

    {:reply, status, state}
  end

  # --- Despacho de escritas para workers supervisionados --------------------------

  defp dispatch(state, from, kind, id, action, reply) do
    if Map.has_key?(state.busy, id) do
      # Evita concorrência de escrita sobre o mesmo compromisso.
      {:reply, {:error, :busy}, state}
    else
      op_id = new_id()
      entry = %{kind: kind, id: id, action: action}

      # Registra a intenção antes de executar (write-ahead journal).
      :ok = Storage.journal_put(op_id, entry)

      task = Task.Supervisor.async_nolink(@task_sup, fn -> execute(entry) end)
      timer = Process.send_after(self(), {:op_timeout, task.ref}, state.op_timeout)

      op = %{
        op_id: op_id,
        kind: kind,
        id: id,
        from: from,
        reply: reply,
        task: task,
        timer: timer,
        started_at: System.monotonic_time(:millisecond),
        started_at_utc: DateTime.utc_now()
      }

      {:noreply,
       %{
         state
         | ops: Map.put(state.ops, task.ref, op),
           busy: Map.put(state.busy, id, task.ref)
       }}
    end
  end

  defp execute(%{id: id, action: {:put, record}}), do: Storage.put(id, record)
  defp execute(%{id: id, action: :delete}), do: Storage.delete(id)

  # --- Mensagens assíncronas: resultados, falhas e timeouts -----------------------

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.fetch(state.ops, ref) do
      {:ok, op} ->
        # Worker terminou normalmente: remove o monitor para não receber :DOWN.
        Process.demonitor(ref, [:flush])

        case result do
          :ok ->
            {:noreply, finish(state, op, :completed, op.reply)}

          {:error, reason} ->
            Logger.error("[Agenda] operação #{op.op_id} falhou no armazenamento: #{inspect(reason)}")
            {:noreply, finish(state, op, {:failed, reason}, {:error, {:storage, reason}})}

          other ->
            Logger.error("[Agenda] resultado inesperado na operação #{op.op_id}: #{inspect(other)}")
            {:noreply, finish(state, op, {:failed, other}, {:error, {:unexpected, other}})}
        end

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.fetch(state.ops, ref) do
      {:ok, op} ->
        Logger.error(
          "[Agenda] worker da operação #{op.op_id} (#{op.kind} #{op.id}) encerrou inesperadamente: #{inspect(reason)}"
        )

        {:noreply, finish(state, op, {:crashed, reason}, {:error, {:crashed, reason}})}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:op_timeout, ref}, state) do
    case Map.fetch(state.ops, ref) do
      {:ok, op} ->
        Task.shutdown(op.task, :brutal_kill)

        Logger.error(
          "[Agenda] operação #{op.op_id} (#{op.kind} #{op.id}) excedeu #{state.op_timeout}ms e foi encerrada"
        )

        {:noreply, finish(state, op, :timed_out, {:error, :timeout})}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # --- Finalização de operações ---------------------------------------------------

  defp finish(state, op, outcome, reply) do
    Process.cancel_timer(op.timer)
    Storage.journal_delete(op.op_id)
    GenServer.reply(op.from, reply)

    entry = %{
      op_id: op.op_id,
      kind: op.kind,
      appointment_id: op.id,
      outcome: outcome,
      finished_at: DateTime.utc_now()
    }

    %{
      state
      | ops: Map.delete(state.ops, op.task.ref),
        busy: Map.delete(state.busy, op.id),
        history: Enum.take([entry | state.history], @history_limit)
    }
  end

  defp new_id do
    :crypto.strong_rand_bytes(12) |> Base.url_encode64(padding: false)
  end
end
