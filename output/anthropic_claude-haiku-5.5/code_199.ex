# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 5. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações em Elixir (arquivo único, apenas biblioteca padrão).
#
# Visão geral:
#   * NotificationSystem        - API pública (fachada).
#   * NotificationSystem.Validator - validação pura de entrada (erros de validação).
#   * NotificationSystem.Store  - GenServer dono de tabelas ETS (armazenamento,
#                                 regras, notificações, expiração e falhas operacionais).
#
# Classificação de erros:
#   {:error, {:validation, [{campo, motivo}]}} - entrada inválida, conflito ou registro inexistente.
#   {:error, {:operational, motivo}}           - falha do sistema (indisponível, timeout, exceção).
#
# Exemplo de uso:
#   {:ok, _} = NotificationSystem.start_link()
#   :ok = NotificationSystem.add_rule(:critico, &(&1.severity == :critical))
#   :ok = NotificationSystem.subscribe()
#   {:ok, _} = NotificationSystem.create(%{id: "r1", title: "CPU", severity: :critical, value: 98.5, ttl_ms: 60_000})
#   receive do {:notification, n} -> n end
#   NotificationSystem.stream_records() |> Enum.take(10)

defmodule NotificationSystem do
  @moduledoc """
  API pública do sistema de gerenciamento de notificações.

  Registros possuem `id`, `title`, `severity` (`:info | :warning | :critical`),
  `value` (numérico) e `ttl_ms` opcional (expiração automática).
  Regras são predicados `fun/1` avaliados a cada escrita; quando retornam `true`,
  uma notificação é gerada e enviada aos assinantes.
  """

  alias NotificationSystem.{Store, Validator}

  # ---------------------------------------------------------------------------
  # Ciclo de vida
  # ---------------------------------------------------------------------------

  @doc "Inicia o sistema. Opções: :sweep_interval_ms, :notification_retention_ms."
  def start_link(opts \\ []), do: Store.start_link(opts)

  # ---------------------------------------------------------------------------
  # CRUD
  # ---------------------------------------------------------------------------

  @doc "Cadastra um registro. Valida a entrada antes de enviar ao armazenamento."
  def create(attrs) do
    with {:ok, fields} <- Validator.validate_create(attrs) do
      Store.call({:create, fields})
    end
  end

  @doc "Consulta um registro vivo (não expirado) pelo id."
  def get(id) do
    with :ok <- Validator.validate_id(id), do: Store.fetch(id)
  end

  @doc "Atualiza campos permitidos de um registro existente."
  def update(id, changes) do
    with :ok <- Validator.validate_id(id),
         {:ok, changes} <- Validator.validate_update(changes) do
      Store.call({:update, id, changes})
    end
  end

  @doc "Remove um registro pelo id."
  def delete(id) do
    with :ok <- Validator.validate_id(id), do: Store.call({:delete, id})
  end

  # ---------------------------------------------------------------------------
  # Regras e notificações
  # ---------------------------------------------------------------------------

  @doc "Registra (ou substitui) uma regra. A função recebe o registro e retorna boolean."
  def add_rule(name, fun) when is_function(fun, 1) do
    Store.call({:add_rule, name, fun})
  end

  @doc "Assina notificações: o processo receberá mensagens `{:notification, evento}`."
  def subscribe(pid \\ self()), do: Store.call({:subscribe, pid})

  @doc """
  Retorna um Stream preguiçoso dos registros vivos.
  Opções: :chunk_size (padrão 500) e :where (filtro `fun/1`).
  Os dados são lidos do ETS em lotes via continuações.
  """
  def stream_records(opts \\ []), do: Store.stream_records(opts)

  @doc "Retorna um Stream preguiçoso das notificações em ordem cronológica."
  def stream_notifications(opts \\ []), do: Store.stream_notifications(opts)

  # ---------------------------------------------------------------------------
  # Importação em massa (sem materializar a entrada inteira)
  # ---------------------------------------------------------------------------

  @doc """
  Importa um enumerable (inclusive Stream de arquivo) em lotes.
  Retorna um resumo com contagens separadas de erros de validação e operacionais.
  Opções: :batch_size (padrão 500).
  """
  def bulk_import(enumerable, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 500)

    summary = %{
      inserted: 0,
      validation_errors: 0,
      operational_errors: 0,
      samples: [],
      last_operational_error: nil
    }

    enumerable
    |> Stream.chunk_every(batch_size)
    |> Enum.reduce(summary, &import_batch/2)
  end

  defp import_batch(batch, acc) do
    results = Enum.map(batch, &Validator.validate_create/1)
    valid = for {:ok, fields} <- results, do: fields
    invalid = for {:error, {:validation, errors}} <- results, do: errors

    # Guarda apenas uma amostra limitada de erros para não crescer em memória.
    acc = %{
      acc
      | validation_errors: acc.validation_errors + length(invalid),
        samples: Enum.take(acc.samples ++ Enum.take(invalid, 10), 10)
    }

    case valid do
      [] -> acc
      _ -> persist_batch(valid, acc)
    end
  end

  defp persist_batch(valid, acc) do
    case Store.call({:bulk_create, valid}) do
      {:ok, %{inserted: inserted, conflicts: conflicts}} ->
        %{
          acc
          | inserted: acc.inserted + inserted,
            validation_errors: acc.validation_errors + conflicts
        }

      {:error, {:operational, reason}} ->
        %{
          acc
          | operational_errors: acc.operational_errors + length(valid),
            last_operational_error: reason
        }
    end
  end
end

# -----------------------------------------------------------------------------
# Validação pura: somente erros de validação são produzidos aqui.
# -----------------------------------------------------------------------------
defmodule NotificationSystem.Validator do
  @moduledoc false

  @severities [:info, :warning, :critical]
  @create_fields [:id, :title, :severity, :value, :ttl_ms]
  @update_fields [:title, :severity, :value]

  @doc "Valida um id (string não vazia)."
  def validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  def validate_id(_), do: {:error, {:validation, [{:id, "deve ser string não vazia"}]}}

  @doc "Valida atributos de criação. ttl_ms é opcional; ausente significa sem expiração."
  def validate_create(attrs) when is_map(attrs) do
    errors =
      List.flatten([
        check(attrs, :id, true, &non_empty_string?/1, "deve ser string não vazia"),
        check(attrs, :title, true, &non_empty_string?/1, "deve ser string não vazia"),
        check(attrs, :severity, true, &(&1 in @severities), "deve ser um de #{inspect(@severities)}"),
        check(attrs, :value, true, &is_number/1, "deve ser numérico"),
        check(attrs, :ttl_ms, false, &(is_integer(&1) and &1 > 0), "deve ser inteiro positivo em ms"),
        unknown_fields(attrs, @create_fields)
      ])

    case errors do
      [] -> {:ok, normalize(attrs)}
      _ -> {:error, {:validation, errors}}
    end
  end

  def validate_create(_), do: {:error, {:validation, [{:record, "deve ser um mapa"}]}}

  @doc "Valida uma atualização parcial."
  def validate_update(changes) when is_map(changes) do
    errors =
      List.flatten([
        check(changes, :title, false, &non_empty_string?/1, "deve ser string não vazia"),
        check(changes, :severity, false, &(&1 in @severities), "deve ser um de #{inspect(@severities)}"),
        check(changes, :value, false, &is_number/1, "deve ser numérico"),
        unknown_fields(changes, @update_fields),
        if(map_size(changes) == 0, do: [{:changes, "nenhum campo informado"}], else: [])
      ])

    case errors do
      [] -> {:ok, Map.take(changes, @update_fields)}
      _ -> {:error, {:validation, errors}}
    end
  end

  def validate_update(_), do: {:error, {:validation, [{:changes, "deve ser um mapa"}]}}

  # -- helpers ----------------------------------------------------------------

  defp check(attrs, field, required?, predicate, message) do
    case Map.fetch(attrs, field) do
      {:ok, value} -> if predicate.(value), do: [], else: [{field, message}]
      :error when required? -> [{field, "é obrigatório"}]
      :error -> []
    end
  end

  defp unknown_fields(map, allowed) do
    map
    |> Map.keys()
    |> Enum.reject(&(&1 in allowed))
    |> Enum.map(&{&1, "campo desconhecido"})
  end

  defp non_empty_string?(value), do: is_binary(value) and byte_size(value) > 0

  defp normalize(attrs) do
    %{
      id: attrs.id,
      title: attrs.title,
      severity: attrs.severity,
      value: attrs.value,
      ttl_ms: Map.get(attrs, :ttl_ms, :infinity)
    }
  end
end

# -----------------------------------------------------------------------------
# Armazenamento: GenServer dono das tabelas ETS. Escritas são serializadas aqui
# (garantindo unicidade e avaliação de regras consistente); leituras e streams
# acessam o ETS diretamente, sem passar pelo processo.
# -----------------------------------------------------------------------------
defmodule NotificationSystem.Store do
  @moduledoc false
  use GenServer

  @records :ns_records
  @events :ns_events
  @timeout 5_000
  @sweep_interval_ms 1_000
  @retention_ms 86_400_000

  @not_found_error {:error, {:validation, [{:id, "não encontrado"}]}}
  @conflict_error {:error, {:validation, [{:id, "já existe"}]}}

  # -- API cliente --------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Chamada síncrona convertendo falhas de processo em erros operacionais."
  def call(msg) do
    GenServer.call(__MODULE__, msg, @timeout)
  catch
    :exit, {:timeout, _} -> {:error, {:operational, :timeout}}
    :exit, {:noproc, _} -> {:error, {:operational, :unavailable}}
    :exit, reason -> {:error, {:operational, {:exit, reason}}}
  end

  @doc "Leitura direta no ETS, ignorando registros expirados."
  def fetch(id) do
    case :ets.lookup(@records, id) do
      [{^id, expires_at, record}] ->
        if alive?(expires_at, now_ms()), do: {:ok, record}, else: @not_found_error

      [] ->
        @not_found_error
    end
  rescue
    ArgumentError -> {:error, {:operational, :unavailable}}
  end

  @doc "Stream dos registros vivos. Filtra expirados dentro do próprio match spec."
  def stream_records(opts \\ []) do
    now = now_ms()

    match_spec = [
      {{:_, :"$1", :"$2"}, [{:orelse, {:==, :"$1", :infinity}, {:>, :"$1", now}}], [:"$2"]}
    ]

    where = Keyword.get(opts, :where, fn _ -> true end)

    @records
    |> paged(match_spec, Keyword.get(opts, :chunk_size, 500))
    |> Stream.filter(where)
  end

  @doc "Stream das notificações em ordem cronológica."
  def stream_notifications(opts \\ []) do
    paged(@events, [{{:_, :"$1"}, [], [:"$1"]}], Keyword.get(opts, :chunk_size, 500))
  end

  # -- GenServer ------------------------------------------------------------------

  @impl true
  def init(opts) do
    :ets.new(@records, [:set, :protected, :named_table, read_concurrency: true])
    :ets.new(@events, [:ordered_set, :protected, :named_table, read_concurrency: true])

    state = %{
      rules: [],
      subscribers: %{},
      sweep_ms: Keyword.get(opts, :sweep_interval_ms, @sweep_interval_ms),
      retention_ms: Keyword.get(opts, :notification_retention_ms, @retention_ms)
    }

    schedule_sweep(state.sweep_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(msg, _from, state) do
    {reply, new_state} = safely(fn -> dispatch(msg, state) end, state)
    {:reply, reply, new_state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = now_ms()

    # Expiração automática: remove registros cujo expires_at já passou.
    :ets.select_delete(@records, [
      {{:_, :"$1", :_}, [{:andalso, {:is_integer, :"$1"}, {:"=<", :"$1", now}}], [true]}
    ])

    # Retenção de notificações antigas.
    purge_events(now - state.retention_ms)

    schedule_sweep(state.sweep_ms)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- despacho de comandos ---------------------------------------------------------

  defp dispatch({:create, fields}, state) do
    now = now_ms()

    if lookup_live(fields.id, now) do
      {@conflict_error, state}
    else
      record = put_record(fields, now)
      notify(record, state)
      {{:ok, record}, state}
    end
  end

  defp dispatch({:update, id, changes}, state) do
    now = now_ms()

    case lookup_live(id, now) do
      nil ->
        {@not_found_error, state}

      old ->
        record = old |> Map.merge(changes) |> Map.put(:updated_at, now)
        :ets.insert(@records, {id, record.expires_at, record})
        notify(record, state)
        {{:ok, record}, state}
    end
  end

  defp dispatch({:delete, id}, state) do
    case lookup_live(id, now_ms()) do
      nil ->
        {@not_found_error, state}

      _ ->
        :ets.delete(@records, id)
        {:ok, state}
    end
  end

  defp dispatch({:bulk_create, batch}, state) do
    now = now_ms()

    counts =
      Enum.reduce(batch, %{inserted: 0, conflicts: 0}, fn fields, acc ->
        if lookup_live(fields.id, now) do
          Map.update!(acc, :conflicts, &(&1 + 1))
        else
          record = put_record(fields, now)
          notify(record, state)
          Map.update!(acc, :inserted, &(&1 + 1))
        end
      end)

    {{:ok, counts}, state}
  end

  defp dispatch({:add_rule, name, fun}, state) do
    {:ok, %{state | rules: List.keystore(state.rules, name, 0, {name, fun})}}
  end

  defp dispatch({:subscribe, pid}, state) do
    ref = Process.monitor(pid)
    {:ok, %{state | subscribers: Map.put(state.subscribers, ref, pid)}}
  end

  defp dispatch(_other, state), do: {{:error, {:operational, :unknown_request}}, state}

  # -- regras e eventos -------------------------------------------------------------

  defp notify(record, state) do
    for {name, fun} <- state.rules, matches?(fun, record) do
      seq = System.unique_integer([:monotonic, :positive])

      event = %{
        id: seq,
        rule: name,
        record_id: record.id,
        severity: record.severity,
        at: record.updated_at,
        message: "regra #{inspect(name)} disparada pelo registro #{record.id}"
      }

      :ets.insert(@events, {{record.updated_at, seq}, event})

      for {_ref, pid} <- state.subscribers, do: send(pid, {:notification, event})
    end

    :ok
  end

  # Erro dentro de uma regra do usuário não derruba o armazenamento.
  defp matches?(fun, record) do
    fun.(record) == true
  rescue
    _ -> false
  end

  defp purge_events(cutoff) do
    case :ets.first(@events) do
      {at, _} = key when at < cutoff ->
        :ets.delete(@events, key)
        purge_events(cutoff)

      _ ->
        :ok
    end
  end

  # -- helpers ------------------------------------------------------------------------

  defp put_record(fields, now) do
    record =
      Map.merge(fields, %{
        inserted_at: now,
        updated_at: now,
        expires_at: expiry(fields.ttl_ms, now)
      })

    :ets.insert(@records, {record.id, record.expires_at, record})
    record
  end

  defp expiry(:infinity, _now), do: :infinity
  defp expiry(ttl, now), do: now + ttl

  defp lookup_live(id, now) do
    case :ets.lookup(@records, id) do
      [{^id, expires_at, record}] -> if alive?(expires_at, now), do: record
      [] -> nil
    end
  end

  defp alive?(:infinity, _now), do: true
  defp alive?(expires_at, now), do: expires_at > now

  # Converte a API de select paginado do ETS em um Stream lazy.
  # Cada lote é carregado sob demanda; nunca a tabela inteira.
  defp paged(table, match_spec, chunk_size) do
    Stream.resource(
      fn -> {:start, match_spec, chunk_size} end,
      fn
        {:start, spec, size} -> page(:ets.select(table, spec, size))
        {:cont, cont} -> page(:ets.select(cont))
      end,
      fn _ -> :ok end
    )
  end

  defp page(:"$end_of_table"), do: {:halt, nil}
  defp page({rows, cont}), do: {rows, {:cont, cont}}

  defp safely(fun, state) do
    fun.()
  rescue
    e -> {{:error, {:operational, {:exception, Exception.message(e)}}}, state}
  catch
    kind, reason -> {{:error, {:operational, {kind, reason}}}, state}
  end

  defp schedule_sweep(ms), do: Process.send_after(self(), :sweep, ms)

  defp now_ms, do: System.system_time(:millisecond)
end
