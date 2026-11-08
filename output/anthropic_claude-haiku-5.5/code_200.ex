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

# Arquivo único: sistema de gerenciamento de notificações.
#
# Dependências: somente a biblioteca padrão do Elixir/OTP (GenServer, ETS, Stream, File).
#
# Uso básico:
#
#   {:ok, _pid} = NotificationSystem.Repo.start_link(subscriber: self())
#   {:ok, rec} = NotificationSystem.Repo.create(%{title: "Disco cheio", message: "95%",
#                                                  target: "ops", severity: :critical,
#                                                  ttl_seconds: 3600})
#   receive do {:notification, n} -> n end
#   {:ok, _} = NotificationSystem.Importer.import_file("alertas.txt")
#
# Categorias de erro:
#   {:error, {:validation, [{campo, mensagem}]}}  -> entrada inválida (culpa do chamador)
#   {:error, {:operational, motivo}}              -> falha de execução/ambiente
#                                                    (:unavailable, :timeout, :not_found,
#                                                     :expired, {:file, reason}, ...)

defmodule NotificationSystem.Validator do
  # Validação pura de atributos de notificação. Não possui efeitos colaterais.
  # Retorna sempre erros de validação, nunca falhas operacionais.

  @severities [:info, :warning, :critical]
  @max_title 120
  @max_message 2_000
  @max_target 255

  @doc "Valida atributos e retorna o registro normalizado."
  def validate(attrs) when is_map(attrs) do
    errors =
      []
      |> check_text(attrs, :title, @max_title)
      |> check_text(attrs, :message, @max_message)
      |> check_text(attrs, :target, @max_target)
      |> check_severity(attrs)
      |> check_priority(attrs)
      |> check_ttl(attrs)

    case errors do
      [] -> {:ok, build(attrs)}
      _ -> {:error, {:validation, Enum.reverse(errors)}}
    end
  end

  def validate(_attrs), do: {:error, {:validation, [{:record, "deve ser um mapa"}]}}

  # Aceita chaves atômicas ou strings (útil para dados vindos de fontes externas).
  defp fetch(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key))
    end
  end

  defp check_text(errors, attrs, field, max) do
    value = fetch(attrs, field)

    if is_binary(value) and String.trim(value) != "" and byte_size(value) <= max do
      errors
    else
      [{field, "obrigatório, texto não vazio de até #{max} bytes"} | errors]
    end
  end

  defp check_severity(errors, attrs) do
    if to_severity(fetch(attrs, :severity)) in @severities do
      errors
    else
      [{:severity, "deve ser info, warning ou critical"} | errors]
    end
  end

  defp check_priority(errors, attrs) do
    case fetch(attrs, :priority) do
      nil -> errors
      p when is_integer(p) and p >= 0 and p <= 10 -> errors
      _ -> [{:priority, "deve ser inteiro entre 0 e 10"} | errors]
    end
  end

  defp check_ttl(errors, attrs) do
    case fetch(attrs, :ttl_seconds) do
      nil -> errors
      t when is_integer(t) and t > 0 -> errors
      _ -> [{:ttl_seconds, "deve ser inteiro positivo ou omitido"} | errors]
    end
  end

  defp to_severity(value) when is_atom(value), do: value

  defp to_severity(value) when is_binary(value),
    do: Enum.find(@severities, &(Atom.to_string(&1) == value))

  defp to_severity(_), do: nil

  defp build(attrs) do
    %{
      title: fetch(attrs, :title),
      message: fetch(attrs, :message),
      target: fetch(attrs, :target),
      severity: to_severity(fetch(attrs, :severity)),
      priority: fetch(attrs, :priority) || 0,
      ttl_seconds: fetch(attrs, :ttl_seconds)
    }
  end
end

defmodule NotificationSystem.Rules do
  # Regras de condição. Cada regra é um mapa com:
  #   :name       -> identificador da regra
  #   :condition  -> função (registro -> boolean)
  #   :message    -> função (registro -> string) que monta a notificação

  @doc "Conjunto padrão de regras."
  def default do
    [
      %{
        name: :critical_severity,
        condition: fn rec -> rec.severity == :critical end,
        message: fn rec -> "Registro #{rec.id} com severidade crítica: #{rec.title}" end
      },
      %{
        name: :high_priority,
        condition: fn rec -> rec.priority >= 8 end,
        message: fn rec -> "Registro #{rec.id} com prioridade #{rec.priority}" end
      }
    ]
  end

  @doc "Avalia todas as regras sobre um registro e devolve as notificações geradas."
  def evaluate(rules, record) do
    now = System.system_time(:millisecond)

    for rule <- rules, rule.condition.(record) do
      %{
        rule: rule.name,
        record_id: record.id,
        target: record.target,
        message: rule.message.(record),
        at: now
      }
    end
  end
end

defmodule NotificationSystem.Repo do
  # Repositório em memória baseado em ETS.
  #
  # - Escritas passam pelo GenServer (serializadas, com validação e regras).
  # - Leituras acessam a tabela ETS diretamente (tabela :protected, leitura concorrente).
  # - Expiração: cada entrada é {id, expires_at_ms | nil, registro}.
  #   Leituras ignoram registros expirados e um sweeper periódico os remove.
  # - Grandes volumes: `stream/1` percorre a tabela em blocos via continuação do
  #   :ets.select, sem materializar todos os registros.

  use GenServer

  alias NotificationSystem.{Rules, Validator}

  @table __MODULE__
  @default_sweep_ms 60_000
  @default_chunk 500

  ## API pública

  @doc """
  Opções:
    * `:subscriber` - pid que recebe `{:notification, notificação}`
    * `:rules` - lista de regras (padrão: `Rules.default/0`)
    * `:sweep_interval_ms` - intervalo de remoção de expirados (padrão: 60s)
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um registro. `ttl_seconds` opcional define expiração automática."
  def create(attrs), do: call({:create, attrs})

  @doc "Cadastra uma lista de registros; retorna resultado individual por item, na mesma ordem."
  def create_many(list) do
    with :ok <- check_list(list), do: call({:create_many, list})
  end

  @doc "Consulta um registro pelo id. Registros expirados não são retornados."
  def get(id) do
    with :ok <- check_id(id), do: with_table(fn -> fetch_live(id) end)
  end

  @doc "Atualiza campos de um registro. Chaves devem ser átomos."
  def update(id, changes) do
    with :ok <- check_id(id),
         :ok <- check_map(changes),
         do: call({:update, id, changes})
  end

  @doc "Remove um registro."
  def delete(id) do
    with :ok <- check_id(id), do: call({:delete, id})
  end

  @doc """
  Retorna um Stream lazy com os registros ativos. Os dados são lidos em blocos de
  `chunk_size`, então o consumo de memória é limitado ao tamanho do bloco.
  """
  def stream(chunk_size \\ @default_chunk) when is_integer(chunk_size) and chunk_size > 0 do
    match_all = [{{:_, :_, :_}, [], [:"$_"]}]

    Stream.resource(
      fn -> :ets.select(@table, match_all, chunk_size) end,
      fn
        :"$end_of_table" -> {:halt, :done}
        {objects, continuation} -> {live_records(objects, now_ms()), :ets.select(continuation)}
      end,
      fn _ -> :ok end
    )
  end

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])

    state = %{
      sweep_ms: Keyword.get(opts, :sweep_interval_ms, @default_sweep_ms),
      rules: Keyword.get(opts, :rules, Rules.default()),
      subscriber: Keyword.get(opts, :subscriber)
    }

    schedule_sweep(state.sweep_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    {:reply, do_create(attrs, state), state}
  end

  def handle_call({:create_many, list}, _from, state) do
    {:reply, {:ok, Enum.map(list, &do_create(&1, state))}, state}
  end

  def handle_call({:update, id, changes}, _from, state) do
    {:reply, do_update(id, changes, state), state}
  end

  def handle_call({:delete, id}, _from, state) do
    {:reply, do_delete(id), state}
  end

  @impl true
  def handle_info(:sweep, state) do
    # Remoção de expirados feita inteiramente dentro do ETS.
    now = now_ms()

    match_spec = [
      {{:_, :"$1", :_}, [{:andalso, {:is_integer, :"$1"}, {:"=<", :"$1", now}}], [true]}
    ]

    :ets.select_delete(@table, match_spec)
    schedule_sweep(state.sweep_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Operações internas (executadas dentro do GenServer)

  defp do_create(attrs, state) do
    case Validator.validate(attrs) do
      {:error, _} = error ->
        error

      {:ok, data} ->
        now = now_ms()
        id = System.unique_integer([:positive, :monotonic])
        expires_at = expiry(data.ttl_seconds, now)

        record =
          Map.merge(data, %{
            id: id,
            inserted_at: now,
            updated_at: now,
            expires_at: expires_at
          })

        :ets.insert(@table, {id, expires_at, record})
        dispatch(state, Rules.evaluate(state.rules, record))
        {:ok, record}
    end
  end

  defp do_update(id, changes, state) do
    with {:ok, current} <- fetch_live(id),
         {:ok, data} <- Validator.validate(Map.merge(current, changes)) do
      now = now_ms()

      # Recalcula a expiração somente se o TTL foi informado na atualização.
      expires_at =
        if Map.has_key?(changes, :ttl_seconds),
          do: expiry(data.ttl_seconds, now),
          else: current.expires_at

      record =
        Map.merge(data, %{
          id: id,
          inserted_at: current.inserted_at,
          updated_at: now,
          expires_at: expires_at
        })

      :ets.insert(@table, {id, expires_at, record})
      dispatch(state, Rules.evaluate(state.rules, record))
      {:ok, record}
    end
  end

  defp do_delete(id) do
    case :ets.take(@table, id) do
      [] ->
        {:error, {:operational, :not_found}}

      [{^id, expires_at, _record}] ->
        if expired?(expires_at, now_ms()),
          do: {:error, {:operational, :expired}},
          else: :ok
    end
  end

  defp fetch_live(id) do
    case :ets.lookup(@table, id) do
      [{^id, expires_at, record}] ->
        if expired?(expires_at, now_ms()),
          do: {:error, {:operational, :expired}},
          else: {:ok, record}

      [] ->
        {:error, {:operational, :not_found}}
    end
  end

  defp dispatch(%{subscriber: nil}, _notifications), do: :ok

  defp dispatch(%{subscriber: pid}, notifications) do
    Enum.each(notifications, &send(pid, {:notification, &1}))
  end

  ## Helpers

  defp call(message) do
    GenServer.call(__MODULE__, message)
  catch
    :exit, {:timeout, _} -> {:error, {:operational, :timeout}}
    :exit, _ -> {:error, {:operational, :unavailable}}
  end

  # Leitura direta na ETS: se o repositório não existir, a tabela não estará disponível.
  defp with_table(fun) do
    fun.()
  rescue
    ArgumentError -> {:error, {:operational, :unavailable}}
  end

  defp live_records(objects, now) do
    for {_id, expires_at, record} <- objects, not expired?(expires_at, now), do: record
  end

  defp expired?(nil, _now), do: false
  defp expired?(expires_at, now), do: expires_at <= now

  defp expiry(nil, _now), do: nil
  defp expiry(seconds, now), do: now + seconds * 1_000

  defp schedule_sweep(ms), do: Process.send_after(self(), :sweep, ms)

  defp now_ms, do: System.system_time(:millisecond)

  defp check_id(id) when is_integer(id), do: :ok
  defp check_id(_), do: {:error, {:validation, [{:id, "deve ser um inteiro"}]}}

  defp check_map(map) when is_map(map), do: :ok
  defp check_map(_), do: {:error, {:validation, [{:changes, "deve ser um mapa"}]}}

  defp check_list(list) when is_list(list), do: :ok
  defp check_list(_), do: {:error, {:validation, [{:records, "deve ser uma lista"}]}}
end

defmodule NotificationSystem.Importer do
  # Importação de arquivos grandes de forma preguiçosa (streaming).
  #
  # Formato por linha (campos separados por ';'):
  #   título;mensagem;destino;severidade[;prioridade[;ttl_segundos]]
  #
  # O arquivo é lido linha a linha, processado em lotes e descartado em seguida.
  # Apenas o lote atual e um número limitado de erros ficam em memória.

  alias NotificationSystem.Repo

  @default_batch 500
  @max_reported_errors 50

  @doc """
  Importa um arquivo. Retorna:
    * `{:ok, %{inserted: n, rejected: n, errors: [{linha, motivo}]}}`
    * `{:error, {:operational, motivo}}` em falhas de arquivo ou do repositório
  """
  def import_file(path, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch)

    try do
      result =
        path
        |> File.stream!()
        |> Stream.with_index(1)
        |> Stream.map(&parse_line/1)
        |> Stream.chunk_every(batch_size)
        |> Enum.reduce_while({:ok, empty_stats()}, fn batch, {:ok, acc} ->
          case process_batch(batch, acc) do
            {:ok, acc} -> {:cont, {:ok, acc}}
            {:error, _} = error -> {:halt, error}
          end
        end)

      case result do
        {:ok, stats} -> {:ok, %{stats | errors: Enum.reverse(stats.errors)}}
        error -> error
      end
    rescue
      e in File.Error -> {:error, {:operational, {:file, e.reason}}}
    end
  end

  defp parse_line({line, n}) do
    case String.split(String.trim(line), ";") do
      [title, message, target, severity | rest] ->
        attrs = %{
          title: title,
          message: message,
          target: target,
          severity: severity,
          priority: parse_int(Enum.at(rest, 0)),
          ttl_seconds: parse_int(Enum.at(rest, 1))
        }

        {n, {:ok, attrs}}

      _ ->
        reason = {:validation, [{:line, "formato: título;mensagem;destino;severidade[;prioridade[;ttl]]"}]}
        {n, {:error, reason}}
    end
  end

  defp parse_int(nil), do: nil
  defp parse_int(""), do: nil

  defp parse_int(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      # Mantém o texto original para que o Validator reporte o erro.
      _ -> value
    end
  end

  defp process_batch(batch, stats) do
    {parsed, invalid} = Enum.split_with(batch, fn {_n, result} -> match?({:ok, _}, result) end)

    stats =
      Enum.reduce(invalid, stats, fn {n, {:error, reason}}, acc ->
        record_rejection(acc, n, reason)
      end)

    numbered = Enum.map(parsed, fn {n, {:ok, attrs}} -> {n, attrs} end)
    {numbers, attrs_list} = Enum.unzip(numbered)

    case Repo.create_many(attrs_list) do
      {:ok, results} ->
        stats =
          numbers
          |> Enum.zip(results)
          |> Enum.reduce(stats, fn
            {_n, {:ok, _record}}, acc -> %{acc | inserted: acc.inserted + 1}
            {n, {:error, reason}}, acc -> record_rejection(acc, n, reason)
          end)

        {:ok, stats}

      {:error, _} = error ->
        error
    end
  end

  defp record_rejection(stats, n, reason) do
    errors =
      if stats.rejected < @max_reported_errors,
        do: [{n, reason} | stats.errors],
        else: stats.errors

    %{stats | rejected: stats.rejected + 1, errors: errors}
  end

  defp empty_stats, do: %{inserted: 0, rejected: 0, errors: []}
end
