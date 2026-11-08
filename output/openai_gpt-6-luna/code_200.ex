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

defmodule NotificationManager do
  @moduledoc """
  Gerenciador persistente de registros e notificações.

  Os registros são armazenados em DETS. `stream/2` retorna um stream de
  itens `{:ok, registro}`; se ocorrer uma falha operacional durante a
  leitura, o stream emite um item `{:error, {:operation, motivo}}`.

  As regras aceitam os operadores `:eq`, `:neq`, `:in`, `:gt`, `:gte`,
  `:lt` e `:lte`. Notificações são enviadas aos processos inscritos como
  `{:notification, notificacao}`.
  """

  use GenServer

  @table :notification_manager_records
  @batch_size 500
  @sweep_interval_ms 60_000

  # A tabela DETS tem nome fixo; execute apenas uma instância por VM.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def create(attrs), do: create(__MODULE__, attrs)

  def create(server, attrs) do
    with :ok <- validate_attrs(attrs, :create) do
      call(server, {:create, attrs})
    end
  end

  def get(id), do: get(__MODULE__, id)
  def get(server, id), do: call(server, {:get, id})

  def update(id, attrs), do: update(__MODULE__, id, attrs)

  def update(server, id, attrs) do
    with :ok <- validate_attrs(attrs, :update) do
      call(server, {:update, id, attrs})
    end
  end

  def remove(id), do: remove(__MODULE__, id)
  def remove(server, id), do: call(server, {:remove, id})

  def subscribe, do: subscribe(__MODULE__)
  def subscribe(server), do: call(server, :subscribe)

  def unsubscribe, do: unsubscribe(__MODULE__)
  def unsubscribe(server), do: call(server, :unsubscribe)

  def stream(batch_size \\ @batch_size), do: stream(__MODULE__, batch_size)

  def stream(server, batch_size) do
    if is_integer(batch_size) and batch_size > 0 and batch_size <= 10_000 do
      stream =
        Stream.resource(
          fn -> nil end,
          fn
            :done ->
              {:halt, :done}

            continuation ->
              case call(server, {:stream_batch, continuation, batch_size}) do
                {:ok, {records, next_continuation}} ->
                  next_state =
                    if next_continuation == :"$end_of_table",
                      do: :done,
                      else: next_continuation

                  items = Enum.map(records, fn {_id, record} -> {:ok, record} end)
                  {items, next_state}

                {:error, {:operation, _reason}} = error ->
                  {[error], :done}

                other ->
                  {[{:error, {:operation, other}}], :done}
              end
          end,
          fn _state -> :ok end
        )

      {:ok, stream}
    else
      {:error, {:validation, :invalid_batch_size}}
    end
  end

  @impl true
  def init(opts) do
    rules = Keyword.get(opts, :rules, [])

    with :ok <- validate_rules(rules),
         path <- Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "notification_manager.dets")),
         :ok <- ensure_parent_directory(path),
         {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             auto_save: 1_000
           ) do
      Process.send_after(self(), :expire_records, @sweep_interval_ms)

      {:ok,
       %{
         rules: rules,
         subscribers: %{},
         monitors: %{},
         sweep_continuation: nil
       }}
    else
      {:error, {:validation, _reason}} = error -> {:stop, error}
      {:error, reason} -> {:stop, {:operation, reason}}
      other -> {:stop, {:operation, other}}
    end
  rescue
    error -> {:stop, {:operation, {error.__struct__, Exception.message(error)}}}
  catch
    kind, reason -> {:stop, {:operation, {kind, reason}}}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    now = System.system_time(:millisecond)
    id = Map.get(attrs, :id) || new_id()

    result =
      guarded(fn ->
        case :dets.lookup(@table, id) do
          [] ->
            record =
              attrs
              |> Map.drop([:ttl_ms, :created_at, :updated_at])
              |> Map.put(:id, id)
              |> Map.put(:created_at, now)
              |> Map.put(:updated_at, now)
              |> Map.put(:expires_at, expiry_from_attrs(attrs, now, nil))

            persist(id, record)
            {:ok, record}

          _ ->
            {:error, {:validation, :id_already_exists}}
        end
      end)

    case result do
      {:ok, record} ->
        notify_subscribers(state, record)
        {:reply, {:ok, record}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    result =
      guarded(fn ->
        case :dets.lookup(@table, id) do
          [{^id, record}] ->
            if expired?(record) do
              :dets.delete(@table, id)
              :dets.sync(@table)
              {:error, :not_found}
            else
              {:ok, record}
            end

          [] ->
            {:error, :not_found}
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    now = System.system_time(:millisecond)

    result =
      guarded(fn ->
        case :dets.lookup(@table, id) do
          [{^id, existing}] ->
            if expired?(existing) do
              :dets.delete(@table, id)
              :dets.sync(@table)
              {:error, :not_found}
            else
              expires_at = expiry_from_attrs(attrs, now, Map.get(existing, :expires_at))

              record =
                existing
                |> Map.merge(Map.drop(attrs, [:ttl_ms, :id, :created_at, :updated_at]))
                |> Map.put(:updated_at, now)
                |> Map.put(:expires_at, expires_at)

              persist(id, record)
              {:ok, record}
            end

          [] ->
            {:error, :not_found}
        end
      end)

    case result do
      {:ok, record} ->
        notify_subscribers(state, record)
        {:reply, {:ok, record}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:remove, id}, _from, state) do
    result =
      guarded(fn ->
        case :dets.lookup(@table, id) do
          [] ->
            {:error, :not_found}

          _ ->
            :dets.delete(@table, id)
            :dets.sync(@table)
            :ok
        end
      end)

    {:reply, result, state}
  end

  def handle_call({:stream_batch, continuation, limit}, _from, state) do
    result =
      guarded(fn ->
        case continuation do
          nil -> :dets.select(@table, match_spec(), limit)
          cont -> :dets.select(cont)
        end
      end)

    case result do
      {:ok, :"$end_of_table"} ->
        {:reply, {:ok, {[], :"$end_of_table"}}, state}

      {:ok, {records, next_continuation}} ->
        {:reply, {:ok, {records, next_continuation}}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:subscribe, {pid, _tag}, state) do
    case Map.fetch(state.subscribers, pid) do
      {:ok, _ref} ->
        {:reply, :ok, state}

      :error ->
        ref = Process.monitor(pid)

        {:reply, :ok,
         %{
           state
           | subscribers: Map.put(state.subscribers, pid, ref),
             monitors: Map.put(state.monitors, ref, pid)
         }}
    end
  end

  def handle_call(:unsubscribe, {pid, _tag}, state) do
    {:reply, :ok, remove_subscriber(state, pid)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    if Map.get(state.monitors, ref) == pid do
      {:noreply, remove_subscriber(state, pid)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:expire_records, state) do
    next_state =
      case expire_batch(state) do
        {:ok, updated_state, more?} ->
          delay = if more?, do: 0, else: @sweep_interval_ms
          Process.send_after(self(), :expire_records, delay)
          updated_state

        {:error, updated_state} ->
          Process.send_after(self(), :expire_records, @sweep_interval_ms)
          %{updated_state | sweep_continuation: nil}
      end

    {:noreply, next_state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  catch
    _, _ -> :ok
  end

  defp validate_attrs(attrs, mode) when is_map(attrs) do
    reserved_invalid? =
      mode == :update and
        (Map.has_key?(attrs, :id) or Map.has_key?(attrs, :created_at))

    has_ttl = Map.has_key?(attrs, :ttl_ms)
    has_expiry = Map.has_key?(attrs, :expires_at)

    cond do
      reserved_invalid? ->
        {:error, {:validation, :reserved_field}}

      has_ttl and has_expiry ->
        {:error, {:validation, :conflicting_expiration_fields}}

      has_ttl and not (is_integer(attrs.ttl_ms) and attrs.ttl_ms > 0) ->
        {:error, {:validation, :invalid_ttl_ms}}

      has_expiry and not (is_nil(attrs.expires_at) or
                            (is_integer(attrs.expires_at) and attrs.expires_at > 0)) ->
        {:error, {:validation, :invalid_expires_at}}

      true ->
        :ok
    end
  end

  defp validate_attrs(_attrs, _mode), do: {:error, {:validation, :attributes_must_be_a_map}}

  defp validate_rules(rules) when is_list(rules) do
    valid_operators = [:eq, :neq, :in, :gt, :gte, :lt, :lte]

    if Enum.all?(rules, fn rule ->
         is_map(rule) and
           Map.has_key?(rule, :field) and
           Map.has_key?(rule, :value) and
           Map.get(rule, :operator) in valid_operators and
           (Map.get(rule, :operator) != :in or is_list(Map.get(rule, :value)))
       end) do
      :ok
    else
      {:error, {:validation, :invalid_notification_rules}}
    end
  end

  defp validate_rules(_), do: {:error, {:validation, :rules_must_be_a_list}}

  defp expiry_from_attrs(attrs, now, existing_expiry) do
    cond do
      Map.has_key?(attrs, :ttl_ms) -> now + Map.fetch!(attrs, :ttl_ms)
      Map.has_key?(attrs, :expires_at) -> Map.fetch!(attrs, :expires_at)
      true -> existing_expiry
    end
  end

  defp expired?(record) do
    case Map.get(record, :expires_at) do
      expiry when is_integer(expiry) -> expiry <= System.system_time(:millisecond)
      _ -> false
    end
  end

  defp persist(id, record) do
    :ok = :dets.insert(@table, {id, record})
    :ok = :dets.sync(@table)
    :ok
  end

  defp notify_subscribers(state, record) do
    now = System.system_time(:millisecond)

    state.rules
    |> Enum.with_index()
    |> Enum.each(fn {rule, index} ->
      if rule_matches?(rule, record) do
        notification = %{
          record_id: record.id,
          rule_id: Map.get(rule, :id, index),
          record: record,
          generated_at: now
        }

        Enum.each(Map.keys(state.subscribers), fn pid ->
          send(pid, {:notification, notification})
        end)
      end
    end)
  end

  defp rule_matches?(rule, record) do
    field_value = Map.get(record, Map.fetch!(rule, :field))
    expected = Map.fetch!(rule, :value)

    case Map.fetch!(rule, :operator) do
      :eq -> field_value == expected
      :neq -> field_value != expected
      :in -> field_value in expected
      :gt -> is_number(field_value) and is_number(expected) and field_value > expected
      :gte -> is_number(field_value) and is_number(expected) and field_value >= expected
      :lt -> is_number(field_value) and is_number(expected) and field_value < expected
      :lte -> is_number(field_value) and is_number(expected) and field_value <= expected
    end
  end

  defp expire_batch(state) do
    guarded(fn ->
      selected =
        case state.sweep_continuation do
          nil -> :dets.select(@table, match_spec(), @batch_size)
          continuation -> :dets.select(continuation)
        end

      case selected do
        :"$end_of_table" ->
          {:complete, nil}

        {records, continuation} ->
          Enum.each(records, fn {id, record} ->
            if expired?(record), do: :dets.delete(@table, id)
          end)

          :dets.sync(@table)

          if continuation == :"$end_of_table" do
            {:complete, nil}
          else
            {:continue, continuation}
          end
      end
    end)
    |> case do
      {:complete, _} ->
        {:ok, %{state | sweep_continuation: nil}, false}

      {:continue, continuation} ->
        {:ok, %{state | sweep_continuation: continuation}, true}

      {:error, _reason} ->
        {:error, state}
    end
  end

  defp match_spec do
    [{{:_, :_}, [], [:"$_"]}]
  end

  defp remove_subscriber(state, pid) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        state

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])

        %{
          state
          | subscribers: subscribers,
            monitors: Map.delete(state.monitors, ref)
        }
    end
  end

  defp ensure_parent_directory(path) do
    case :filelib.ensure_dir(String.to_charlist(path)) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp new_id do
    :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  end

  defp call(server, message) do
    try do
      GenServer.call(server, message)
    catch
      :exit, reason -> {:error, {:operation, reason}}
    end
  end

  defp guarded(fun) do
    fun.()
  rescue
    error -> {:error, {:operation, {error.__struct__, Exception.message(error)}}}
  catch
    kind, reason -> {:error, {:operation, {kind, reason}}}
  end
end
