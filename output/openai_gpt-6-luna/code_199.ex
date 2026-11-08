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
  use GenServer

  @default_batch_size 500
  @max_batch_size 5_000
  @max_expirations_per_sweep 500
  @expiry_interval_ms 1_000
  @comparison_ops [:gt, :gte, :lt, :lte]

  # Records are maps. An optional :ttl_ms sets their expiration relative to
  # the time they are created or updated.
  def start_link(opts \\ []) do
    {server_opts, manager_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, manager_opts, server_opts)
  end

  def create(server, attrs, opts \\ []), do: call(server, {:create, attrs, opts})
  def get(server, id), do: call(server, {:get, id})
  def update(server, id, attrs, opts \\ []), do: call(server, {:update, id, attrs, opts})
  def delete(server, id), do: call(server, {:delete, id})

  # The stream fetches bounded-size pages as it is consumed; records are not
  # collected into a list in the caller's process.
  def stream(server, batch_size \\ @default_batch_size) do
    paged_stream(server, :records, batch_size)
  end

  def notifications_stream(server, batch_size \\ @default_batch_size) do
    paged_stream(server, :notifications, batch_size)
  end

  # Processes an enumerable one item at a time and stops at the first error.
  def import(server, enumerable, opts \\ []) do
    try do
      enumerable
      |> Enum.reduce_while(0, fn attrs, count ->
        case create(server, attrs, opts) do
          {:ok, _record} -> {:cont, count + 1}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:error, reason} -> {:error, reason}
        count -> {:ok, count}
      end
    rescue
      error -> {:error, {:operational, {:import_failed, error}}}
    catch
      kind, reason -> {:error, {:operational, {:import_failed, kind, reason}}}
    end
  end

  @impl true
  def init(opts) do
    rules = Keyword.get(opts, :rules, [])

    case validate_rules(rules) do
      :ok ->
        records = :ets.new(:notification_manager_records, [:ordered_set, :private])
        expirations = :ets.new(:notification_manager_expirations, [:ordered_set, :private])
        notifications = :ets.new(:notification_manager_notifications, [:ordered_set, :private])
        Process.send_after(self(), :expire, @expiry_interval_ms)

        {:ok,
         %{
           records: records,
           expirations: expirations,
           notifications: notifications,
           rules: rules,
           next_id: 1,
           next_notification_id: 1
         }}

      {:error, reason} ->
        {:stop, {:validation, reason}}
    end
  end

  @impl true
  def handle_call(request, _from, state) do
    state = sweep_expired(state)

    case request do
      {:create, attrs, opts} ->
        case validate_create(attrs, opts) do
          {:ok, ttl_ms} ->
            id = state.next_id
            now = monotonic_ms()
            expires_at = expiration_time(now, ttl_ms)
            record = %{id: id, data: attrs, created_at: system_ms(), expires_at: expires_at}
            :ets.insert(state.records, {id, record})
            add_expiration(state.expirations, id, expires_at)
            state = notify_matching_rules(state, record)

            {:reply, {:ok, record}, %{state | next_id: id + 1}}

          {:error, reason} ->
            {:reply, validation_error(reason), state}
        end

      {:get, id} ->
        case validate_id(id) do
          :ok ->
            reply =
              case :ets.lookup(state.records, id) do
                [{^id, record}] ->
                  if expired?(record) do
                    remove_record(state, record)
                    {:error, {:operational, :not_found}}
                  else
                    {:ok, record}
                  end

                [] ->
                  {:error, {:operational, :not_found}}
              end

            {:reply, reply, state}

          {:error, reason} ->
            {:reply, validation_error(reason), state}
        end

      {:update, id, attrs, opts} ->
        with :ok <- validate_id(id),
             :ok <- validate_attrs(attrs),
             {:ok, ttl_setting} <- validate_update_options(opts) do
          case :ets.lookup(state.records, id) do
            [{^id, old_record}] ->
              if expired?(old_record) do
                remove_record(state, old_record)
                {:reply, {:error, {:operational, :not_found}}, state}
              else
                ttl_ms =
                  case ttl_setting do
                    :unchanged -> remaining_ttl(old_record)
                    value -> value
                  end

                now = monotonic_ms()
                expires_at =
                  case ttl_setting do
                    :unchanged -> old_record.expires_at
                    _ -> expiration_time(now, ttl_ms)
                  end

                record = %{
                  old_record
                  | data: Map.merge(old_record.data, attrs),
                    updated_at: system_ms(),
                    expires_at: expires_at
                }

                remove_expiration(state.expirations, old_record)
                :ets.insert(state.records, {id, record})
                add_expiration(state.expirations, id, expires_at)
                state = notify_matching_rules(state, record)

                {:reply, {:ok, record}, state}
              end

            [] ->
              {:reply, {:error, {:operational, :not_found}}, state}
          end
        else
          {:error, reason} -> {:reply, validation_error(reason), state}
        end

      {:delete, id} ->
        case validate_id(id) do
          :ok ->
            case :ets.lookup(state.records, id) do
              [{^id, record}] ->
                remove_record(state, record)
                {:reply, :ok, state}

              [] ->
                {:reply, {:error, {:operational, :not_found}}, state}
            end

          {:error, reason} ->
            {:reply, validation_error(reason), state}
        end

      {:page, kind, cursor, size} ->
        case validate_page(cursor, size) do
          :ok ->
            {items, next_cursor} = fetch_page(state, kind, cursor, size)
            {:reply, {:ok, items, next_cursor}, state}

          {:error, reason} ->
            {:reply, validation_error(reason), state}
        end
    end
  end

  @impl true
  def handle_info(:expire, state) do
    state = sweep_expired(state)

    delay =
      if more_expired?(state.expirations) do
        0
      else
        @expiry_interval_ms
      end

    Process.send_after(self(), :expire, delay)
    {:noreply, state}
  end

  defp call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, {:server_unavailable, reason}}}
    end
  end

  defp paged_stream(server, kind, batch_size) do
    Stream.resource(
      fn -> {0, false} end,
      fn
        {_cursor, true} ->
          {:halt, nil}

        {cursor, false} ->
          case call(server, {:page, kind, cursor, batch_size}) do
            {:ok, [], _next_cursor} ->
              {:halt, nil}

            {:ok, items, next_cursor} ->
              {items, {next_cursor, false}}

            {:error, reason} ->
              raise "notification manager stream failed: #{inspect(reason)}"
          end
      end,
      fn _ -> :ok end
    )
  end

  defp validate_create(attrs, opts) do
    with :ok <- validate_attrs(attrs),
         {:ok, ttl_ms} <- validate_create_options(opts) do
      {:ok, ttl_ms}
    end
  end

  defp validate_attrs(attrs) when is_map(attrs), do: :ok
  defp validate_attrs(_), do: {:error, :attributes_must_be_a_map}

  defp validate_id(id) when is_integer(id) and id > 0, do: :ok
  defp validate_id(_), do: {:error, :id_must_be_a_positive_integer}

  defp validate_create_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:ttl_ms] == [] do
      validate_ttl(Keyword.get(opts, :ttl_ms))
    else
      {:error, :invalid_options}
    end
  end

  defp validate_create_options(_), do: {:error, :invalid_options}

  defp validate_update_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:ttl_ms] == [] do
      if Keyword.has_key?(opts, :ttl_ms) do
        validate_ttl(Keyword.get(opts, :ttl_ms))
      else
        {:ok, :unchanged}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp validate_update_options(_), do: {:error, :invalid_options}

  defp validate_ttl(nil), do: {:ok, nil}
  defp validate_ttl(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp validate_ttl(_), do: {:error, :ttl_ms_must_be_a_positive_integer_or_nil}

  defp validate_page(cursor, size)
       when is_integer(cursor) and cursor >= 0 and is_integer(size) and size > 0 and
              size <= @max_batch_size,
       do: :ok

  defp validate_page(_, _), do: {:error, :invalid_page_parameters}

  defp validate_rules(rules) when is_list(rules) do
    if Enum.all?(rules, &valid_rule?/1), do: :ok, else: {:error, :invalid_notification_rules}
  end

  defp validate_rules(_), do: {:error, :rules_must_be_a_list}

  defp valid_rule?(%{field: _field, op: :eq, value: _value}), do: true

  defp valid_rule?(%{field: _field, op: op, value: value})
       when op in @comparison_ops and is_number(value),
       do: true

  defp valid_rule?(_), do: false

  defp notify_matching_rules(state, record) do
    Enum.reduce(state.rules, state, fn rule, acc ->
      if rule_matches?(rule, record.data) do
        id = acc.next_notification_id

        notification = %{
          id: id,
          record_id: record.id,
          rule: Map.get(rule, :name, rule.field),
          data: record.data,
          created_at: system_ms()
        }

        :ets.insert(acc.notifications, {id, notification})
        %{acc | next_notification_id: id + 1}
      else
        acc
      end
    end)
  end

  defp rule_matches?(%{field: field, op: op, value: expected}, data) do
    case Map.fetch(data, field) do
      {:ok, actual} when op == :eq ->
        actual === expected

      {:ok, actual} when op in @comparison_ops and is_number(actual) ->
        compare(actual, op, expected)

      _ ->
        false
    end
  end

  defp compare(actual, :gt, expected), do: actual > expected
  defp compare(actual, :gte, expected), do: actual >= expected
  defp compare(actual, :lt, expected), do: actual < expected
  defp compare(actual, :lte, expected), do: actual <= expected

  defp fetch_page(state, :records, cursor, size) do
    fetch_record_page(state, :ets.next(state.records, cursor), size, cursor, [])
  end

  defp fetch_page(state, :notifications, cursor, size) do
    fetch_table_page(state.notifications, :ets.next(state.notifications, cursor), size, cursor, [])
  end

  defp fetch_record_page(_state, :"$end_of_table", _size, cursor, acc) do
    {Enum.reverse(acc), cursor}
  end

  defp fetch_record_page(state, key, size, cursor, acc) do
    [{^key, record}] = :ets.lookup(state.records, key)
    next = :ets.next(state.records, key)

    if expired?(record) do
      remove_record(state, record)
      fetch_record_page(state, next, size, key, acc)
    else
      acc = [record | acc]

      if length(acc) >= size do
        {Enum.reverse(acc), key}
      else
        fetch_record_page(state, next, size, key, acc)
      end
    end
  end

  defp fetch_table_page(_table, :"$end_of_table", _size, cursor, acc) do
    {Enum.reverse(acc), cursor}
  end

  defp fetch_table_page(table, key, size, cursor, acc) do
    [{^key, item}] = :ets.lookup(table, key)
    acc = [item | acc]

    if length(acc) >= size do
      {Enum.reverse(acc), key}
    else
      fetch_table_page(table, :ets.next(table, key), size, key, acc)
    end
  end

  defp sweep_expired(state) do
    sweep_expired(state, monotonic_ms(), @max_expirations_per_sweep)
  end

  defp sweep_expired(state, _now, 0), do: state

  defp sweep_expired(state, now, remaining) do
    case :ets.first(state.expirations) do
      :"$end_of_table" ->
        state

      {expires_at, id} = key when expires_at <= now ->
        :ets.delete(state.expirations, key)

        case :ets.lookup(state.records, id) do
          [{^id, record}] when record.expires_at == expires_at ->
            :ets.delete(state.records, id)

          _ ->
            :ok
        end

        sweep_expired(state, now, remaining - 1)

      _ ->
        state
    end
  end

  defp more_expired?(expirations) do
    case :ets.first(expirations) do
      {expires_at, _id} -> expires_at <= monotonic_ms()
      :"$end_of_table" -> false
    end
  end

  defp remove_record(state, record) do
    :ets.delete(state.records, record.id)
    remove_expiration(state.expirations, record)
    :ok
  end

  defp add_expiration(_table, _id, nil), do: :ok
  defp add_expiration(table, id, expires_at), do: :ets.insert(table, {{expires_at, id}, true})

  defp remove_expiration(_table, %{expires_at: nil}), do: :ok

  defp remove_expiration(table, record) do
    :ets.delete(table, {record.expires_at, record.id})
    :ok
  end

  defp expired?(%{expires_at: nil}), do: false
  defp expired?(%{expires_at: expires_at}), do: expires_at <= monotonic_ms()

  defp expiration_time(_now, nil), do: nil
  defp expiration_time(now, ttl_ms), do: now + ttl_ms

  defp remaining_ttl(%{expires_at: nil}), do: nil
  defp remaining_ttl(%{expires_at: expires_at}), do: max(expires_at - monotonic_ms(), 1)

  defp validation_error(reason), do: {:error, {:validation, reason}}
  defp monotonic_ms, do: System.monotonic_time(:millisecond)
  defp system_ms, do: System.system_time(:millisecond)
end
