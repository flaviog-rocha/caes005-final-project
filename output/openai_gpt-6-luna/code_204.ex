# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageExchange do
  use GenServer

  @table :message_exchange_store
  @end_of_table :"$end_of_table"

  # Inicie com start_link(path: "messages.dets", deliver: callback).
  # O callback recebe um mapa de mensagem e deve retornar :ok em caso de sucesso.
  # Qualquer outro retorno ou exceção será tratado como falha temporária.
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def send_message(server \\ __MODULE__, from, to, body) do
    GenServer.call(server, {:send_message, from, to, body})
  end

  # Retorna um Stream; os registros são lidos do DETS um por vez.
  # Opções aceitas: :to e :status.
  def stream_messages(server \\ __MODULE__, opts \\ []) do
    table = GenServer.call(server, :table)
    recipient = Keyword.get(opts, :to)
    status = Keyword.get(opts, :status)

    Stream.resource(
      fn -> {:first, table} end,
      fn state -> stream_next(state, recipient, status) end,
      fn _state -> :ok end
    )
  end

  def stop(server \\ __MODULE__) do
    GenServer.stop(server)
  end

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, "messages.dets") |> to_string()
    :ok = File.mkdir_p(Path.dirname(path))

    case :dets.open_file(@table,
           file: String.to_charlist(path),
           type: :set,
           keypos: 2,
           repair: true
         ) do
      {:ok, table} ->
        initialize_counter(table)

        state = %{
          table: table,
          deliver: valid_callback(Keyword.get(opts, :deliver)),
          retry_base_ms: positive_integer(Keyword.get(opts, :retry_base_ms, 1_000), 1_000),
          retry_max_ms: positive_integer(Keyword.get(opts, :retry_max_ms, 60_000), 60_000)
        }

        if state.deliver, do: send(self(), :work)
        {:ok, state}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(:table, _from, state) do
    {:reply, state.table, state}
  end

  def handle_call({:send_message, from, to, body}, _from, state) do
    id = :dets.update_counter(state.table, :next_id, {3, 1}, {:meta, :next_id, 0})
    now = System.system_time(:millisecond)

    record =
      {:message, id, from, to, body, :pending, 0, now, now, nil}

    :ok = :dets.insert(state.table, record)
    :ok = :dets.sync(state.table)

    if state.deliver, do: send(self(), :work)
    {:reply, {:ok, id}, state}
  end

  @impl true
  def handle_info(:work, %{deliver: nil} = state), do: {:noreply, state}

  def handle_info(:work, state) do
    now = System.system_time(:millisecond)

    case find_due(state.table, now) do
      nil ->
        case earliest_pending(state.table) do
          nil ->
            {:noreply, state}

          next_at ->
            delay = max(next_at - now, 0)
            Process.send_after(self(), :work, delay)
            {:noreply, state}
        end

      record ->
        process_record(record, state)
        send(self(), :work)
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :dets.sync(state.table)
    :dets.close(state.table)
  end

  defp process_record(
         {:message, id, from, to, body, :pending, attempts, next_at, inserted_at, _last_error},
         state
       ) do
    message = %{
      id: id,
      from: from,
      to: to,
      body: body,
      attempts: attempts,
      inserted_at: inserted_at
    }

    case invoke_callback(state.deliver, message) do
      :ok ->
        updated =
          {:message, id, from, to, body, :delivered, attempts, next_at, inserted_at, nil}

        :ok = :dets.insert(state.table, updated)
        :ok = :dets.sync(state.table)

      {:error, reason} ->
        new_attempts = attempts + 1
        exponent = min(new_attempts - 1, 30)
        delay = min(state.retry_base_ms * Integer.pow(2, exponent), state.retry_max_ms)
        retry_at = System.system_time(:millisecond) + delay
        error = inspect(reason, limit: 5, printable_limit: 200)

        updated =
          {:message, id, from, to, body, :pending, new_attempts, retry_at, inserted_at, error}

        :ok = :dets.insert(state.table, updated)
        :ok = :dets.sync(state.table)
    end
  end

  defp invoke_callback(callback, message) do
    try do
      case callback.(message) do
        :ok -> :ok
        {:ok, _value} -> :ok
        other -> {:error, other}
      end
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp find_due(table, now) do
    find_due_from(table, :dets.first(table), now)
  end

  defp find_due_from(_table, @end_of_table, _now), do: nil

  defp find_due_from(table, key, now) do
    result =
      case :dets.lookup(table, key) do
        [{:message, _id, _from, _to, _body, :pending, _attempts, next_at, _inserted, _error} = record]
        when next_at <= now ->
          record

        _ ->
          nil
      end

    if result do
      result
    else
      find_due_from(table, :dets.next(table, key), now)
    end
  end

  defp earliest_pending(table) do
    earliest_pending_from(table, :dets.first(table), nil)
  end

  defp earliest_pending_from(_table, @end_of_table, earliest), do: earliest

  defp earliest_pending_from(table, key, earliest) do
    next_earliest =
      case :dets.lookup(table, key) do
        [{:message, _id, _from, _to, _body, :pending, _attempts, next_at, _inserted, _error}] ->
          if earliest == nil, do: next_at, else: min(earliest, next_at)

        _ ->
          earliest
      end

    earliest_pending_from(table, :dets.next(table, key), next_earliest)
  end

  defp stream_next({:first, table}, recipient, status) do
    stream_from(table, :dets.first(table), recipient, status)
  end

  defp stream_next({table, key, recipient, status}, _recipient, _status) do
    stream_from(table, key, recipient, status)
  end

  defp stream_from(_table, @end_of_table, _recipient, _status), do: {:halt, nil}

  defp stream_from(table, key, recipient, status) do
    next_key = :dets.next(table, key)

    case :dets.lookup(table, key) do
      [record] when elem(record, 0) == :message ->
        message = record_to_map(record)

        if (recipient == nil or message.to == recipient) and
             (status == nil or message.status == status) do
          {[message], {table, next_key, recipient, status}}
        else
          stream_from(table, next_key, recipient, status)
        end

      _ ->
        stream_from(table, next_key, recipient, status)
    end
  end

  defp record_to_map(
         {:message, id, from, to, body, status, attempts, next_at, inserted_at, last_error}
       ) do
    %{
      id: id,
      from: from,
      to: to,
      body: body,
      status: status,
      attempts: attempts,
      next_at: next_at,
      inserted_at: inserted_at,
      last_error: last_error
    }
  end

  defp initialize_counter(table) do
    case :dets.lookup(table, :next_id) do
      [] ->
        :ok = :dets.insert(table, {:meta, :next_id, 0})
        :ok = :dets.sync(table)

      _ ->
        :ok
    end
  end

  defp valid_callback(callback) when is_function(callback, 1), do: callback
  defp valid_callback(_), do: nil

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default
end
