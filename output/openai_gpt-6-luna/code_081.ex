# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager do
  use GenServer

  @batch_size 128

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def enqueue(server \\ __MODULE__, entity, payload) do
    GenServer.call(server, {:enqueue, entity, payload})
  end

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table, :notification_manager_dets)
    file = opts |> Keyword.get(:file, "notifications.dets") |> to_charlist()
    handler = Keyword.fetch!(opts, :handler)

    case :dets.open_file(table, file: file, type: :set, repair: true) do
      {:ok, ^table} ->
        {:ok, %{table: table, handler: handler, workers: %{}}, {:continue, :resume}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:resume, state) do
    case :dets.select(state.table, all_records_match_spec(), @batch_size) do
      {records, continuation} ->
        state =
          Enum.reduce(records, state, fn {{entity, _id}, _payload, _attempts, _next_at}, acc ->
            ensure_worker(acc, entity)
          end)

        {:noreply, state, {:continue, {:resume, continuation}}}

      :"$end_of_table" ->
        {:noreply, state}
    end
  end

  def handle_continue({:resume, continuation}, state) do
    case :dets.select(continuation) do
      {records, next_continuation} ->
        state =
          Enum.reduce(records, state, fn {{entity, _id}, _payload, _attempts, _next_at}, acc ->
            ensure_worker(acc, entity)
          end)

        {:noreply, state, {:continue, {:resume, next_continuation}}}

      :"$end_of_table" ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:enqueue, entity, payload}, _from, state) do
    id = {System.system_time(:nanosecond), :erlang.unique_integer([:positive, :monotonic])}
    key = {entity, id}
    record = {key, payload, 0, 0}

    case :dets.insert(state.table, record) do
      :ok ->
        {:reply, {:ok, key}, ensure_worker(state, entity)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Enum.find(state.workers, fn {_entity, {_pid, worker_ref}} -> worker_ref == ref end) do
      {entity, _worker} ->
        state = %{state | workers: Map.delete(state.workers, entity)}
        {:noreply, ensure_worker_if_pending(state, entity)}

      nil ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end

  defp ensure_worker(state, entity) do
    case Map.get(state.workers, entity) do
      {pid, _ref} ->
        send(pid, :wake)
        state

      nil ->
        parent = self()
        table = state.table
        handler = state.handler

        {pid, ref} =
          spawn_monitor(fn ->
            worker_loop(parent, table, handler, entity)
          end)

        %{state | workers: Map.put(state.workers, entity, {pid, ref})}
    end
  end

  defp ensure_worker_if_pending(state, entity) do
    if entity_has_records?(state.table, entity) do
      ensure_worker(state, entity)
    else
      state
    end
  end

  defp worker_loop(parent, table, handler, entity) do
    now = System.system_time(:millisecond)

    case :dets.select(table, entity_match_spec(entity, now), @batch_size) do
      {[], _continuation} ->
        idle_or_exit(parent, table, handler, entity)

      {records, _continuation} ->
        Enum.each(records, &process_record(table, handler, entity, &1))
        worker_loop(parent, table, handler, entity)

      :"$end_of_table" ->
        idle_or_exit(parent, table, handler, entity)
    end
  end

  defp idle_or_exit(parent, table, handler, entity) do
    case earliest_next_attempt(table, entity) do
      nil ->
        :ok

      next_at ->
        timeout = max(next_at - System.system_time(:millisecond), 0)

        receive do
          :wake -> worker_loop(parent, table, handler, entity)
        after
          timeout -> worker_loop(parent, table, handler, entity)
        end
    end
  end

  defp process_record(table, handler, entity, {key, payload, attempts, _next_at}) do
    case safely_call(handler, entity, payload) do
      :ok ->
        :dets.delete(table, key)

      {:permanent_error, _reason} ->
        :dets.delete(table, key)

      {:temporary_error, _reason} ->
        next_attempt = attempts + 1
        delay = retry_delay(next_attempt)
        record = {key, payload, next_attempt, System.system_time(:millisecond) + delay}
        :dets.insert(table, record)
    end
  end

  defp safely_call(handler, entity, payload) do
    result =
      try do
        handler.(entity, payload)
      rescue
        _ -> {:error, :handler_exception}
      catch
        _, _ -> {:error, :handler_exception}
      end

    case result do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, reason, :permanent} -> {:permanent_error, reason}
      {:error, reason} -> {:temporary_error, reason}
      other -> {:temporary_error, {:unexpected_result, other}}
    end
  end

  defp retry_delay(attempt) do
    base = 1_000
    cap = 60_000
    multiplier = 1 <<< min(attempt - 1, 20)
    min(base * multiplier, cap)
  end

  defp earliest_next_attempt(table, entity) do
    scan_earliest(table, entity, nil, nil)
  end

  defp scan_earliest(table, entity, continuation, earliest) do
    result =
      case continuation do
        nil -> :dets.select(table, entity_match_spec(entity), @batch_size)
        value -> :dets.select(value)
      end

    case result do
      {records, next_continuation} ->
        earliest =
          Enum.reduce(records, earliest, fn {_key, _payload, _attempts, next_at}, current ->
            if current == nil, do: next_at, else: min(current, next_at)
          end)

        scan_earliest(table, entity, next_continuation, earliest)

      :"$end_of_table" ->
        earliest
    end
  end

  defp entity_has_records?(table, entity) do
    case :dets.select(table, entity_match_spec(entity), 1) do
      {[_record | _], _continuation} -> true
      :"$end_of_table" -> false
    end
  end

  defp entity_match_spec(entity, due_at \\ nil) do
    guards = [{:"=:=", :"$1", {:const, entity}}]

    guards =
      if due_at == nil do
        guards
      else
        guards ++ [{:"=<", :"$4", due_at}]
      end

    {{{:"$1", :"$2"}, :"$3", :"$4", :"$5"}, guards, [:"$_"]}
  end

  defp all_records_match_spec do
    {{:"$1", :"$2", :"$3", :"$4"}, [], [:"$_"]}
  end
end
