# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Permitir a execução de operações simultâneas.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentScheduler do
  use GenServer

  @table __MODULE__
  @default_tick_ms 1_000
  @default_retry_base_ms 1_000
  @default_retry_max_ms 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))
  end

  @doc """
  Agenda uma operação periódica para uma entidade.

  A operação deve ser `{modulo, funcao, argumentos}`. Resultados `{:error, motivo}`
  e `{:retry, motivo}`, assim como exceções, são considerados falhas temporárias.
  """
  def schedule(server, entity_id, interval_ms, operation, opts \\ []) do
    GenServer.call(
      server,
      {:schedule, entity_id, interval_ms, operation, opts}
    )
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:path, "appointments.dets")
      |> Path.expand()
      |> String.to_charlist()

    tick_ms = Keyword.get(opts, :tick_ms, @default_tick_ms)

    case :dets.open_file(@table,
           file: path,
           type: :set,
           auto_save: 1_000
         ) do
      {:ok, @table} ->
        recover_interrupted_jobs()
        Process.send_after(self(), :tick, tick_ms)
        {:ok, %{tick_ms: tick_ms, running: %{}}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call(
        {:schedule, entity_id, interval_ms, operation, opts},
        _from,
        state
      ) do
    with :ok <- validate_interval(interval_ms),
         :ok <- validate_operation(operation),
         :ok <- validate_retry_options(opts),
         false <- Map.has_key?(state.running, entity_id) do
      retry_base_ms =
        Keyword.get(opts, :retry_base_ms, @default_retry_base_ms)

      retry_max_ms =
        Keyword.get(opts, :retry_max_ms, @default_retry_max_ms)

      record = %{
        interval_ms: interval_ms,
        operation: operation,
        next_at: now_ms() + interval_ms,
        status: :scheduled,
        attempts: 0,
        retry_base_ms: retry_base_ms,
        retry_max_ms: retry_max_ms
      }

      case :dets.insert(@table, {entity_id, record}) do
        :ok -> {:reply, :ok, state}
        {:error, reason} -> {:reply, {:error, {:storage, reason}}, state}
      end
    else
      true ->
        {:reply, {:error, :entity_running}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    due_entities =
      :dets.foldl(
        fn
          {entity_id, %{status: :scheduled, next_at: next_at}}, acc
          when next_at <= now_ms() ->
            [entity_id | acc]

          _, acc ->
            acc
        end,
        [],
        @table
      )

    state = Enum.reduce(due_entities, state, &dispatch(&1, &2))
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:job_result, entity_id, result}, state) do
    case Map.pop(state.running, entity_id) do
      {nil, _running} ->
        {:noreply, state}

      {ref, running} ->
        Process.demonitor(ref, [:flush])
        finish_job(entity_id, result)
        {:noreply, %{state | running: running}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_entity_id, running_ref} ->
           running_ref == ref
         end) do
      nil ->
        {:noreply, state}

      {entity_id, _running_ref} ->
        finish_job(entity_id, {:failure, {:worker_down, reason}})
        {:noreply, %{state | running: Map.delete(state.running, entity_id)}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp dispatch(entity_id, state) do
    case :dets.lookup(@table, entity_id) do
      [{^entity_id, %{status: :scheduled} = record}] ->
        updated = %{record | status: :running, started_at: now_ms()}
        persist!(entity_id, updated)

        scheduler = self()

        {pid, ref} =
          spawn_monitor(fn ->
            result = execute(updated.operation)
            send(scheduler, {:job_result, entity_id, result})
          end)

        _ = pid
        %{state | running: Map.put(state.running, entity_id, ref)}

      _ ->
        state
    end
  end

  defp execute({module, function, arguments}) do
    try do
      case apply(module, function, arguments) do
        {:error, reason} -> {:failure, reason}
        {:retry, reason} -> {:failure, reason}
        _result -> :success
      end
    rescue
      error -> {:failure, {:exception, error, __STACKTRACE__}}
    catch
      kind, reason -> {:failure, {kind, reason}}
    end
  end

  defp finish_job(entity_id, result) do
    case :dets.lookup(@table, entity_id) do
      [{^entity_id, record}] ->
        updated =
          case result do
            :success ->
              record
              |> Map.put(:status, :scheduled)
              |> Map.put(:attempts, 0)
              |> Map.put(:next_at, now_ms() + record.interval_ms)
              |> Map.delete(:started_at)

            {:failure, _reason} ->
              attempts = record.attempts + 1

              record
              |> Map.put(:status, :scheduled)
              |> Map.put(:attempts, attempts)
              |> Map.put(
                :next_at,
                now_ms() + retry_delay(record, attempts)
              )
              |> Map.delete(:started_at)
          end

        persist!(entity_id, updated)

      [] ->
        :ok
    end
  end

  defp retry_delay(record, attempts) do
    multiplier = Integer.pow(2, min(attempts - 1, 30))

    min(
      record.retry_base_ms * multiplier,
      record.retry_max_ms
    )
  end

  # Cada entidade é armazenada como um único objeto DETS: as transições
  # substituem o registro completo, em vez de atualizar campos separadamente.
  defp persist!(entity_id, record) do
    case :dets.insert(@table, {entity_id, record}) do
      :ok -> :ok
      {:error, reason} -> raise "Falha ao gravar em DETS: #{inspect(reason)}"
    end
  end

  defp recover_interrupted_jobs do
    interrupted =
      :dets.foldl(
        fn
          {entity_id, %{status: :running} = record}, acc ->
            [{entity_id, record} | acc]

          _, acc ->
            acc
        end,
        [],
        @table
      )

    Enum.each(interrupted, fn {entity_id, record} ->
      record =
        record
        |> Map.put(:status, :scheduled)
        |> Map.put(:next_at, now_ms())
        |> Map.delete(:started_at)

      persist!(entity_id, record)
    end)
  end

  defp validate_interval(interval_ms)
       when is_integer(interval_ms) and interval_ms > 0,
       do: :ok

  defp validate_interval(_), do: {:error, :invalid_interval}

  defp validate_operation({module, function, arguments})
       when is_atom(module) and is_atom(function) and is_list(arguments),
       do: :ok

  defp validate_operation(_), do: {:error, :invalid_operation}

  defp validate_retry_options(opts) do
    base = Keyword.get(opts, :retry_base_ms, @default_retry_base_ms)
    max = Keyword.get(opts, :retry_max_ms, @default_retry_max_ms)

    if is_integer(base) and base > 0 and is_integer(max) and max >= base do
      :ok
    else
      {:error, :invalid_retry_options}
    end
  end

  defp now_ms, do: System.system_time(:millisecond)
end
