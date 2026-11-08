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

  @default_tick_ms 1_000
  @default_retry_base_ms 1_000
  @default_max_retry_ms 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def schedule(server, entity, id, due_at, payload, opts \\ []) do
    GenServer.call(server, {:schedule, entity, id, due_at, payload, opts})
  end

  def cancel(server, entity, id) do
    GenServer.call(server, {:cancel, entity, id})
  end

  def list(server) do
    GenServer.call(server, :list)
  end

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table, :appointment_scheduler_dets)
    file = opts |> Keyword.get(:file, "appointments.dets") |> to_charlist()

    case :dets.open_file(table, file: file, type: :set, repair: true) do
      {:ok, table} ->
        {:ok, task_supervisor} = Task.Supervisor.start_link()

        state = %{
          table: table,
          task_supervisor: task_supervisor,
          handler: Keyword.fetch!(opts, :handler),
          tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
          max_retry_ms: Keyword.get(opts, :max_retry_ms, @default_max_retry_ms),
          tasks: %{},
          in_flight_by_entity: %{}
        }

        Process.send_after(self(), :tick, state.tick_ms)
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:schedule, entity, id, due_at, payload, opts}, _from, state) do
    with {:ok, due_at_ms} <- normalize_due_at(due_at),
         {:ok, interval_ms} <- validate_interval(Keyword.get(opts, :interval_ms)),
         {:ok, max_attempts} <- validate_non_negative_integer(Keyword.get(opts, :max_attempts, 5)),
         {:ok, retry_base_ms} <-
           validate_positive_integer(
             Keyword.get(opts, :retry_base_ms, @default_retry_base_ms)
           ) do
      key = {entity, id}
      revision = next_revision(state.table, key)

      appointment = %{
        id: id,
        payload: payload,
        due_at: due_at_ms,
        interval_ms: interval_ms,
        attempts: 0,
        max_attempts: max_attempts,
        retry_base_ms: retry_base_ms,
        status: :scheduled,
        revision: revision
      }

      case persist(state.table, key, appointment) do
        :ok -> {:reply, :ok, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel, entity, id}, _from, state) do
    case :dets.delete(state.table, {entity, id}) do
      :ok ->
        case :dets.sync(state.table) do
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:list, _from, state) do
    appointments =
      :dets.foldl(
        fn {{entity, _id}, appointment}, acc ->
          [%{entity: entity, appointment: appointment} | acc]
        end,
        [],
        state.table
      )

    {:reply, appointments, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = dispatch_due(state)
    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {task_info, tasks} ->
        Process.demonitor(ref, [:flush])
        state = %{state | tasks: tasks}
        state = release_entity(state, task_info.entity, ref)
        state = finish_task(state, task_info, result)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {task_info, tasks} ->
        state = %{state | tasks: tasks}
        state = release_entity(state, task_info.entity, ref)
        state = finish_task(state, task_info, {:error, {:task_exit, reason}})
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end

  defp dispatch_due(state) do
    now = System.system_time(:millisecond)

    :dets.foldl(
      fn {{entity, _id} = key, appointment}, acc ->
        if appointment.status == :scheduled and appointment.due_at <= now and
             not Map.has_key?(acc.in_flight_by_entity, entity) do
          start_task(acc, entity, key, appointment)
        else
          acc
        end
      end,
      state,
      state.table
    )
  end

  defp start_task(state, entity, key, appointment) do
    handler = state.handler

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        try do
          handler.execute(entity, appointment.id, appointment.payload)
        rescue
          exception -> {:error, {:exception, exception, __STACKTRACE__}}
        catch
          kind, reason -> {:error, {kind, reason}}
        end
      end)

    task_info = %{
      entity: entity,
      key: key,
      revision: appointment.revision
    }

    %{
      state
      | tasks: Map.put(state.tasks, task.ref, task_info),
        in_flight_by_entity: Map.put(state.in_flight_by_entity, entity, task.ref)
    }
  end

  defp finish_task(state, %{key: key, revision: revision}, result) do
    case :dets.lookup(state.table, key) do
      [{^key, %{revision: ^revision, status: :scheduled} = appointment}] ->
        complete_appointment(state, key, appointment, result)

      _ ->
        state
    end
  end

  defp complete_appointment(state, key, appointment, result)
       when result in [:ok] do
    if appointment.interval_ms do
      next_appointment = %{
        appointment
        | due_at: System.system_time(:millisecond) + appointment.interval_ms,
          attempts: 0
      }

      persist(state.table, key, next_appointment)
      state
    else
      :dets.delete(state.table, key)
      :dets.sync(state.table)
      state
    end
  end

  defp complete_appointment(state, key, appointment, {:ok, _result}) do
    complete_appointment(state, key, appointment, :ok)
  end

  defp complete_appointment(state, key, appointment, _failure) do
    attempts = appointment.attempts + 1

    cond do
      attempts <= appointment.max_attempts ->
        delay =
          appointment.retry_base_ms
          |> Kernel.*(:math.pow(2, attempts - 1) |> trunc())
          |> min(state.max_retry_ms)

        retry_appointment = %{appointment | attempts: attempts, due_at: System.system_time(:millisecond) + delay}
        persist(state.table, key, retry_appointment)
        state

      appointment.interval_ms ->
        next_appointment = %{
          appointment
          | attempts: 0,
            due_at: System.system_time(:millisecond) + appointment.interval_ms
        }

        persist(state.table, key, next_appointment)
        state

      true ->
        failed_appointment = %{appointment | attempts: attempts, status: :failed}
        persist(state.table, key, failed_appointment)
        state
    end
  end

  defp release_entity(state, entity, ref) do
    if Map.get(state.in_flight_by_entity, entity) == ref do
      %{state | in_flight_by_entity: Map.delete(state.in_flight_by_entity, entity)}
    else
      state
    end
  end

  # Cada compromisso é armazenado como um único objeto DETS, para que uma
  # atualização substitua o registro inteiro em vez de deixá-lo parcialmente atualizado.
  defp persist(table, key, appointment) do
    with :ok <- :dets.insert(table, {key, appointment}),
         :ok <- :dets.sync(table) do
      :ok
    end
  end

  defp next_revision(table, key) do
    case :dets.lookup(table, key) do
      [{^key, appointment}] -> appointment.revision + 1
      [] -> 1
    end
  end

  defp normalize_due_at(%DateTime{} = datetime) do
    {:ok, DateTime.to_unix(datetime, :millisecond)}
  end

  defp normalize_due_at(value) when is_integer(value), do: {:ok, value}
  defp normalize_due_at(_value), do: {:error, :invalid_due_at}

  defp validate_interval(nil), do: {:ok, nil}
  defp validate_interval(value), do: validate_positive_integer(value)

  defp validate_positive_integer(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp validate_positive_integer(_value), do: {:error, :expected_positive_integer}

  defp validate_non_negative_integer(value) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp validate_non_negative_integer(_value), do: {:error, :expected_non_negative_integer}
end
