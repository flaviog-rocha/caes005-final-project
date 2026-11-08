# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentScheduler do
  use GenServer

  @tick_interval 250
  @max_persist_attempts 3

  # Integers for due_at are Unix timestamps in milliseconds.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def schedule(server, id, due_at, details \\ %{}) do
    GenServer.call(server, {:schedule, id, due_at, details})
  end

  def cancel(server, id), do: GenServer.call(server, {:cancel, id})
  def list(server), do: GenServer.call(server, :list)
  def subscribe(server), do: GenServer.call(server, {:subscribe, self()})
  def unsubscribe(server), do: GenServer.call(server, {:unsubscribe, self()})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "appointments.term")

    with {:ok, appointments} <- load(path) do
      # In-progress notifications are retried after a restart.
      appointments =
        Map.new(appointments, fn {id, appointment} ->
          if appointment.status in [:notifying, :retry_wait] do
            {id, %{appointment | status: :pending}}
          else
            {id, appointment}
          end
        end)

      state = %{
        appointments: appointments,
        path: path,
        notifier: Keyword.get(opts, :notifier, fn _appointment -> :ok end),
        max_retries: Keyword.get(opts, :max_retries, 5),
        base_retry_ms: Keyword.get(opts, :base_retry_ms, 500),
        subscribers: MapSet.new(),
        monitors: %{}
      }

      case persist(state.appointments, path) do
        :ok ->
          Process.send_after(self(), :tick, @tick_interval)
          {:ok, state}

        {:error, reason} ->
          {:stop, {:persistence_error, reason}}
      end
    else
      {:error, reason} -> {:stop, {:persistence_error, reason}}
    end
  end

  @impl true
  def handle_call({:schedule, id, due_at, details}, _from, state) do
    with {:ok, timestamp} <- to_timestamp(due_at),
         false <- Map.has_key?(state.appointments, id) do
      appointment = %{
        id: id,
        due_at: timestamp,
        details: details,
        status: :pending,
        attempts: 0
      }

      appointments = Map.put(state.appointments, id, appointment)

      case persist(appointments, state.path) do
        :ok -> {:reply, {:ok, appointment}, %{state | appointments: appointments}}
        {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
      end
    else
      true -> {:reply, {:error, :already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    case Map.fetch(state.appointments, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: :notified}} ->
        {:reply, {:error, :already_notified}, state}

      {:ok, _appointment} ->
        appointments = Map.delete(state.appointments, id)

        case persist(appointments, state.path) do
          :ok -> {:reply, :ok, %{state | appointments: appointments}}
          {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
        end
    end
  end

  def handle_call(:list, _from, state) do
    appointments =
      state.appointments
      |> Map.values()
      |> Enum.sort_by(& &1.due_at)

    {:reply, appointments, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if MapSet.member?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      monitor = Process.monitor(pid)

      {:reply, :ok,
       %{
         state
         | subscribers: MapSet.put(state.subscribers, pid),
           monitors: Map.put(state.monitors, monitor, pid)
       }}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {monitors, removed} =
      Enum.reduce(state.monitors, {state.monitors, false}, fn {monitor, monitored_pid},
                                                             {acc, found} ->
        if monitored_pid == pid do
          Process.demonitor(monitor, [:flush])
          {Map.delete(acc, monitor), true}
        else
          {acc, found}
        end
      end)

    subscribers = MapSet.delete(state.subscribers, pid)
    {:reply, :ok, %{state | subscribers: subscribers, monitors: monitors}}
  end

  @impl true
  def handle_info(:tick, state) do
    now = System.system_time(:millisecond)

    state =
      state.appointments
      |> Enum.filter(fn {_id, appointment} ->
        appointment.status == :pending and appointment.due_at <= now
      end)
      |> Enum.reduce(state, fn {id, _appointment}, acc -> begin_delivery(id, acc, true) end)

    Process.send_after(self(), :tick, @tick_interval)
    {:noreply, state}
  end

  def handle_info({:retry_delivery, id}, state) do
    {:noreply, begin_delivery(id, state, false)}
  end

  def handle_info({:delivery_result, id, result}, state) do
    {:noreply, process_delivery_result(id, result, state)}
  end

  def handle_info({:persist_success, id}, state) do
    case Map.fetch(state.appointments, id) do
      {:ok, %{status: :notifying} = appointment} ->
        updated = %{appointment | status: :notified}
        appointments = Map.put(state.appointments, id, updated)

        case persist(appointments, state.path) do
          :ok -> {:noreply, %{state | appointments: appointments}}
          {:error, _reason} ->
            Process.send_after(self(), {:persist_success, id}, 1_000)
            {:noreply, state}
        end

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_failed_delivery, id, reason}, state) do
    {:noreply, process_delivery_result(id, {:error, reason}, state)}
  end

  def handle_info({:DOWN, monitor, :process, pid, _reason}, state) do
    {:noreply,
     %{
       state
       | subscribers: MapSet.delete(state.subscribers, pid),
         monitors: Map.delete(state.monitors, monitor)
     }}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp begin_delivery(id, state, broadcast?) do
    case Map.fetch(state.appointments, id) do
      {:ok, %{status: status} = appointment} when status in [:pending, :retry_wait] ->
        updated = %{appointment | status: :notifying}
        appointments = Map.put(state.appointments, id, updated)

        case persist(appointments, state.path) do
          :ok ->
            state = %{state | appointments: appointments}

            if broadcast? do
              event = %{event: :appointment_due, appointment: updated}
              Enum.each(state.subscribers, &send(&1, event))
            end

            parent = self()
            notifier = state.notifier

            spawn(fn ->
              result =
                try do
                  case notifier.(updated) do
                    :ok -> :ok
                    {:ok, _value} -> :ok
                    {:error, reason} -> {:error, reason}
                    other -> {:error, {:unexpected_result, other}}
                  end
                rescue
                  error -> {:error, {:exception, error}}
                catch
                  kind, reason -> {:error, {kind, reason}}
                end

              send(parent, {:delivery_result, id, result})
            end)

            state

          {:error, _reason} ->
            Process.send_after(self(), :tick, @tick_interval)
            state
        end

      _ ->
        state
    end
  end

  defp process_delivery_result(id, result, state) do
    case Map.fetch(state.appointments, id) do
      {:ok, %{status: :notifying} = appointment} when result == :ok ->
        updated = %{appointment | status: :notified}
        appointments = Map.put(state.appointments, id, updated)

        case persist(appointments, state.path) do
          :ok -> %{state | appointments: appointments}
          {:error, _reason} ->
            Process.send_after(self(), {:persist_success, id}, 1_000)
            state
        end

      {:ok, %{status: :notifying} = appointment} ->
        if appointment.attempts < state.max_retries do
          attempts = appointment.attempts + 1
          updated = %{appointment | status: :retry_wait, attempts: attempts}
          appointments = Map.put(state.appointments, id, updated)

          case persist(appointments, state.path) do
            :ok ->
              delay =
                min(state.base_retry_ms * trunc(:math.pow(2, attempts - 1)), 60_000)

              Process.send_after(self(), {:retry_delivery, id}, delay)
              %{state | appointments: appointments}

            {:error, _reason} ->
              Process.send_after(self(), {:retry_failed_delivery, id, result}, 1_000)
              state
          end
        else
          updated = %{appointment | status: :failed}
          appointments = Map.put(state.appointments, id, updated)

          case persist(appointments, state.path) do
            :ok -> %{state | appointments: appointments}
            {:error, _reason} ->
              Process.send_after(self(), {:retry_failed_delivery, id, result}, 1_000)
              state
          end
        end

      _ ->
        state
    end
  end

  defp to_timestamp(%DateTime{} = datetime),
    do: {:ok, DateTime.to_unix(datetime, :millisecond)}

  defp to_timestamp(timestamp) when is_integer(timestamp), do: {:ok, timestamp}
  defp to_timestamp(_value), do: {:error, :invalid_due_at}

  defp load(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary, [:safe]) do
            appointments when is_map(appointments) -> {:ok, appointments}
            _ -> {:error, :invalid_persistence_data}
          end
        rescue
          _ -> {:error, :invalid_persistence_data}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist(appointments, path) do
    do_persist(appointments, path, @max_persist_attempts)
  end

  defp do_persist(_appointments, _path, 0), do: {:error, :write_failed}

  defp do_persist(appointments, path, attempts) do
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))
    binary = :erlang.term_to_binary(appointments)

    result =
      with :ok <- File.mkdir_p(directory),
           :ok <- File.write(temporary_path, binary, [:binary, :sync]),
           :ok <- File.rename(temporary_path, path) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        File.rm(temporary_path)

        if attempts > 1 do
          Process.sleep(50)
          do_persist(appointments, path, attempts - 1)
        else
          {:error, reason}
        end
    end
  end
end
