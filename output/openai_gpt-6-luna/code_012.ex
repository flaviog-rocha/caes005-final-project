# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentScheduler do
  use GenServer

  @default_interval 1_000
  @default_retry_delay 5_000
  @default_max_retries 3

  def start_link(opts \\ []) do
    server_opts =
      case Keyword.get(opts, :name) do
        nil -> []
        name -> [name: name]
      end

    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  def schedule(server, id, at, payload) do
    GenServer.call(server, {:schedule, id, at, payload})
  end

  def cancel(server, id) do
    GenServer.call(server, {:cancel, id})
  end

  def list(server) do
    GenServer.call(server, :list)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "appointments.term")

    with {:ok, appointments} <- load_appointments(path) do
      {:ok, task_supervisor} = Task.Supervisor.start_link()

      recovered =
        Map.new(appointments, fn
          {id, %{status: :running} = appointment} ->
            {id, %{appointment | status: :scheduled}}

          entry ->
            entry
        end)

      state = %{
        path: path,
        handler: Keyword.get(opts, :handler, fn _payload -> :ok end),
        interval: positive_integer(Keyword.get(opts, :interval, @default_interval)),
        retry_delay: nonnegative_integer(Keyword.get(opts, :retry_delay, @default_retry_delay)),
        max_retries: nonnegative_integer(Keyword.get(opts, :max_retries, @default_max_retries)),
        task_supervisor: task_supervisor,
        tasks: %{},
        appointments: recovered
      }

      if recovered != appointments, do: persist!(state)
      schedule_sweep(state.interval)
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:schedule, id, at, payload}, _from, state) do
    with {:ok, at_ms} <- normalize_time(at),
         false <- Map.has_key?(state.appointments, id) do
      appointment = %{
        id: id,
        at: at_ms,
        next_at: at_ms,
        payload: payload,
        status: :scheduled,
        retries: 0
      }

      updated = %{state | appointments: Map.put(state.appointments, id, appointment)}
      persist!(updated)
      {:reply, :ok, updated}
    else
      true -> {:reply, {:error, :already_exists}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    case Map.get(state.appointments, id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      %{status: :running} ->
        {:reply, {:error, :already_running}, state}

      _appointment ->
        updated = %{state | appointments: Map.delete(state.appointments, id)}
        persist!(updated)
        {:reply, :ok, updated}
    end
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state.appointments), state}
  end

  @impl true
  def handle_info(:sweep, state) do
    now = System.system_time(:millisecond)

    updated =
      Enum.reduce(state.appointments, state, fn {id, appointment}, acc ->
        if appointment.status == :scheduled and appointment.next_at <= now do
          start_appointment(id, appointment, acc)
        else
          acc
        end
      end)

    schedule_sweep(updated.interval)
    {:noreply, updated}
  end

  def handle_info({ref, {:appointment_result, result}}, state) do
    Process.demonitor(ref, [:flush])

    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {id, tasks} ->
        state = %{state | tasks: tasks}

        if successful?(result) do
          {:noreply, complete_appointment(id, state)}
        else
          {:noreply, retry_or_fail(id, result, state)}
        end
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {id, tasks} ->
        state = %{state | tasks: tasks}
        {:noreply, retry_or_fail(id, reason, state)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_appointment(id, appointment, state) do
    handler = state.handler

    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        result =
          try do
            handler.(appointment.payload)
          rescue
            exception -> {:error, Exception.format(:error, exception, __STACKTRACE__)}
          catch
            kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
          end

        {:appointment_result, result}
      end)

    running = %{appointment | status: :running}
    updated = %{state | appointments: Map.put(state.appointments, id, running)}
    updated = %{updated | tasks: Map.put(updated.tasks, task.ref, id)}
    persist!(updated)
    updated
  end

  defp complete_appointment(id, state) do
    case Map.get(state.appointments, id) do
      nil ->
        state

      appointment ->
        completed =
          appointment
          |> Map.put(:status, :completed)
          |> Map.put(:completed_at, System.system_time(:millisecond))

        updated = %{state | appointments: Map.put(state.appointments, id, completed)}
        persist!(updated)
        updated
    end
  end

  defp retry_or_fail(id, reason, state) do
    case Map.get(state.appointments, id) do
      nil ->
        state

      appointment ->
        retries = appointment.retries + 1
        error = inspect(reason)

        if retries <= state.max_retries do
          retried =
            appointment
            |> Map.put(:status, :scheduled)
            |> Map.put(:retries, retries)
            |> Map.put(:next_at, System.system_time(:millisecond) + state.retry_delay)
            |> Map.put(:last_error, error)

          updated = %{state | appointments: Map.put(state.appointments, id, retried)}
          persist!(updated)
          updated
        else
          failed =
            appointment
            |> Map.put(:status, :failed)
            |> Map.put(:retries, retries)
            |> Map.put(:last_error, error)

          updated = %{state | appointments: Map.put(state.appointments, id, failed)}
          persist!(updated)
          updated
        end
    end
  end

  defp successful?(:ok), do: true
  defp successful?({:ok, _value}), do: true
  defp successful?(_result), do: false

  defp normalize_time(%DateTime{} = datetime) do
    {:ok, DateTime.to_unix(datetime, :millisecond)}
  end

  defp normalize_time(timestamp) when is_integer(timestamp), do: {:ok, timestamp}
  defp normalize_time(_), do: {:error, :invalid_time}

  defp load_appointments(path) do
    case File.read(path) do
      {:ok, contents} ->
        try do
          case :erlang.binary_to_term(contents, [:safe]) do
            %{version: 1, appointments: appointments} when is_map(appointments) ->
              {:ok, appointments}

            _ ->
              {:error, :invalid_persistence_file}
          end
        rescue
          _ -> {:error, :invalid_persistence_file}
        end

      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist!(state) do
    directory = Path.dirname(state.path)
    File.mkdir_p!(directory)

    contents =
      :erlang.term_to_binary(%{
        version: 1,
        appointments: state.appointments
      })

    temporary_path = state.path <> ".tmp"
    File.write!(temporary_path, contents)

    case File.rename(temporary_path, state.path) do
      :ok -> :ok
      {:error, reason} -> raise "could not persist appointments: #{inspect(reason)}"
    end
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp positive_integer(value) when is_integer(value) and value > 0, do: value
  defp positive_integer(_), do: @default_interval

  defp nonnegative_integer(value) when is_integer(value) and value >= 0, do: value
  defp nonnegative_integer(_), do: @default_retry_delay
end
