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
  @default_retry_delay 1_000
  @default_max_retries 3
  @default_max_concurrency 10

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def schedule(server \\ __MODULE__, at, payload) do
    GenServer.call(server, {:schedule, normalize_time(at), payload})
  end

  def cancel(server \\ __MODULE__, id) do
    GenServer.call(server, {:cancel, id})
  end

  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "appointments.bin")

    state = %{
      appointments: load_appointments(path),
      path: path,
      handler: Keyword.get(opts, :handler, fn _payload -> :ok end),
      interval: Keyword.get(opts, :interval, @default_interval),
      retry_delay: Keyword.get(opts, :retry_delay, @default_retry_delay),
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      tasks: %{},
      timer: nil
    }

    {:ok, schedule_tick(state)}
  end

  @impl true
  def handle_call({:schedule, at, payload}, _from, state) do
    id = System.unique_integer([:positive, :monotonic])

    appointment = %{
      id: id,
      at: at,
      payload: payload,
      status: :pending,
      attempts: 0,
      retry_at: nil,
      completed_at: nil
    }

    updated = put_in(state.appointments[id], appointment)

    case persist(updated) do
      :ok -> {:reply, {:ok, id}, updated}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    if Map.has_key?(state.appointments, id) do
      updated = update_in(state.appointments, &Map.delete(&1, id))

      case persist(updated) do
        :ok -> {:reply, :ok, updated}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    appointments =
      state.appointments
      |> Map.values()
      |> Enum.sort_by(&{&1.at, &1.id})

    {:reply, appointments, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      state
      |> Map.put(:timer, nil)
      |> dispatch_due()

    {:noreply, schedule_tick(state)}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {id, tasks} ->
        Process.demonitor(ref, [:flush])
        state = %{state | tasks: tasks}
        {:noreply, settle_attempt(state, id, result)}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{tasks: tasks} = state
      ) do
    case Map.pop(tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {id, remaining_tasks} ->
        state = %{state | tasks: remaining_tasks}
        {:noreply, settle_attempt(state, id, {:error, {:temporary, reason}})}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch_due(state) do
    available = max(state.max_concurrency - map_size(state.tasks), 0)
    now = System.system_time(:millisecond)

    due =
      state.appointments
      |> Map.values()
      |> Enum.filter(fn appointment ->
        appointment.status in [:pending, :retrying] and
          appointment.at <= now and
          (is_nil(appointment.retry_at) or appointment.retry_at <= now)
      end)
      |> Enum.sort_by(&{&1.at, &1.id})
      |> Enum.take(available)

    Enum.reduce(due, state, fn appointment, acc ->
      start_attempt(acc, appointment)
    end)
  end

  defp start_attempt(state, appointment) do
    updated_appointment = %{
      appointment
      | status: :running,
        attempts: appointment.attempts + 1,
        retry_at: nil
    }

    state = put_in(state.appointments[appointment.id], updated_appointment)
    _ = persist(state)

    task =
      Task.async_nolink(fn ->
        invoke_handler(state.handler, updated_appointment)
      end)

    put_in(state.tasks[task.ref], appointment.id)
  end

  defp invoke_handler(handler, appointment) when is_function(handler, 2) do
    handler.(appointment.payload, appointment)
  end

  defp invoke_handler(handler, appointment) when is_function(handler, 1) do
    handler.(appointment.payload)
  end

  defp invoke_handler(_handler, _appointment), do: {:error, :invalid_handler}

  defp settle_attempt(state, id, result) do
    case Map.get(state.appointments, id) do
      nil ->
        state

      appointment ->
        case classify_result(result) do
          :success ->
            updated = %{
              appointment
              | status: :completed,
                completed_at: System.system_time(:millisecond),
                retry_at: nil
            }

            state
            |> put_in([:appointments, id], updated)
            |> persist_state()

          {:temporary, _reason} when appointment.attempts <= state.max_retries ->
            delay = state.retry_delay * Integer.pow(2, min(appointment.attempts - 1, 10))

            updated = %{
              appointment
              | status: :retrying,
                retry_at: System.system_time(:millisecond) + delay
            }

            state
            |> put_in([:appointments, id], updated)
            |> persist_state()

          _failure ->
            updated = %{appointment | status: :failed, retry_at: nil}

            state
            |> put_in([:appointments, id], updated)
            |> persist_state()
        end
    end
  end

  defp classify_result(:ok), do: :success
  defp classify_result({:ok, _value}), do: :success
  defp classify_result({:error, :temporary}), do: {:temporary, :temporary}
  defp classify_result({:error, {:temporary, reason}}), do: {:temporary, reason}
  defp classify_result({:error, reason}), do: {:permanent, reason}
  defp classify_result(_result), do: {:permanent, :invalid_result}

  defp persist_state(state) do
    _ = persist(state)
    state
  end

  defp persist(%{path: nil}), do: :ok

  defp persist(state) do
    directory = Path.dirname(state.path)
    temporary_path = state.path <> ".tmp." <> Integer.to_string(:erlang.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, :erlang.term_to_binary(state.appointments)),
         :ok <- File.rename(temporary_path, state.path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temporary_path)
        {:error, reason}
    end
  end

  defp load_appointments(nil), do: %{}

  defp load_appointments(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          binary
          |> :erlang.binary_to_term([:safe])
          |> Map.new(fn {id, appointment} ->
            appointment =
              if appointment.status == :running do
                %{appointment | status: :pending, retry_at: nil}
              else
                appointment
              end

            {id, appointment}
          end)
        rescue
          _ -> %{}
        end

      {:error, _reason} ->
        %{}
    end
  end

  defp schedule_tick(%{timer: nil} = state) do
    timer = Process.send_after(self(), :tick, state.interval)
    %{state | timer: timer}
  end

  defp schedule_tick(state), do: state

  defp normalize_time(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :millisecond)
  defp normalize_time(time) when is_integer(time), do: time

  defp normalize_time(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_unix(:millisecond)
  end
end
