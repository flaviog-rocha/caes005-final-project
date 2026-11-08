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

  @table :appointment_scheduler_dets
  @max_timer_delay 86_400_000

  def start_link(opts \\ []) do
    GenServer.start_link(
      __MODULE__,
      opts,
      name: Keyword.get(opts, :name, __MODULE__)
    )
  end

  def create(attrs, server \\ __MODULE__) when is_map(attrs) do
    GenServer.call(server, {:create, attrs})
  end

  def cancel(id, server \\ __MODULE__) do
    GenServer.call(server, {:cancel, id})
  end

  def get(id, server \\ __MODULE__) do
    GenServer.call(server, {:get, id})
  end

  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  def subscribe(server \\ __MODULE__) do
    GenServer.call(server, {:subscribe, self()})
  end

  def unsubscribe(server \\ __MODULE__) do
    GenServer.call(server, {:unsubscribe, self()})
  end

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, "appointments.dets") |> to_charlist()

    case :dets.open_file(@table, file: path, type: :set, repair: true) do
      {:ok, table} ->
        appointments =
          :dets.foldl(
            fn
              {id, appointment}, acc when is_map(appointment) ->
                Map.put(acc, id, appointment)

              _, acc ->
                acc
            end,
            %{},
            table
          )

        state = %{
          table: table,
          appointments: appointments,
          subscribers: %{},
          monitors: %{},
          pending: MapSet.new(),
          timer: nil
        }

        {:ok, schedule_next(state)}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = Map.get(attrs, :id)
    scheduled_at = Map.get(attrs, :scheduled_at)

    cond do
      not is_binary(id) or id == "" ->
        {:reply, {:error, :invalid_id}, state}

      not is_integer(scheduled_at) ->
        {:reply, {:error, :invalid_scheduled_at}, state}

      Map.has_key?(state.appointments, id) ->
        {:reply, {:error, :already_exists}, state}

      true ->
        appointment = %{
          id: id,
          scheduled_at: scheduled_at,
          data: Map.get(attrs, :data, %{}),
          notified: false,
          cancelled: false
        }

        case persist(state.table, id, appointment) do
          :ok ->
            next_state =
              %{state | appointments: Map.put(state.appointments, id, appointment)}
              |> schedule_next()

            {:reply, {:ok, appointment}, next_state}

          {:error, reason} ->
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end
    end
  end

  def handle_call({:cancel, id}, _from, state) do
    case Map.fetch(state.appointments, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{notified: true}} ->
        {:reply, {:error, :already_notified}, state}

      {:ok, %{cancelled: true}} ->
        {:reply, {:error, :already_cancelled}, state}

      {:ok, appointment} ->
        updated = %{appointment | cancelled: true}

        case persist(state.table, id, updated) do
          :ok ->
            next_state =
              %{state | appointments: Map.put(state.appointments, id, updated)}
              |> schedule_next()

            {:reply, :ok, next_state}

          {:error, reason} ->
            {:reply, {:error, {:persistence_failed, reason}}, state}
        end
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.appointments, id) do
        {:ok, appointment} -> {:ok, appointment}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state.appointments), state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)

      next_state = %{
        state
        | subscribers: Map.put(state.subscribers, pid, ref),
          monitors: Map.put(state.monitors, ref, pid)
      }

      {:reply, :ok, next_state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {:reply, :ok, remove_subscriber(state, pid)}
  end

  @impl true
  def handle_info(:tick, state) do
    now = System.system_time(:millisecond)

    due_ids =
      state.appointments
      |> Enum.filter(fn {id, appointment} ->
        appointment.scheduled_at <= now and
          not appointment.notified and
          not appointment.cancelled and
          not MapSet.member?(state.pending, id)
      end)
      |> Enum.map(&elem(&1, 0))

    next_state =
      Enum.reduce(due_ids, state, fn id, acc ->
        deliver_notification(id, acc, 1)
      end)

    {:noreply, schedule_next(next_state)}
  end

  def handle_info({:retry_delivery, id, attempt}, state) do
    next_state =
      state
      |> Map.update!(:pending, &MapSet.delete(&1, id))
      |> deliver_notification(id, attempt)

    {:noreply, schedule_next(next_state)}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.monitors, ref) do
      ^pid -> {:noreply, remove_subscriber(state, pid)}
      _ -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
    :ok
  end

  defp deliver_notification(id, state, attempt) do
    case Map.get(state.appointments, id) do
      %{notified: false, cancelled: false} = appointment ->
        notice = %{appointment | notified: true}

        Enum.each(state.subscribers, fn {pid, _ref} ->
          send(pid, {:appointment_due, notice})
        end)

        case persist(state.table, id, notice) do
          :ok ->
            %{
              state
              | appointments: Map.put(state.appointments, id, notice),
                pending: MapSet.delete(state.pending, id)
            }

          {:error, _reason} ->
            delay = retry_delay(attempt)
            Process.send_after(self(), {:retry_delivery, id, attempt + 1}, delay)

            %{state | pending: MapSet.put(state.pending, id)}
        end

      _ ->
        %{state | pending: MapSet.delete(state.pending, id)}
    end
  end

  defp persist(table, id, appointment) do
    retry(
      fn ->
        case :dets.insert(table, {id, appointment}) do
          :ok -> :dets.sync(table)
          {:error, reason} -> {:error, reason}
        end
      end,
      4
    )
  end

  defp retry(operation, attempts) do
    case safe_call(operation) do
      :ok ->
        :ok

      {:error, reason} when attempts > 1 ->
        Process.sleep(25 * Integer.pow(2, 4 - attempts))
        retry(operation, attempts - 1)

      {:error, reason} ->
        {:error, reason}

      other when attempts > 1 ->
        Process.sleep(25 * Integer.pow(2, 4 - attempts))
        retry(operation, attempts - 1)

      other ->
        {:error, other}
    end
  end

  defp safe_call(operation) do
    try do
      operation.()
    rescue
      error -> {:error, error}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp retry_delay(attempt) do
    min(1_000 * Integer.pow(2, min(attempt - 1, 6)), 60_000)
  end

  defp schedule_next(state) do
    if state.timer, do: Process.cancel_timer(state.timer)

    next_time =
      state.appointments
      |> Enum.reject(fn {id, appointment} ->
        appointment.notified or
          appointment.cancelled or
          MapSet.member?(state.pending, id)
      end)
      |> Enum.map(fn {_id, appointment} -> appointment.scheduled_at end)
      |> Enum.min(fn -> nil end)

    case next_time do
      nil ->
        %{state | timer: nil}

      scheduled_at ->
        delay =
          max(scheduled_at - System.system_time(:millisecond), 0)
          |> min(@max_timer_delay)

        %{state | timer: Process.send_after(self(), :tick, delay)}
    end
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
end
