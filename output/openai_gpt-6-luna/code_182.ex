# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir comunicação entre diferentes processos.
# 3. Validar os dados recebidos antes de realizar operações.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentScheduler.Store do
  @moduledoc false
  use GenServer

  @table :appointment_scheduler_records

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def claim(record), do: GenServer.call(__MODULE__, {:claim, record})
  def update(id, status), do: GenServer.call(__MODULE__, {:update, id, status})
  def queued, do: GenServer.call(__MODULE__, :queued)

  @impl true
  def init(:ok) do
    :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    {:ok, :ok}
  end

  @impl true
  def handle_call({:claim, record}, _from, state) do
    result =
      if :ets.insert_new(@table, {record.id, record}) do
        :ok
      else
        :already_scheduled
      end

    {:reply, result, state}
  end

  def handle_call({:update, id, status}, _from, state) do
    case :ets.lookup(@table, id) do
      [{^id, record}] ->
        :ets.insert(@table, {id, %{record | status: status}})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:queued, _from, state) do
    records =
      @table
      |> :ets.tab2list()
      |> Enum.map(fn {_id, record} -> record end)
      |> Enum.filter(&(&1.status == :queued))

    {:reply, records, state}
  end
end

defmodule AppointmentScheduler.Coordinator do
  @moduledoc false
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def schedule(appointment, server \\ __MODULE__) do
    GenServer.call(server, {:schedule, appointment})
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      queue =
        AppointmentScheduler.Store.queued()
        |> Enum.sort_by(&{&1.at_ms, &1.id})

      state = %{
        max_concurrency: max_concurrency,
        queue: queue,
        running: %{},
        timer: nil
      }

      {:ok, dispatch(state)}
    end
  end

  @impl true
  def handle_call({:schedule, appointment}, _from, state) do
    case validate(appointment) do
      {:ok, record} ->
        case AppointmentScheduler.Store.claim(record) do
          :ok ->
            state = %{state | queue: insert_sorted(state.queue, record)}
            {:reply, {:ok, :scheduled}, dispatch(state)}

          :already_scheduled ->
            {:reply, {:ok, :already_scheduled}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:dispatch, state) do
    {:noreply, dispatch(%{state | timer: nil})}
  end

  def handle_info({ref, _result}, state) when is_reference(ref) do
    case Map.pop(state.running, ref) do
      {nil, _running} ->
        {:noreply, state}

      {record, running} ->
        Process.demonitor(ref, [:flush])
        AppointmentScheduler.Store.update(record.id, :completed)
        {:noreply, dispatch(%{state | running: running})}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.running, ref) do
      {nil, _running} ->
        {:noreply, state}

      {record, running} ->
        AppointmentScheduler.Store.update(record.id, :failed)
        {:noreply, dispatch(%{state | running: running})}
    end
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}
  def handle_info(_message, state), do: {:noreply, state}

  defp validate(%{id: id, at: %DateTime{} = at, payload: payload, action: action})
       when is_binary(id) and byte_size(id) > 0 and is_function(action, 1) do
    try do
      {:ok,
       %{
         id: id,
         at_ms: DateTime.to_unix(at, :millisecond),
         payload: payload,
         action: action,
         status: :queued
       }}
    rescue
      _ -> {:error, :invalid_datetime}
    end
  end

  defp validate(_appointment), do: {:error, :invalid_appointment}

  defp insert_sorted(queue, record) do
    (queue ++ [record])
    |> Enum.sort_by(&{&1.at_ms, &1.id})
  end

  defp dispatch(state) do
    state = cancel_timer(state)

    state =
      if map_size(state.running) < state.max_concurrency do
        start_due(state)
      else
        state
      end

    arm_timer(state)
  end

  defp start_due(state) do
    now = System.system_time(:millisecond)

    case state.queue do
      [record | rest]
      when record.at_ms <= now and map_size(state.running) < state.max_concurrency ->
        AppointmentScheduler.Store.update(record.id, :started)

        try do
          task = Task.async(fn -> record.action.(record.payload) end)
          running = Map.put(state.running, task.ref, record)
          start_due(%{state | queue: rest, running: running})
        rescue
          _ ->
            AppointmentScheduler.Store.update(record.id, :failed)
            start_due(%{state | queue: rest})
        catch
          _, _ ->
            AppointmentScheduler.Store.update(record.id, :failed)
            start_due(%{state | queue: rest})
        end

      _ ->
        state
    end
  end

  defp arm_timer(state) do
    can_start? = map_size(state.running) < state.max_concurrency

    case {can_start?, state.queue} do
      {true, [next | _]} ->
        delay = max(next.at_ms - System.system_time(:millisecond), 0)
        %{state | timer: Process.send_after(self(), :dispatch, delay)}

      _ ->
        state
    end
  end

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: timer} = state) do
    Process.cancel_timer(timer)
    %{state | timer: nil}
  end
end

defmodule AppointmentScheduler do
  @moduledoc """
  Agendador em memória. Cada compromisso exige `:id` único, `:at` como
  `DateTime`, `:payload` e `:action` como função de aridade 1.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def schedule(appointment, server \\ AppointmentScheduler.Coordinator) do
    AppointmentScheduler.Coordinator.schedule(appointment, server)
  end

  @impl true
  def init(opts) do
    children = [
      {AppointmentScheduler.Store, []},
      {AppointmentScheduler.Coordinator, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
