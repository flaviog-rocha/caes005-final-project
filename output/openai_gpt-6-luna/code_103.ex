# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Impedir que uma mesma operação seja executada mais de uma vez.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  use GenServer

  @default_max_concurrency System.schedulers_online()

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def submit(server, operation_id, operation) when is_function(operation, 0) do
    GenServer.call(server, {:submit, operation_id, operation}, :infinity)
  end

  def submit(_server, _operation_id, _operation), do: {:error, :invalid_operation}

  def history(server) do
    GenServer.call(server, :history)
  end

  def status(server, operation_id) do
    GenServer.call(server, {:status, operation_id})
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    {:ok, dynamic_supervisor} =
      DynamicSupervisor.start_link(strategy: :one_for_one)

    {:ok,
     %{
       dynamic_supervisor: dynamic_supervisor,
       max_concurrency: max_concurrency,
       active: %{},
       monitors: %{},
       queue: :queue.new(),
       records: %{},
       order: []
     }}
  end

  @impl true
  def handle_call({:submit, operation_id, operation}, _from, state) do
    case Map.fetch(state.records, operation_id) do
      {:ok, record} ->
        {:reply, {:duplicate, record}, state}

      :error ->
        record = %{
          operation_id: operation_id,
          status: :queued,
          submitted_at: now(),
          started_at: nil,
          finished_at: nil,
          result: nil,
          error: nil,
          unexpected_shutdown: false
        }

        state =
          state
          |> put_record(operation_id, record)
          |> Map.update!(:order, &[operation_id | &1])
          |> Map.update!(:queue, &:queue.in({operation_id, operation}, &1))
          |> dispatch()

        {:reply, {:ok, operation_id}, state}
    end
  end

  def handle_call(:history, _from, state) do
    records =
      state.order
      |> Enum.reverse()
      |> Enum.map(&Map.fetch!(state.records, &1))

    {:reply, records, state}
  end

  def handle_call({:status, operation_id}, _from, state) do
    {:reply, Map.fetch(state.records, operation_id), state}
  end

  @impl true
  def handle_info({:operation_result, token, outcome}, state) do
    case Map.pop(state.active, token) do
      {nil, _active} ->
        {:noreply, state}

      {active, remaining_active} ->
        Process.demonitor(active.monitor, [:flush])

        state = %{
          state
          | active: remaining_active,
            monitors: Map.delete(state.monitors, active.monitor)
        }

        record = Map.fetch!(state.records, active.operation_id)

        record =
          case outcome do
            {:ok, result} ->
              %{record | status: :completed, result: result, finished_at: now()}

            {:error, error} ->
              %{record | status: :failed, error: error, finished_at: now()}
          end

        state
        |> put_record(active.operation_id, record)
        |> dispatch()
        |> then(&{:noreply, &1})
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {token, remaining_monitors} ->
        case Map.pop(state.active, token) do
          {nil, remaining_active} ->
            {:noreply, %{state | monitors: remaining_monitors, active: remaining_active}}

          {active, remaining_active} ->
            record = Map.fetch!(state.records, active.operation_id)

            record = %{
              record
              | status: :failed,
                error: {:process_down, reason},
                finished_at: now(),
                unexpected_shutdown: true
            }

            state = %{
              state
              | monitors: remaining_monitors,
                active: remaining_active
            }

            state
            |> put_record(active.operation_id, record)
            |> dispatch()
            |> then(&{:noreply, &1})
        end
    end
  end

  def handle_info({:EXIT, supervisor, _reason}, %{dynamic_supervisor: supervisor} = state) do
    case DynamicSupervisor.start_link(strategy: :one_for_one) do
      {:ok, replacement} ->
        {:noreply, %{state | dynamic_supervisor: replacement}}

      {:error, reason} ->
        {:stop, {:dynamic_supervisor_restart_failed, reason}, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if Process.alive?(state.dynamic_supervisor) do
      Supervisor.stop(state.dynamic_supervisor, :normal)
    end

    :ok
  end

  defp dispatch(state) do
    if map_size(state.active) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, {operation_id, operation}}, queue} ->
          token = make_ref()

          child_spec = %{
            id: {UserSessionManager.Worker, token},
            start:
              {UserSessionManager.Worker, :start_link,
               [self(), token, operation]},
            restart: :temporary,
            shutdown: 5_000,
            type: :worker
          }

          state = %{state | queue: queue}

          case DynamicSupervisor.start_child(state.dynamic_supervisor, child_spec) do
            {:ok, pid} ->
              monitor = Process.monitor(pid)
              record = Map.fetch!(state.records, operation_id)

              record = %{
                record
                | status: :running,
                  started_at: now()
              }

              state = %{
                state
                | active:
                    Map.put(state.active, token, %{
                      operation_id: operation_id,
                      pid: pid,
                      monitor: monitor
                    }),
                  monitors: Map.put(state.monitors, monitor, token)
              }

              state
              |> put_record(operation_id, record)
              |> dispatch()

            {:ok, pid, _info} ->
              monitor = Process.monitor(pid)
              record = Map.fetch!(state.records, operation_id)

              record = %{
                record
                | status: :running,
                  started_at: now()
              }

              state = %{
                state
                | active:
                    Map.put(state.active, token, %{
                      operation_id: operation_id,
                      pid: pid,
                      monitor: monitor
                    }),
                  monitors: Map.put(state.monitors, monitor, token)
              }

              state
              |> put_record(operation_id, record)
              |> dispatch()

            {:error, reason} ->
              record = Map.fetch!(state.records, operation_id)

              record = %{
                record
                | status: :failed,
                  error: {:start_child, reason},
                  finished_at: now()
              }

              state
              |> put_record(operation_id, record)
              |> dispatch()
          end
      end
    end
  end

  defp put_record(state, operation_id, record) do
    %{state | records: Map.put(state.records, operation_id, record)}
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule UserSessionManager.Worker do
  use GenServer

  def start_link(manager, token, operation) do
    GenServer.start_link(__MODULE__, {manager, token, operation})
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :run}}

  @impl true
  def handle_continue(:run, {manager, token, operation} = state) do
    outcome =
      try do
        {:ok, operation.()}
      rescue
        exception ->
          {:error, {:error, exception, __STACKTRACE__}}
      catch
        kind, reason ->
          {:error, {kind, reason, __STACKTRACE__}}
      end

    send(manager, {:operation_result, token, outcome})
    {:stop, :normal, state}
  end
end
