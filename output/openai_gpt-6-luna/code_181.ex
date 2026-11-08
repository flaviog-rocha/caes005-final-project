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

defmodule AppointmentSystem do
  use Supervisor

  @task_supervisor AppointmentSystem.TaskSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    handler = Keyword.get(opts, :handler, fn _appointment -> :ok end)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    unless is_function(handler, 1) do
      raise ArgumentError, "handler deve ser uma função de aridade 1"
    end

    children = [
      AppointmentSystem.Store,
      {Task.Supervisor, name: @task_supervisor},
      {AppointmentSystem.Scheduler,
       max_concurrency: max_concurrency, handler: handler}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def submit(id, attrs) do
    with :ok <- validate_id(id),
         :ok <- validate_attrs(attrs) do
      GenServer.call(AppointmentSystem.Scheduler, {:submit, id, attrs})
    end
  end

  def get(id) when is_binary(id) do
    AppointmentSystem.Store.get(id)
  end

  def get(_id), do: {:error, :invalid_id}

  defp validate_id(id) when is_binary(id) do
    if String.trim(id) == "", do: {:error, :invalid_id}, else: :ok
  end

  defp validate_id(_id), do: {:error, :invalid_id}

  defp validate_attrs(attrs) when is_map(attrs) do
    title = Map.get(attrs, :title)
    starts_at = Map.get(attrs, :starts_at)

    cond do
      not is_binary(title) ->
        {:error, :invalid_title}

      String.trim(title) == "" ->
        {:error, :invalid_title}

      not match?(%DateTime{}, starts_at) ->
        {:error, :invalid_starts_at}

      true ->
        :ok
    end
  end

  defp validate_attrs(_attrs), do: {:error, :invalid_appointment}
end

defmodule AppointmentSystem.Store do
  use GenServer

  @key {__MODULE__, :appointments}

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    {:ok, :persistent_term.get(@key, %{})}
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list, do: GenServer.call(__MODULE__, :list)
  def put(id, record), do: GenServer.call(__MODULE__, {:put, id, record})

  @impl true
  def handle_call({:get, id}, _from, records) do
    reply =
      case Map.fetch(records, id) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, :not_found}
      end

    {:reply, reply, records}
  end

  def handle_call(:list, _from, records) do
    {:reply, Map.to_list(records), records}
  end

  def handle_call({:put, id, record}, _from, records) do
    updated = Map.put(records, id, record)
    :persistent_term.put(@key, updated)
    {:reply, :ok, updated}
  end
end

defmodule AppointmentSystem.Scheduler do
  use GenServer

  @task_supervisor AppointmentSystem.TaskSupervisor

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.fetch!(opts, :max_concurrency)
    handler = Keyword.fetch!(opts, :handler)

    queue =
      AppointmentSystem.Store.list()
      |> Enum.sort_by(fn {_id, record} -> record.inserted_at end)
      |> Enum.reduce(:queue.new(), fn {id, record}, queue ->
        case record.status do
          :queued ->
            :queue.in(id, queue)

          :running ->
            # Operações em andamento não são repetidas após uma falha: isso
            # preserva a garantia de execução no máximo uma vez.
            save_record(id, Map.merge(record, %{
              status: :interrupted,
              updated_at: now()
            }))

            queue

          _ ->
            queue
        end
      end)

    send(self(), :dispatch)

    {:ok,
     %{
       max_concurrency: max_concurrency,
       handler: handler,
       queue: queue,
       active: %{}
     }}
  end

  @impl true
  def handle_call({:submit, id, attrs}, _from, state) do
    case AppointmentSystem.Store.get(id) do
      {:ok, record} ->
        {:reply, {:error, {:already_submitted, record.status}}, state}

      {:error, :not_found} ->
        timestamp = now()

        record = %{
          id: id,
          attrs: attrs,
          status: :queued,
          inserted_at: timestamp,
          updated_at: timestamp
        }

        :ok = AppointmentSystem.Store.put(id, record)
        state = %{state | queue: :queue.in(id, state.queue)}
        send(self(), :dispatch)

        {:reply, {:ok, :queued}, state}
    end
  end

  @impl true
  def handle_info(:dispatch, state) do
    {:noreply, dispatch(state)}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {%{id: id}, active} ->
        Process.demonitor(ref, [:flush])

        case AppointmentSystem.Store.get(id) do
          {:ok, record} ->
            save_record(id, Map.merge(record, %{
              status: :completed,
              result: result,
              updated_at: now()
            }))

          _ ->
            :ok
        end

        state = %{state | active: active}
        {:noreply, dispatch(state)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.active, ref) do
      {nil, _active} ->
        {:noreply, state}

      {%{id: id}, active} ->
        case AppointmentSystem.Store.get(id) do
          {:ok, record} ->
            save_record(id, Map.merge(record, %{
              status: :failed,
              reason: reason,
              updated_at: now()
            }))

          _ ->
            :ok
        end

        state = %{state | active: active}
        {:noreply, dispatch(state)}
    end
  end

  defp dispatch(state) when map_size(state.active) >= state.max_concurrency,
    do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, id}, queue} ->
        state = %{state | queue: queue}

        case AppointmentSystem.Store.get(id) do
          {:ok, %{status: :queued, attrs: attrs} = record} ->
            # Registrar antes de iniciar o processo impede que uma operação
            # já iniciada seja submetida novamente após uma falha.
            save_record(id, Map.merge(record, %{
              status: :running,
              updated_at: now()
            }))

            handler = state.handler
            task = Task.Supervisor.async_nolink(@task_supervisor, fn -> handler.(attrs) end)

            state = %{state | active: Map.put(state.active, task.ref, %{id: id})}
            dispatch(state)

          _ ->
            dispatch(state)
        end
    end
  end

  defp save_record(id, record) do
    :ok = AppointmentSystem.Store.put(id, record)
  end

  defp now, do: System.system_time(:millisecond)
end
