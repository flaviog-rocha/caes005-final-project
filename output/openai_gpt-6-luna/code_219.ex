# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir cadastrar, consultar, atualizar e remover registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Detectar encerramentos inesperados de processos.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AppointmentSystem do
  @moduledoc """
  Sistema de agendamento em memória, com operações CRUD síncronas e assíncronas.
  """

  alias AppointmentSystem.Store

  @store AppointmentSystem.Store

  def start_link(opts \\ []) do
    AppointmentSystem.Supervisor.start_link(opts)
  end

  def create(attrs) do
    with {:ok, attrs} <- validate_appointment(attrs) do
      GenServer.call(@store, {:create, attrs})
    end
  end

  def get(id) do
    with :ok <- validate_id(id) do
      GenServer.call(@store, {:get, id})
    end
  end

  def list do
    GenServer.call(@store, :list)
  end

  def update(id, attrs) do
    with :ok <- validate_id(id),
         {:ok, attrs} <- validate_changes(attrs) do
      GenServer.call(@store, {:update, id, attrs})
    end
  end

  def delete(id) do
    with :ok <- validate_id(id) do
      GenServer.call(@store, {:delete, id})
    end
  end

  @doc "Agenda uma operação CRUD para execução assíncrona."
  def submit(operation) do
    with {:ok, operation} <- prepare_operation(operation) do
      GenServer.call(@store, {:submit, operation})
    end
  end

  def get_operation(id) do
    with :ok <- validate_id(id) do
      GenServer.call(@store, {:get_operation, id})
    end
  end

  def list_operations do
    GenServer.call(@store, :list_operations)
  end

  defp prepare_operation({:create, attrs}) do
    with {:ok, attrs} <- validate_appointment(attrs) do
      id = new_id()
      {:ok, {:create, id, attrs}}
    end
  end

  defp prepare_operation({:update, id, attrs}) do
    with :ok <- validate_id(id),
         {:ok, attrs} <- validate_changes(attrs) do
      {:ok, {:update, id, attrs}}
    end
  end

  defp prepare_operation({:delete, id}) do
    with :ok <- validate_id(id) do
      {:ok, {:delete, id}}
    end
  end

  defp prepare_operation(_), do: {:error, :invalid_operation}

  defp validate_appointment(attrs) when is_map(attrs) do
    allowed = [:title, :starts_at, :ends_at, :description]
    required = [:title, :starts_at, :ends_at]

    cond do
      Enum.any?(Map.keys(attrs), &(&1 not in allowed)) ->
        {:error, :unknown_fields}

      Enum.any?(required, &(not Map.has_key?(attrs, &1))) ->
        {:error, :missing_fields}

      true ->
        normalized = Map.take(attrs, allowed)

        case validate_complete_appointment(normalized) do
          :ok -> {:ok, normalized}
          error -> error
        end
    end
  end

  defp validate_appointment(_), do: {:error, :invalid_appointment}

  defp validate_changes(attrs) when is_map(attrs) do
    allowed = [:title, :starts_at, :ends_at, :description]

    cond do
      map_size(attrs) == 0 ->
        {:error, :empty_update}

      Enum.any?(Map.keys(attrs), &(&1 not in allowed)) ->
        {:error, :unknown_fields}

      true ->
        case validate_individual_fields(attrs) do
          :ok -> {:ok, attrs}
          error -> error
        end
    end
  end

  defp validate_changes(_), do: {:error, :invalid_appointment}

  defp validate_complete_appointment(attrs) do
    with :ok <- validate_individual_fields(attrs),
         true <- DateTime.compare(attrs.starts_at, attrs.ends_at) == :lt do
      :ok
    else
      false -> {:error, :invalid_time_range}
      error -> error
    end
  end

  defp validate_individual_fields(attrs) do
    Enum.reduce_while(attrs, :ok, fn
      {:title, value}, :ok when is_binary(value) ->
        if String.trim(value) == "", do: {:halt, {:error, :invalid_title}}, else: {:cont, :ok}

      {:starts_at, %DateTime{}}, :ok ->
        {:cont, :ok}

      {:ends_at, %DateTime{}}, :ok ->
        {:cont, :ok}

      {:description, value}, :ok when is_binary(value) ->
        {:cont, :ok}

      _, :ok ->
        {:halt, {:error, :invalid_field}}
    end)
  end

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0, do: :ok
  defp validate_id(_), do: {:error, :invalid_id}

  defp new_id do
    "appointment-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
  end

  defmodule Supervisor do
    use Supervisor

    def start_link(opts \\ []) do
      Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
      children = [
        {Task.Supervisor, name: AppointmentSystem.TaskPool},
        AppointmentSystem.Store
      ]

      Supervisor.init(children, strategy: :one_for_one)
    end
  end

  defmodule Store do
    use GenServer

    @snapshot_key {AppointmentSystem.Store, :snapshot}
    @max_attempts 3

    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
      snapshot =
        :persistent_term.get(@snapshot_key, %{
          appointments: %{},
          jobs: %{}
        })

      jobs =
        Map.new(snapshot.jobs, fn {id, job} ->
          if job.status == :running do
            {id, %{job | status: :queued}}
          else
            {id, job}
          end
        end)

      state = %{appointments: snapshot.appointments, jobs: jobs, monitors: %{}}
      persist(state)
      send(self(), :dispatch_pending)
      {:ok, state}
    end

    @impl true
    def handle_call({:create, attrs}, _from, state) do
      case AppointmentSystem.validate_appointment(attrs) do
        {:ok, attrs} ->
          id = "appointment-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))
          appointment = Map.put(attrs, :id, id)
          appointments = Map.put(state.appointments, id, appointment)
          next_state = persist_state(%{state | appointments: appointments})
          {:reply, {:ok, appointment}, next_state}

        error ->
          {:reply, error, state}
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

    def handle_call({:update, id, attrs}, _from, state) do
      case {AppointmentSystem.validate_id(id), AppointmentSystem.validate_changes(attrs),
            Map.fetch(state.appointments, id)} do
        {:ok, {:ok, attrs}, {:ok, existing}} ->
          updated = Map.merge(existing, attrs)

          case AppointmentSystem.validate_appointment(Map.delete(updated, :id)) do
            {:ok, validated} ->
              appointment = Map.put(validated, :id, id)
              appointments = Map.put(state.appointments, id, appointment)
              next_state = persist_state(%{state | appointments: appointments})
              {:reply, {:ok, appointment}, next_state}

            error ->
              {:reply, error, state}
          end

        {:ok, {:ok, _attrs}, :error} ->
          {:reply, {:error, :not_found}, state}

        {{:error, _} = error, _, _} ->
          {:reply, error, state}

        {_, {:error, _} = error, _} ->
          {:reply, error, state}
      end
    end

    def handle_call({:delete, id}, _from, state) do
      case Map.pop(state.appointments, id) do
        {nil, _appointments} ->
          {:reply, {:error, :not_found}, state}

        {appointment, appointments} ->
          next_state = persist_state(%{state | appointments: appointments})
          {:reply, {:ok, appointment}, next_state}
      end
    end

    def handle_call({:submit, operation}, _from, state) do
      id = "operation-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

      job = %{
        id: id,
        operation: operation,
        status: :queued,
        attempts: 0,
        result: nil,
        error: nil,
        inserted_at: DateTime.utc_now()
      }

      next_state = persist_state(%{state | jobs: Map.put(state.jobs, id, job)})
      send(self(), :dispatch_pending)
      {:reply, {:ok, id}, next_state}
    end

    def handle_call({:get_operation, id}, _from, state) do
      reply =
        case Map.fetch(state.jobs, id) do
          {:ok, job} -> {:ok, public_job(job)}
          :error -> {:error, :not_found}
        end

      {:reply, reply, state}
    end

    def handle_call(:list_operations, _from, state) do
      jobs = state.jobs |> Map.values() |> Enum.map(&public_job/1)
      {:reply, jobs, state}
    end

    def handle_call({:execute_job, id}, _from, state) do
      case Map.fetch(state.jobs, id) do
        {:ok, %{status: :completed, result: result}} ->
          {:reply, result, state}

        {:ok, %{status: status} = job} when status in [:running, :queued] ->
          {result, appointments} = execute(job.operation, state.appointments)
          completed = %{job | status: :completed, result: result, error: nil}

          next_state =
            persist_state(%{
              state
              | appointments: appointments,
                jobs: Map.put(state.jobs, id, completed)
            })

          {:reply, result, next_state}

        {:ok, _job} ->
          {:reply, {:error, :operation_unavailable}, state}

        :error ->
          {:reply, {:error, :not_found}, state}
      end
    end

    @impl true
    def handle_info(:dispatch_pending, state) do
      next_state =
        state.jobs
        |> Enum.filter(fn {_id, job} -> job.status == :queued end)
        |> Enum.reduce(state, fn {id, _job}, acc -> start_worker(id, acc) end)

      {:noreply, next_state}
    end

    def handle_info({reference, _result}, state) when is_reference(reference) do
      case Map.pop(state.monitors, reference) do
        {nil, _monitors} ->
          {:noreply, state}

        {_id, monitors} ->
          Process.demonitor(reference, [:flush])
          {:noreply, %{state | monitors: monitors}}
      end
    end

    def handle_info({:DOWN, reference, :process, _pid, reason}, state) do
      case Map.pop(state.monitors, reference) do
        {nil, _monitors} ->
          {:noreply, state}

        {id, monitors} ->
          state = %{state | monitors: monitors}

          case Map.get(state.jobs, id) do
            %{status: :completed} ->
              {:noreply, state}

            %{attempts: attempts} = job when attempts < @max_attempts ->
              retried = %{job | status: :queued, error: {:worker_terminated, reason}}
              next_state = persist_state(%{state | jobs: Map.put(state.jobs, id, retried)})
              Process.send_after(self(), :dispatch_pending, 50)
              {:noreply, next_state}

            %{ } = job ->
              failed = %{job | status: :failed, error: {:worker_terminated, reason}}
              {:noreply, persist_state(%{state | jobs: Map.put(state.jobs, id, failed)})}

            _ ->
              {:noreply, state}
          end
      end
    end

    def handle_info(_message, state), do: {:noreply, state}

    defp start_worker(id, state) do
      job = Map.fetch!(state.jobs, id)
      running = %{job | status: :running, attempts: job.attempts + 1, error: nil}
      state = persist_state(%{state | jobs: Map.put(state.jobs, id, running)})

      try do
        task =
          Task.Supervisor.async_nolink(AppointmentSystem.TaskPool, fn ->
            GenServer.call(__MODULE__, {:execute_job, id}, :infinity)
          end)

        %{state | monitors: Map.put(state.monitors, task.ref, id)}
      catch
        :exit, reason ->
          queued = %{running | status: :queued, error: {:worker_start_failed, reason}}
          next_state = persist_state(%{state | jobs: Map.put(state.jobs, id, queued)})
          Process.send_after(self(), :dispatch_pending, 100)
          next_state
      end
    end

    defp execute({:create, id, attrs}, appointments) do
      with {:ok, attrs} <- AppointmentSystem.validate_appointment(attrs),
           false <- Map.has_key?(appointments, id) do
        appointment = Map.put(attrs, :id, id)
        {{:ok, appointment}, Map.put(appointments, id, appointment)}
      else
        true -> {{:error, :already_exists}, appointments}
        error -> {error, appointments}
      end
    end

    defp execute({:update, id, attrs}, appointments) do
      with :ok <- AppointmentSystem.validate_id(id),
           {:ok, attrs} <- AppointmentSystem.validate_changes(attrs),
           {:ok, existing} <- Map.fetch(appointments, id),
           {:ok, validated} <-
             AppointmentSystem.validate_appointment(Map.merge(Map.delete(existing, :id), attrs)) do
        appointment = Map.put(validated, :id, id)
        {{:ok, appointment}, Map.put(appointments, id, appointment)}
      else
        :error -> {{:error, :not_found}, appointments}
        error -> {error, appointments}
      end
    end

    defp execute({:delete, id}, appointments) do
      with :ok <- AppointmentSystem.validate_id(id),
           {:ok, appointment} <- Map.fetch(appointments, id) do
        {{:ok, appointment}, Map.delete(appointments, id)}
      else
        :error -> {{:error, :not_found}, appointments}
        error -> {error, appointments}
      end
    end

    defp persist_state(state) do
      persist(state)
      state
    end

    defp persist(state) do
      :persistent_term.put(@snapshot_key, %{
        appointments: state.appointments,
        jobs: state.jobs
      })
    end

    defp public_job(job) do
      Map.take(job, [:id, :status, :attempts, :result, :error, :inserted_at])
    end
  end
end
