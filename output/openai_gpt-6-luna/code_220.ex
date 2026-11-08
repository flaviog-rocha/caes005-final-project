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

defmodule AppointmentScheduler do
  use GenServer

  @fields [:title, :starts_at, :ends_at]
  @required_fields @fields

  # Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  def create(server \\ __MODULE__, attributes) do
    GenServer.call(server, {:create, attributes})
  end

  def get(server \\ __MODULE__, id) do
    GenServer.call(server, {:get, id})
  end

  def list(server \\ __MODULE__) do
    GenServer.call(server, :list)
  end

  def update(server \\ __MODULE__, id, attributes) do
    GenServer.call(server, {:update, id, attributes})
  end

  def delete(server \\ __MODULE__, id) do
    GenServer.call(server, {:delete, id})
  end

  # Submit an operation for execution and receive its operation ID immediately.
  def submit(server \\ __MODULE__, action) do
    GenServer.call(server, {:submit, action})
  end

  def operation_status(server \\ __MODULE__, operation_id) do
    GenServer.call(server, {:operation_status, operation_id})
  end

  def operations(server \\ __MODULE__) do
    GenServer.call(server, :operations)
  end

  def in_progress(server \\ __MODULE__) do
    GenServer.call(server, :in_progress)
  end

  # The caller receives the standard {:DOWN, ref, :process, pid, reason} message.
  def monitor(server \\ __MODULE__) do
    case resolve_pid(server) do
      {:ok, pid} -> {:ok, Process.monitor(pid)}
      :error -> {:error, :not_found}
    end
  end

  @impl true
  def init(_initial_state) do
    {:ok, %{appointments: %{}, next_id: 1, operations: %{}}}
  end

  @impl true
  def handle_call({:create, attributes}, _from, state) do
    {result, state} = perform({:create, attributes}, state)
    {:reply, result, state}
  end

  def handle_call({:get, id}, _from, state) do
    {result, state} = perform({:get, id}, state)
    {:reply, result, state}
  end

  def handle_call(:list, _from, state) do
    {result, state} = perform(:list, state)
    {:reply, result, state}
  end

  def handle_call({:update, id, attributes}, _from, state) do
    {result, state} = perform({:update, id, attributes}, state)
    {:reply, result, state}
  end

  def handle_call({:delete, id}, _from, state) do
    {result, state} = perform({:delete, id}, state)
    {:reply, result, state}
  end

  def handle_call({:submit, action}, _from, state) do
    operation_id = make_ref()

    operation = %{
      id: operation_id,
      action: action,
      status: :queued,
      submitted_at: now(),
      completed_at: nil,
      result: nil
    }

    state = put_in(state.operations[operation_id], operation)
    send(self(), {:execute_operation, operation_id})

    {:reply, {:ok, operation_id}, state}
  end

  def handle_call({:operation_status, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:operations, _from, state) do
    {:reply, state.operations, state}
  end

  def handle_call(:in_progress, _from, state) do
    current =
      state.operations
      |> Enum.filter(fn {_id, operation} ->
        operation.status in [:queued, :running]
      end)
      |> Map.new()

    {:reply, current, state}
  end

  @impl true
  def handle_info({:execute_operation, operation_id}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        running = %{operation | status: :running}
        state = put_in(state.operations[operation_id], running)

        {result, state} = perform(operation.action, state)
        status = if match?({:ok, _}, result), do: :completed, else: :failed

        completed = %{
          running
          | status: status,
            completed_at: now(),
            result: result
        }

        {:noreply, put_in(state.operations[operation_id], completed)}

      :error ->
        {:noreply, state}
    end
  end

  defp perform({:create, attributes}, state) do
    with {:ok, validated} <- validate(attributes, :create) do
      id = state.next_id
      appointment = Map.put(validated, :id, id)

      state = %{
        state
        | appointments: Map.put(state.appointments, id, appointment),
          next_id: id + 1
      }

      {{:ok, appointment}, state}
    else
      {:error, errors} -> {{:error, errors}, state}
    end
  end

  defp perform({:get, id}, state) do
    if valid_id?(id) do
      case Map.fetch(state.appointments, id) do
        {:ok, appointment} -> {{:ok, appointment}, state}
        :error -> {{:error, :not_found}, state}
      end
    else
      {{:error, :invalid_id}, state}
    end
  end

  defp perform(:list, state) do
    appointments =
      state.appointments
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {{:ok, appointments}, state}
  end

  defp perform({:update, id, attributes}, state) do
    cond do
      not valid_id?(id) ->
        {{:error, :invalid_id}, state}

      not Map.has_key?(state.appointments, id) ->
        {{:error, :not_found}, state}

      true ->
        existing = Map.fetch!(state.appointments, id)
        merged = Map.merge(Map.delete(existing, :id), attributes)

        with {:ok, validated} <- validate(merged, :update) do
          appointment = Map.put(validated, :id, id)
          appointments = Map.put(state.appointments, id, appointment)
          {{:ok, appointment}, %{state | appointments: appointments}}
        else
          {:error, errors} -> {{:error, errors}, state}
        end
    end
  end

  defp perform({:delete, id}, state) do
    if valid_id?(id) do
      case Map.pop(state.appointments, id) do
        {nil, _appointments} ->
          {{:error, :not_found}, state}

        {appointment, appointments} ->
          {{:ok, appointment}, %{state | appointments: appointments}}
      end
    else
      {{:error, :invalid_id}, state}
    end
  end

  defp perform(_unsupported_action, state) do
    {{:error, :invalid_action}, state}
  end

  defp validate(attributes, mode) when is_map(attributes) do
    unknown_fields = Enum.reject(Map.keys(attributes), &(&1 in @fields))

    missing_fields =
      if mode == :create do
        Enum.reject(@required_fields, &Map.has_key?(attributes, &1))
      else
        []
      end

    errors =
      %{}
      |> maybe_add_error(:unknown_fields, unknown_fields)
      |> maybe_add_error(:required_fields, missing_fields)
      |> validate_title(attributes)
      |> validate_datetime(:starts_at, attributes)
      |> validate_datetime(:ends_at, attributes)
      |> validate_time_order(attributes)

    if map_size(errors) == 0 do
      {:ok, Map.take(attributes, @fields)}
    else
      {:error, errors}
    end
  end

  defp validate(_attributes, _mode) do
    {:error, %{attributes: :must_be_a_map}}
  end

  defp validate_title(errors, attributes) do
    if Map.has_key?(attributes, :title) do
      case Map.get(attributes, :title) do
        title when is_binary(title) ->
          if String.trim(title) == "",
            do: Map.put(errors, :title, :must_not_be_empty),
            else: errors

        _ ->
          Map.put(errors, :title, :must_be_a_string)
      end
    else
      errors
    end
  end

  defp validate_datetime(errors, field, attributes) do
    if Map.has_key?(attributes, field) and
         not match?(%DateTime{}, Map.get(attributes, field)) do
      Map.put(errors, field, :must_be_a_datetime)
    else
      errors
    end
  end

  defp validate_time_order(errors, attributes) do
    starts_at = Map.get(attributes, :starts_at)
    ends_at = Map.get(attributes, :ends_at)

    if match?(%DateTime{}, starts_at) and match?(%DateTime{}, ends_at) do
      try do
        if DateTime.compare(starts_at, ends_at) != :lt do
          Map.put(errors, :ends_at, :must_be_after_starts_at)
        else
          errors
        end
      rescue
        _ -> Map.put(errors, :datetime, :invalid_datetime)
      end
    else
      errors
    end
  end

  defp maybe_add_error(errors, _field, []), do: errors
  defp maybe_add_error(errors, field, value), do: Map.put(errors, field, value)

  defp valid_id?(id), do: is_integer(id) and id > 0

  defp now, do: System.system_time(:millisecond)

  defp resolve_pid(pid) when is_pid(pid), do: {:ok, pid}

  defp resolve_pid(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> :error
      pid -> {:ok, pid}
    end
  end

  defp resolve_pid(_), do: :error
end

defmodule AppointmentScheduler.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    supervisor_name = Keyword.get(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: supervisor_name)
  end

  @impl true
  def init(opts) do
    scheduler_opts = Keyword.get(opts, :scheduler_opts, [])

    children = [
      %{
        id: AppointmentScheduler,
        start: {AppointmentScheduler, :start_link, [scheduler_opts]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
