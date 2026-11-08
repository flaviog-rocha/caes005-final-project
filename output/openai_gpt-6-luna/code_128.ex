# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Implementar expiração automática de registros.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir consultar o estado atual das operações em andamento.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  use GenServer

  @default_ttl 60_000
  @default_max_concurrency max(System.schedulers_online(), 1)
  @default_max_retries 2

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def submit(sensor_id, fun, opts \\ []) when is_function(fun, 0) do
    submit(__MODULE__, sensor_id, fun, opts)
  end

  def submit(server, sensor_id, fun, opts)
      when is_function(fun, 0) and is_list(opts) do
    GenServer.call(server, {:submit, sensor_id, fun})
  end

  def operations(server \\ __MODULE__) do
    GenServer.call(server, :operations)
  end

  def operation(id), do: operation(__MODULE__, id)

  def operation(server, id) do
    GenServer.call(server, {:operation, id})
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       max_concurrency: positive_integer(Keyword.get(opts, :max_concurrency), @default_max_concurrency),
       max_retries: non_negative_integer(Keyword.get(opts, :max_retries), @default_max_retries),
       ttl: ttl_value(Keyword.get(opts, :ttl, @default_ttl)),
       jobs: %{},
       queue: :queue.new(),
       inflight: %{}
     }}
  end

  @impl true
  def handle_call({:submit, sensor_id, fun}, _from, state) do
    id = make_ref()
    expiry_token = make_ref()

    job = %{
      id: id,
      sensor_id: sensor_id,
      status: :queued,
      attempts: 0,
      result: nil,
      submitted_at: System.monotonic_time(:millisecond),
      expiry_token: expiry_token,
      fun: fun
    }

    schedule_expiration(state.ttl, id, expiry_token)

    state =
      state
      |> put_in([:jobs, id], job)
      |> update_in([:queue], &:queue.in(id, &1))
      |> launch_available()

    {:reply, {:ok, id}, state}
  end

  def handle_call(:operations, _from, state) do
    result = Map.new(state.jobs, fn {id, job} -> {id, public_job(job)} end)
    {:reply, result, state}
  end

  def handle_call({:operation, id}, _from, state) do
    reply =
      case Map.fetch(state.jobs, id) do
        {:ok, job} -> {:ok, public_job(job)}
        :error -> :error
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({:operation_result, id, attempt, result}, state) do
    {:noreply, complete_attempt(state, id, attempt, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.inflight, ref) do
      {nil, _inflight} ->
        {:noreply, state}

      {%{id: id, attempt: attempt}, inflight} ->
        state = %{state | inflight: inflight}
        result = {:error, {:temporary, {:task_exit, reason}}}
        {:noreply, finish_attempt(state, id, attempt, result)}
    end
  end

  def handle_info({:expire, id, token}, state) do
    case Map.get(state.jobs, id) do
      %{expiry_token: ^token} ->
        {:noreply, %{state | jobs: Map.delete(state.jobs, id)}}

      _ ->
        {:noreply, state}
    end
  end

  defp launch_available(state) do
    if map_size(state.inflight) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, id}, queue} ->
          state = %{state | queue: queue}

          case Map.get(state.jobs, id) do
            %{status: :queued} = job ->
              attempt = job.attempts + 1
              parent = self()
              fun = job.fun

              {pid, ref} =
                spawn_monitor(fn ->
                  result = execute(fun)
                  send(parent, {:operation_result, id, attempt, result})
                end)

              job = %{job | status: :running, attempts: attempt}
              inflight = Map.put(state.inflight, ref, %{id: id, attempt: attempt, pid: pid})

              launch_available(%{
                state
                | jobs: Map.put(state.jobs, id, job),
                  inflight: inflight
              })

            _ ->
              launch_available(state)
          end
      end
    end
  end

  defp execute(fun) do
    fun.()
  rescue
    exception -> {:error, {:temporary, {:exception, exception}}}
  catch
    kind, reason -> {:error, {:temporary, {:caught, kind, reason}}}
  end

  defp complete_attempt(state, id, attempt, result) do
    {matching, remaining} =
      Enum.reduce(state.inflight, {nil, %{}}, fn {ref, entry}, {found, acc} ->
        if entry.id == id and entry.attempt == attempt do
          {ref, acc}
        else
          {found, Map.put(acc, ref, entry)}
        end
      end)

    if matching do
      Process.demonitor(matching, [:flush])
      state = %{state | inflight: remaining}
      finish_attempt(state, id, attempt, result)
    else
      state
    end
  end

  defp finish_attempt(state, id, attempt, result) do
    case Map.get(state.jobs, id) do
      %{status: :running, attempts: ^attempt} = job ->
        case result do
          {:error, {:temporary, _reason}} when attempt <= state.max_retries ->
            job = %{job | status: :queued, result: result}

            state
            |> put_in([:jobs, id], job)
            |> update_in([:queue], &:queue.in(id, &1))
            |> launch_available()

          _ ->
            status = if match?({:error, _}, result), do: :failed, else: :completed
            job = %{job | status: status, result: result}

            state
            |> put_in([:jobs, id], job)
            |> launch_available()
        end

      _ ->
        launch_available(state)
    end
  end

  defp schedule_expiration(:infinity, _id, _token), do: :ok

  defp schedule_expiration(ttl, id, token) do
    Process.send_after(self(), {:expire, id, token}, ttl)
    :ok
  end

  defp public_job(job) do
    Map.take(job, [:id, :sensor_id, :status, :attempts, :result, :submitted_at])
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp non_negative_integer(value, _default) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value, default), do: default

  defp ttl_value(:infinity), do: :infinity
  defp ttl_value(value) when is_integer(value) and value >= 0, do: value
  defp ttl_value(_value), do: @default_ttl
end
