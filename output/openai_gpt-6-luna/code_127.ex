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

  @public_fields [
    :id,
    :sensor_id,
    :status,
    :attempt,
    :submitted_at,
    :started_at,
    :finished_at,
    :result,
    :last_error
  ]

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    server_opts = if is_nil(name), do: [], else: [name: name]
    GenServer.start_link(__MODULE__, opts, server_opts)
  end

  def submit(server, sensor_id, fun, opts \\ []) do
    with true <- is_function(fun, 0) || {:error, :invalid_operation},
         true <- Keyword.keyword?(opts) || {:error, :invalid_options},
         {:ok, operation_opts} <- validate_operation_opts(opts) do
      GenServer.call(server, {:submit, sensor_id, fun, operation_opts})
    end
  end

  def status(server, id), do: GenServer.call(server, {:status, id})

  def in_progress(server), do: GenServer.call(server, :in_progress)

  @impl true
  def init(opts) do
    config = %{
      max_concurrency: positive!(Keyword.get(opts, :max_concurrency, 10), :max_concurrency),
      retries: non_negative!(Keyword.get(opts, :retries, 2), :retries),
      retry_delay: non_negative!(Keyword.get(opts, :retry_delay, 1_000), :retry_delay),
      ttl: non_negative!(Keyword.get(opts, :ttl, 60_000), :ttl)
    }

    {:ok, %{config: config, records: %{}, queue: :queue.new(), running: %{}}}
  end

  @impl true
  def handle_call({:submit, sensor_id, fun, operation_opts}, _from, state) do
    id = make_ref()
    now = now()

    record =
      Map.merge(
        %{
          id: id,
          sensor_id: sensor_id,
          status: :queued,
          attempt: 0,
          submitted_at: now,
          started_at: nil,
          finished_at: nil,
          result: nil,
          last_error: nil,
          fun: fun,
          expiration: nil
        },
        operation_opts
      )

    state =
      state
      |> put_in([:records, id], record)
      |> update_in([:queue], &:queue.in(id, &1))
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.records, id) do
        {:ok, record} -> {:ok, public_record(record)}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call(:in_progress, _from, state) do
    records =
      state.records
      |> Map.values()
      |> Enum.reject(&(&1.status in [:completed, :failed]))
      |> Enum.sort_by(& &1.submitted_at)
      |> Enum.map(&public_record/1)

    {:reply, records, state}
  end

  @impl true
  def handle_info({:operation_result, id, pid, outcome}, state) do
    case Map.get(state.running, id) do
      %{pid: ^pid, ref: ref} ->
        Process.demonitor(ref, [:flush])

        state =
          state
          |> update_in([:running], &Map.delete(&1, id))
          |> finish_attempt(id, outcome)
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_id, task} -> task.ref == ref end) do
      {id, _task} ->
        state =
          state
          |> update_in([:running], &Map.delete(&1, id))
          |> finish_attempt(id, {:error, {:temporary, {:process_down, reason}}})
          |> dispatch()

        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, id}, state) do
    case Map.get(state.records, id) do
      %{status: :retrying} ->
        state =
          state
          |> update_in([:records, id, :status], fn _ -> :queued end)
          |> update_in([:queue], &:queue.in(id, &1))
          |> dispatch()

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:expire, id, token}, state) do
    case Map.get(state.records, id) do
      %{expiration: ^token, status: status} when status in [:completed, :failed] ->
        {:noreply, update_in(state.records, &Map.delete(&1, id))}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp dispatch(state) do
    if map_size(state.running) >= state.config.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, id}, queue} ->
          state = %{state | queue: queue}

          case Map.get(state.records, id) do
            %{status: :queued} = record ->
              parent = self()
              attempt = record.attempt + 1

              {pid, ref} =
                spawn_monitor(fn ->
                  send(parent, {:operation_result, id, self(), execute(record.fun)})
                end)

              record =
                record
                |> Map.put(:status, :running)
                |> Map.put(:attempt, attempt)
                |> Map.put(:started_at, now())

              state =
                state
                |> put_in([:records, id], record)
                |> put_in([:running, id], %{pid: pid, ref: ref})

              dispatch(state)

            _ ->
              dispatch(state)
          end
      end
    end
  end

  defp execute(fun) do
    fun.()
  rescue
    error -> {:error, {:temporary, {:exception, error, __STACKTRACE__}}}
  catch
    kind, reason -> {:error, {:temporary, {kind, reason}}}
  end

  defp finish_attempt(state, id, outcome) do
    record = Map.fetch!(state.records, id)

    if temporary_failure?(outcome) and record.attempt <= record.retries do
      Process.send_after(self(), {:retry, id}, record.retry_delay)

      record =
        record
        |> Map.put(:status, :retrying)
        |> Map.put(:result, nil)
        |> Map.put(:last_error, outcome)

      put_in(state.records[id], record)
    else
      status = if match?({:error, _}, outcome), do: :failed, else: :completed
      token = make_ref()
      Process.send_after(self(), {:expire, id, token}, record.ttl)

      record =
        record
        |> Map.put(:status, status)
        |> Map.put(:result, outcome)
        |> Map.put(:last_error, if(status == :failed, do: outcome, else: nil))
        |> Map.put(:finished_at, now())
        |> Map.put(:expiration, token)

      put_in(state.records[id], record)
    end
  end

  defp temporary_failure?({:error, :temporary}), do: true
  defp temporary_failure?({:error, {:temporary, _reason}}), do: true
  defp temporary_failure?(_), do: false

  defp public_record(record), do: Map.take(record, @public_fields)

  defp validate_operation_opts(opts) do
    with {:ok, retries} <- optional_non_negative(opts, :retries),
         {:ok, retry_delay} <- optional_non_negative(opts, :retry_delay),
         {:ok, ttl} <- optional_non_negative(opts, :ttl) do
      {:ok,
       %{
         retries: retries,
         retry_delay: retry_delay,
         ttl: ttl
       }}
    end
  end

  defp optional_non_negative(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> {:ok, nil}
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  defp positive!(value, _key) when is_integer(value) and value > 0, do: value
  defp positive!(_value, key), do: raise(ArgumentError, "invalid option: #{key}")

  defp non_negative!(value, _key) when is_integer(value) and value >= 0, do: value
  defp non_negative!(_value, key), do: raise(ArgumentError, "invalid option: #{key}")

  defp now, do: System.monotonic_time(:millisecond)
end
