# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Impedir atualizações parcialmente concluídas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule AtomicCache do
  use GenServer

  defstruct data: %{},
            version: 0,
            max_concurrency: 1,
            running: %{},
            queue: :queue.new()

  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def get(server, key, default \\ nil) do
    GenServer.call(server, {:get, key, default})
  end

  def all(server) do
    GenServer.call(server, :all)
  end

  def put(server, key, value) do
    transact(server, fn data -> Map.put(data, key, value) end)
  end

  def delete(server, key) do
    transact(server, fn data -> Map.delete(data, key) end)
  end

  def clear(server) do
    transact(server, fn _data -> %{} end)
  end

  def transact(server, fun) when is_function(fun, 1) do
    GenServer.call(server, {:transact, fun}, :infinity)
  end

  def transact(_server, _fun), do: {:error, :invalid_callback}

  def status(server) do
    GenServer.call(server, :status)
  end

  @impl true
  def init(opts) do
    data = Keyword.get(opts, :initial_data, %{})
    max_concurrency = Keyword.get(opts, :max_concurrency, 1)

    cond do
      not is_map(data) ->
        {:stop, {:invalid_initial_data, data}}

      not (is_integer(max_concurrency) and max_concurrency > 0) ->
        {:stop, {:invalid_max_concurrency, max_concurrency}}

      true ->
        {:ok, %__MODULE__{data: data, max_concurrency: max_concurrency}}
    end
  end

  @impl true
  def handle_call({:get, key, default}, _from, state) do
    {:reply, Map.get(state.data, key, default), state}
  end

  def handle_call(:all, _from, state) do
    {:reply, state.data, state}
  end

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       version: state.version,
       max_concurrency: state.max_concurrency,
       active: map_size(state.running),
       queued: :queue.len(state.queue)
     }, state}
  end

  def handle_call({:transact, fun}, from, state) do
    job = %{ref: make_ref(), from: from, fun: fun}
    state = %{state | queue: :queue.in(job, state.queue)}
    {:noreply, dispatch(state)}
  end

  @impl true
  def handle_info({:transaction_result, job_ref, outcome}, state) do
    case Map.pop(state.running, job_ref) do
      {nil, _running} ->
        {:noreply, state}

      {job, running} ->
        Process.demonitor(job.monitor_ref, [:flush])

        {reply, state} =
          case outcome do
            {:ok, new_data, result} when job.version == state.version ->
              {{:ok, result},
               %{state | data: new_data, version: state.version + 1, running: running}}

            {:ok, _new_data, _result} ->
              {{:error, :conflict}, %{state | running: running}}

            {:error, reason} ->
              {{:error, reason}, %{state | running: running}}
          end

        GenServer.reply(job.from, reply)
        {:noreply, dispatch(state)}
    end
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, state) do
    case Enum.find(state.running, fn {_job_ref, job} ->
           job.monitor_ref == monitor_ref
         end) do
      nil ->
        {:noreply, state}

      {job_ref, job} ->
        state = %{state | running: Map.delete(state.running, job_ref)}
        GenServer.reply(job.from, {:error, {:worker_down, reason}})
        {:noreply, dispatch(state)}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.running, fn {_job_ref, job} ->
      Process.exit(job.pid, :kill)
    end)

    :ok
  end

  defp dispatch(state) do
    if map_size(state.running) < state.max_concurrency do
      case :queue.out(state.queue) do
        {{:value, queued_job}, queue} ->
          parent = self()
          snapshot = state.data
          version = state.version

          {pid, monitor_ref} =
            spawn_monitor(fn ->
              outcome = run_transaction(queued_job.fun, snapshot)
              send(parent, {:transaction_result, queued_job.ref, outcome})
            end)

          job = %{
            from: queued_job.from,
            pid: pid,
            monitor_ref: monitor_ref,
            version: version
          }

          state = %{
            state
            | queue: queue,
              running: Map.put(state.running, queued_job.ref, job)
          }

          dispatch(state)

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end

  defp run_transaction(fun, snapshot) do
    try do
      normalize_result(fun.(snapshot))
    rescue
      error ->
        {:error, {:callback_exception, error.__struct__, Exception.message(error)}}
    catch
      kind, reason ->
        {:error, {:callback_catch, kind, reason}}
    end
  end

  defp normalize_result({:ok, data, result}) when is_map(data),
    do: {:ok, data, result}

  defp normalize_result({:error, reason}), do: {:error, reason}
  defp normalize_result(data) when is_map(data), do: {:ok, data, :ok}

  defp normalize_result(other),
    do: {:error, {:invalid_transaction_result, other}}
end
