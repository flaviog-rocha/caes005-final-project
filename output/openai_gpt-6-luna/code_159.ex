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

  @moduledoc """
  Cache com transações atômicas, leituras concorrentes limitadas e monitoramento
  de processos que executam operações.
  """

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def get(cache, key), do: GenServer.call(cache, {:get, key})

  def put(cache, key, value) do
    case transact(cache, fn data ->
           {:commit, :ok, Map.put(data, key, value)}
         end) do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  def delete(cache, key) do
    transact(cache, fn data ->
      {:commit, Map.fetch(data, key), Map.delete(data, key)}
    end)
  end

  def read(cache, fun) when is_function(fun, 1) do
    GenServer.call(cache, {:operation, :read, fun}, :infinity)
  end

  def transact(cache, fun) when is_function(fun, 1) do
    GenServer.call(cache, {:operation, :transaction, fun}, :infinity)
  end

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if not (is_integer(max_concurrency) and max_concurrency > 0) do
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    else
      {:ok,
       %{
         data: Keyword.get(opts, :initial_data, %{}),
         max_concurrency: max_concurrency,
         queue: :queue.new(),
         workers: %{},
         transaction_ref: nil
       }}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    reply =
      case Map.fetch(state.data, key) do
        {:ok, value} -> {:ok, value}
        :error -> :error
      end

    {:reply, reply, state}
  end

  def handle_call({:operation, kind, fun}, from, state) do
    queue = :queue.in({kind, from, fun}, state.queue)
    {:noreply, schedule(%{state | queue: queue})}
  end

  @impl true
  def handle_info({:worker_result, request_ref, result}, state) do
    case Map.pop(state.workers, request_ref) do
      {nil, _workers} ->
        {:noreply, state}

      {worker, workers} ->
        Process.demonitor(worker.monitor, [:flush])

        state = %{state | workers: workers}

        state =
          case worker.kind do
            :read ->
              GenServer.reply(worker.from, result)
              state

            :transaction ->
              finish_transaction(state, worker.from, result)
          end

        transaction_ref =
          if worker.kind == :transaction, do: nil, else: state.transaction_ref

        {:noreply, schedule(%{state | transaction_ref: transaction_ref})}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Enum.find(state.workers, fn {_ref, worker} -> worker.monitor == monitor end) do
      nil ->
        {:noreply, state}

      {request_ref, worker} ->
        GenServer.reply(worker.from, {:error, {:worker_terminated, reason}})

        workers = Map.delete(state.workers, request_ref)

        transaction_ref =
          if worker.kind == :transaction, do: nil, else: state.transaction_ref

        {:noreply, schedule(%{state | workers: workers, transaction_ref: transaction_ref})}
    end
  end

  defp finish_transaction(state, from, {:ok, {:commit, result, new_data}})
       when is_map(new_data) do
    GenServer.reply(from, {:ok, result})
    %{state | data: new_data}
  end

  defp finish_transaction(state, from, {:ok, {:abort, reason}}) do
    GenServer.reply(from, {:error, {:aborted, reason}})
    state
  end

  defp finish_transaction(state, from, {:ok, other}) do
    GenServer.reply(from, {:error, {:invalid_transaction_result, other}})
    state
  end

  defp finish_transaction(state, from, {:error, reason}) do
    GenServer.reply(from, {:error, reason})
    state
  end

  defp schedule(%{transaction_ref: ref} = state) when not is_nil(ref), do: state

  defp schedule(state) do
    cond do
      map_size(state.workers) >= state.max_concurrency ->
        state

      true ->
        case :queue.peek(state.queue) do
          :empty ->
            state

          {:value, {:read, _from, _fun}} ->
            start_next(state)

          {:value, {:transaction, _from, _fun}} when map_size(state.workers) == 0 ->
            start_next(state)

          _ ->
            state
        end
    end
  end

  defp start_next(state) do
    {{:value, {kind, from, fun}}, queue} = :queue.out(state.queue)
    request_ref = make_ref()
    parent = self()
    snapshot = state.data

    {_pid, monitor} =
      spawn_monitor(fn ->
        result = invoke(fun, snapshot)
        send(parent, {:worker_result, request_ref, result})
      end)

    worker = %{kind: kind, from: from, monitor: monitor}
    workers = Map.put(state.workers, request_ref, worker)

    state = %{state | queue: queue, workers: workers}

    if kind == :transaction do
      %{state | transaction_ref: request_ref}
    else
      schedule(state)
    end
  end

  defp invoke(fun, snapshot) do
    try do
      {:ok, fun.(snapshot)}
    rescue
      exception -> {:error, {:exception, exception, __STACKTRACE__}}
    catch
      kind, reason -> {:error, {:caught, kind, reason, __STACKTRACE__}}
    end
  end
end
