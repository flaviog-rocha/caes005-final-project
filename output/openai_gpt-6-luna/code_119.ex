# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LoanManager do
  use GenServer

  @type operation ::
          {:checkout, entity_id :: term(), book_id :: term()}
          | {:return, entity_id :: term(), book_id :: term()}

  def start_link(opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    retries = Keyword.get(opts, :retries, 2)
    failure_injector = Keyword.get(opts, :failure_injector, fn _entity_id, _operation -> :ok end)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, "max_concurrency must be a positive integer"
    end

    unless is_integer(retries) and retries >= 0 do
      raise ArgumentError, "retries must be a non-negative integer"
    end

    GenServer.start_link(__MODULE__, {max_concurrency, retries, failure_injector}, opts)
  end

  @spec process(GenServer.server(), Enumerable.t()) :: [term()]
  def process(manager, operations) do
    {max_concurrency, retries} = GenServer.call(manager, :settings)

    operations
    |> Stream.map(& &1)
    |> Task.async_stream(
      fn operation -> execute(manager, operation, retries) end,
      max_concurrency: max_concurrency,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
  end

  @impl true
  def init({max_concurrency, retries, failure_injector}) do
    {:ok,
     %{
       max_concurrency: max_concurrency,
       retries: retries,
       failure_injector: failure_injector,
       workers: %{}
     }}
  end

  @impl true
  def handle_call(:settings, _from, state) do
    {:reply, {state.max_concurrency, state.retries}, state}
  end

  def handle_call({:worker, entity_id}, _from, state) do
    case Map.get(state.workers, entity_id) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          {:reply, pid, state}
        else
          start_worker(entity_id, state)
        end

      nil ->
        start_worker(entity_id, state)
    end
  end

  defp start_worker(entity_id, state) do
    case GenServer.start(
           LoanManager.Worker,
           {entity_id, state.failure_injector}
         ) do
      {:ok, pid} ->
        {:reply, pid, put_in(state.workers[entity_id], pid)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp execute(manager, {:checkout, entity_id, book_id} = operation, retries) do
    perform_with_retry(manager, entity_id, operation, retries, fn pid ->
      GenServer.call(pid, {:checkout, book_id})
    end)
  end

  defp execute(manager, {:return, entity_id, book_id} = operation, retries) do
    perform_with_retry(manager, entity_id, operation, retries, fn pid ->
      GenServer.call(pid, {:return, book_id})
    end)
  end

  defp execute(_manager, operation, _retries), do: {:error, {:invalid_operation, operation}}

  defp perform_with_retry(manager, entity_id, operation, retries, request) do
    result =
      try do
        case GenServer.call(manager, {:worker, entity_id}) do
          pid when is_pid(pid) -> request.(pid)
          {:error, reason} -> {:error, reason}
        end
      catch
        :exit, _reason -> {:error, :temporary}
      end

    case result do
      {:error, :temporary} when retries > 0 ->
        perform_with_retry(manager, entity_id, operation, retries - 1, request)

      other ->
        other
    end
  end
end

defmodule LoanManager.Worker do
  use GenServer

  def init({entity_id, failure_injector}) do
    {:ok,
     %{
       entity_id: entity_id,
       failure_injector: failure_injector,
       loans: MapSet.new()
     }}
  end

  def handle_call({:checkout, book_id}, _from, state) do
    operation = {:checkout, state.entity_id, book_id}

    if temporary_failure?(state.failure_injector, state.entity_id, operation) do
      {:reply, {:error, :temporary}, state}
    else
      if MapSet.member?(state.loans, book_id) do
        {:reply, {:error, :already_checked_out}, state}
      else
        {:reply, :ok, %{state | loans: MapSet.put(state.loans, book_id)}}
      end
    end
  end

  def handle_call({:return, book_id}, _from, state) do
    operation = {:return, state.entity_id, book_id}

    if temporary_failure?(state.failure_injector, state.entity_id, operation) do
      {:reply, {:error, :temporary}, state}
    else
      if MapSet.member?(state.loans, book_id) do
        {:reply, :ok, %{state | loans: MapSet.delete(state.loans, book_id)}}
      else
        {:reply, {:error, :not_checked_out}, state}
      end
    end
  end

  defp temporary_failure?(failure_injector, entity_id, operation) do
    try do
      failure_injector.(entity_id, operation) in [:temporary, {:error, :temporary}]
    rescue
      _ -> true
    catch
      _, _ -> true
    end
  end
end
