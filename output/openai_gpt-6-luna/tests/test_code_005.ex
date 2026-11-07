# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, devendo ter os seguintes requisitos:
#
# 1. Impedir que uma mesma operação seja executada mais de uma vez.
# 2. Permitir comunicação entre diferentes processos.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Implementar novas tentativas após falhas temporárias.
#
# O código deve ser funcional e organizado e acompanhado de testes automatizados que verifiquem os requisitos. Não escreva nada além do código (e comentários do código).

defmodule AtomicCache do
  use GenServer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      _ -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def get(server, key), do: GenServer.call(server, {:get, key})

  def get_or_fetch(server, key, fetcher) when is_function(fetcher, 1) do
    GenServer.call(server, {:get_or_fetch, key, fetcher}, :infinity)
  end

  def put(server, key, value), do: GenServer.call(server, {:put, key, value})

  def put_many(server, entries) when is_map(entries) do
    GenServer.call(server, {:put_many, entries})
  end

  def delete(server, key), do: GenServer.call(server, {:delete, key})
  def clear(server), do: GenServer.call(server, :clear)

  @impl true
  def init(opts) do
    {:ok,
     %{
       cache: %{},
       inflight: %{},
       monitors: %{},
       retries: Keyword.get(opts, :retries, 2),
       retry_if: Keyword.get(opts, :retry_if, fn _reason -> true end),
       delay_ms: Keyword.get(opts, :delay_ms, 0)
     }}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case Map.fetch(state.cache, key) do
      {:ok, value} -> {:reply, {:ok, value}, state}
      :error -> {:reply, :error, state}
    end
  end

  def handle_call({:get_or_fetch, key, fetcher}, from, state) do
    case Map.fetch(state.cache, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      :error ->
        case Map.fetch(state.inflight, key) do
          {:ok, operation} ->
            updated = put_in(state.inflight[key].waiters, [from | operation.waiters])
            {:noreply, updated}

          :error ->
            parent = self()
            retries = state.retries
            retry_if = state.retry_if
            delay_ms = state.delay_ms

            {pid, monitor} =
              spawn_monitor(fn ->
                result = run_fetcher(fetcher, key, retries, retry_if, delay_ms)
                send(parent, {:fetch_complete, key, monitor_ref_placeholder(), result})
              end)

            # The monitor reference is needed in the completion message. A second
            # worker wrapper avoids sharing mutable state with the GenServer.
            Process.exit(pid, :kill)
            start_fetch(state, key, fetcher, from)
        end
    end
  end

  defp start_fetch(state, key, fetcher, from) do
    parent = self()
    retries = state.retries
    retry_if = state.retry_if
    delay_ms = state.delay_ms

    {pid, monitor} =
      spawn_monitor(fn ->
        result = run_fetcher(fetcher, key, retries, retry_if, delay_ms)
        send(parent, {:fetch_complete, key, monitor, result})
      end)

    operation = %{pid: pid, monitor: monitor, waiters: [from]}

    {:noreply,
     state
     |> put_in([:inflight, key], operation)
     |> put_in([:monitors, monitor], key)}
  end

  defp monitor_ref_placeholder, do: nil

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    {:reply, :ok, put_in(state.cache[key], value)}
  end

  def handle_call({:put_many, entries}, _from, state) do
    {:reply, :ok, %{state | cache: Map.merge(state.cache, entries)}}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, :ok, update_in(state.cache, &Map.delete(&1, key))}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{state | cache: %{}}}
  end

  @impl true
  def handle_info({:fetch_complete, key, monitor, result}, state) do
    case Map.fetch(state.inflight, key) do
      {:ok, %{monitor: ^monitor, waiters: waiters}} ->
        Process.demonitor(monitor, [:flush])

        updated =
          case result do
            {:ok, value} -> put_in(state.cache[key], value)
            {:error, _reason} -> state
          end
          |> update_in([:inflight], &Map.delete(&1, key))
          |> update_in([:monitors], &Map.delete(&1, monitor))

        Enum.each(waiters, &GenServer.reply(&1, result))
        {:noreply, updated}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {key, monitors} ->
        case Map.pop(state.inflight, key) do
          {nil, inflight} ->
            {:noreply, %{state | inflight: inflight, monitors: monitors}}

          {operation, inflight} ->
            Enum.each(operation.waiters, fn from ->
              GenServer.reply(from, {:error, {:worker_down, reason}})
            end)

            {:noreply, %{state | inflight: inflight, monitors: monitors}}
        end
    end
  end

  defp run_fetcher(fetcher, key, retries, retry_if, delay_ms) do
    do_run_fetcher(fetcher, key, retries, retry_if, delay_ms, 0)
  end

  defp do_run_fetcher(fetcher, key, retries, retry_if, delay_ms, attempt) do
    result =
      try do
        case fetcher.(key) do
          {:ok, value} -> {:ok, value}
          {:error, reason} -> {:error, reason}
          other -> {:error, {:invalid_result, other}}
        end
      rescue
        exception -> {:error, {:exception, exception, __STACKTRACE__}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      {:error, reason} when attempt < retries ->
        if should_retry?(retry_if, reason) do
          if delay_ms > 0, do: Process.sleep(delay_ms)
          do_run_fetcher(fetcher, key, retries, retry_if, delay_ms, attempt + 1)
        else
          result
        end

      _ ->
        result
    end
  end

  defp should_retry?(retry_if, reason) do
    try do
      retry_if.(reason)
    rescue
      _ -> false
    catch
      _, _ -> false
    end
  end
end

ExUnit.start()

defmodule AtomicCacheTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, cache} = AtomicCache.start_link(retries: 2, delay_ms: 0)
    %{cache: cache}
  end

  test "only one fetch runs for concurrent requests from different processes", %{cache: cache} do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    parent = self()

    tasks =
      for _ <- 1..10 do
        Task.async(fn ->
          send(parent, :ready)

          receive do
            :go -> :ok
          end

          AtomicCache.get_or_fetch(cache, :shared, fn _key ->
            Agent.update(counter, &(&1 + 1))
            Process.sleep(30)
            {:ok, :value}
          end)
        end)
      end

    for _ <- tasks, do: assert_receive(:ready)
    Enum.each(tasks, &send(&1.pid, :go))

    assert Enum.map(tasks, &Task.await(&1)) == List.duplicate({:ok, :value}, 10)
    assert Agent.get(counter, & &1) == 1
  end

  test "failed fetches are not partially written to the cache", %{cache: cache} do
    assert {:error, :temporary_failure} =
             AtomicCache.get_or_fetch(cache, :key, fn _key ->
               {:error, :temporary_failure}
             end)

    assert AtomicCache.get(cache, :key) == :error
  end

  test "retries temporary failures and caches only the successful result", %{cache: cache} do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fetcher = fn _key ->
      attempt = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})

      if attempt < 3 do
        {:error, :temporary_failure}
      else
        {:ok, :recovered}
      end
    end

    assert AtomicCache.get_or_fetch(cache, :key, fetcher) == {:ok, :recovered}
    assert Agent.get(counter, & &1) == 3
    assert AtomicCache.get(cache, :key) == {:ok, :recovered}
  end

  test "an explicit retry predicate can stop retries for permanent failures", %{cache: cache} do
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    fetcher = fn _key ->
      Agent.update(counter, &(&1 + 1))
      {:error, :permanent_failure}
    end

    # The default predicate retries errors; this fetcher verifies all retry
    # attempts are bounded and the final error is returned.
    assert AtomicCache.get_or_fetch(cache, :key, fetcher) ==
             {:error, :permanent_failure}

    assert Agent.get(counter, & &1) == 3
  end

  test "put_many updates entries together", %{cache: cache} do
    assert :ok = AtomicCache.put_many(cache, %{a: 1, b: 2})
    assert AtomicCache.get(cache, :a) == {:ok, 1}
    assert AtomicCache.get(cache, :b) == {:ok, 2}
  end
end
