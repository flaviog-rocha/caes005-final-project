# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir comunicação entre diferentes processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Implementar expiração automática de registros.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Ecommerce.Orders do
  @moduledoc false

  use Supervisor

  alias Ecommerce.Orders.{Recovery, Store, Worker}

  @registry Ecommerce.Orders.Registry
  @order_supervisor Ecommerce.Orders.DynamicSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path =
      Keyword.get(
        opts,
        :path,
        Path.join(File.cwd!(), "orders.dets")
      )

    children = [
      {Registry, keys: :unique, name: @registry},
      {Store, path: path},
      {DynamicSupervisor,
       strategy: :one_for_one,
       name: @order_supervisor,
       max_restarts: 100,
       max_seconds: 5},
      Recovery
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def create(id, data, ttl_ms) when is_integer(ttl_ms) and ttl_ms > 0 do
    record = %{
      id: id,
      data: data,
      expires_at: System.system_time(:millisecond) + ttl_ms
    }

    with :ok <- Store.insert_new(id, record),
         {:ok, _pid} <- ensure_started(id) do
      {:ok, record}
    end
  end

  def create(_id, _data, _ttl_ms), do: {:error, :invalid_ttl}

  def get(id) do
    with {:ok, pid} <- ensure_started(id) do
      safe_call(pid, :get)
    end
  end

  def update(id, data, ttl_ms \\ :keep)

  def update(id, data, ttl_ms)
      when ttl_ms == :keep or (is_integer(ttl_ms) and ttl_ms > 0) do
    with {:ok, pid} <- ensure_started(id) do
      safe_call(pid, {:update, data, ttl_ms})
    end
  end

  def update(_id, _data, _ttl_ms), do: {:error, :invalid_ttl}

  def delete(id) do
    with {:ok, pid} <- ensure_started(id) do
      safe_call(pid, :delete)
    end
  end

  def list do
    Store.all()
  end

  defp ensure_started(id) do
    case Registry.lookup(@registry, id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case Store.fetch(id) do
          {:ok, _record} ->
            start_worker(id)

          :error ->
            {:error, :not_found}
        end
    end
  end

  defp start_worker(id) do
    case DynamicSupervisor.start_child(
           @order_supervisor,
           Worker.child_spec(id)
         ) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, :already_present} ->
        case Registry.lookup(@registry, id) do
          [{pid, _}] -> {:ok, pid}
          [] -> {:error, :not_running}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp safe_call(pid, message) do
    try do
      GenServer.call(pid, message)
    catch
      :exit, reason -> {:error, {:process_unavailable, reason}}
    end
  end
end

defmodule Ecommerce.Orders.Store do
  @moduledoc false

  use GenServer

  @table __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, @table} -> {:ok, nil}
      {:error, reason} -> {:stop, reason}
    end
  end

  def insert_new(id, record), do: GenServer.call(__MODULE__, {:insert_new, id, record})
  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})
  def put(id, record), do: GenServer.call(__MODULE__, {:put, id, record})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
  def all, do: GenServer.call(__MODULE__, :all)

  @impl true
  def handle_call({:insert_new, id, record}, _from, state) do
    result =
      case :dets.lookup(@table, id) do
        [] ->
          persist(:dets.insert(@table, {id, record}))

        _ ->
          {:error, :already_exists}
      end

    {:reply, result, state}
  end

  def handle_call({:fetch, id}, _from, state) do
    result =
      case :dets.lookup(@table, id) do
        [{^id, record}] -> {:ok, record}
        [] -> :error
      end

    {:reply, result, state}
  end

  def handle_call({:put, id, record}, _from, state) do
    {:reply, persist(:dets.insert(@table, {id, record})), state}
  end

  def handle_call({:delete, id}, _from, state) do
    result = persist(:dets.delete(@table, id))
    {:reply, result, state}
  end

  def handle_call(:all, _from, state) do
    records =
      :dets.foldl(
        fn {_id, record}, acc -> [record | acc] end,
        [],
        @table
      )

    {:reply, records, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp persist(:ok) do
    case :dets.sync(@table) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist({:error, reason}), do: {:error, reason}
end

defmodule Ecommerce.Orders.Worker do
  @moduledoc false

  use GenServer

  @registry Ecommerce.Orders.Registry
  @store Ecommerce.Orders.Store

  def child_spec(id) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [id]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(id) do
    GenServer.start_link(
      __MODULE__,
      id,
      name: {:via, Registry, {@registry, id}}
    )
  end

  @impl true
  def init(id) do
    case @store.fetch(id) do
      {:ok, record} ->
        {:ok, schedule_expiration(%{id: id, record: record, timer: nil, token: nil})}

      :error ->
        :ignore
    end
  end

  @impl true
  def handle_call(:get, _from, state) do
    {:reply, {:ok, state.record}, state}
  end

  def handle_call({:update, data, ttl_ms}, _from, state) do
    expires_at =
      case ttl_ms do
        :keep -> state.record.expires_at
        value -> System.system_time(:millisecond) + value
      end

    record = %{state.record | data: data, expires_at: expires_at}

    case @store.put(state.id, record) do
      :ok ->
        state = %{state | record: record} |> schedule_expiration()
        {:reply, {:ok, record}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:delete, _from, state) do
    case @store.delete(state.id) do
      :ok -> {:stop, :normal, :ok, cancel_timer(state)}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info({:expire, token}, %{token: token} = state) do
    if state.record.expires_at <= System.system_time(:millisecond) do
      case @store.delete(state.id) do
        :ok ->
          {:stop, :normal, state |> cancel_timer()}

        {:error, _reason} ->
          {:noreply, retry_expiration(state)}
      end
    else
      {:noreply, schedule_expiration(state)}
    end
  end

  def handle_info({:expire, _stale_token}, state), do: {:noreply, state}

  defp schedule_expiration(state) do
    state = cancel_timer(state)
    token = make_ref()
    remaining = max(state.record.expires_at - System.system_time(:millisecond), 0)
    delay = min(remaining, 4_294_967_295)
    timer = Process.send_after(self(), {:expire, token}, delay)
    %{state | timer: timer, token: token}
  end

  defp retry_expiration(state) do
    state = cancel_timer(state)
    token = make_ref()
    timer = Process.send_after(self(), {:expire, token}, 1_000)
    %{state | timer: timer, token: token}
  end

  defp cancel_timer(%{timer: nil} = state), do: %{state | token: nil}

  defp cancel_timer(state) do
    Process.cancel_timer(state.timer)
    %{state | timer: nil, token: nil}
  end
end

defmodule Ecommerce.Orders.Recovery do
  @moduledoc false

  use GenServer

  alias Ecommerce.Orders.{Store, Worker}

  @order_supervisor Ecommerce.Orders.DynamicSupervisor

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    send(self(), :recover)
    {:ok, nil}
  end

  @impl true
  def handle_info(:recover, state) do
    Store.all()
    |> Enum.each(fn record ->
      DynamicSupervisor.start_child(
        @order_supervisor,
        Worker.child_spec(record.id)
      )
    end)

    {:noreply, state}
  end
end
