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

# arquivo: order_system.exs
# Execução: elixir order_system.exs
# Requisitos atendidos:
#   1. Detecção de encerramentos inesperados (Process.monitor + handle_info :DOWN)
#   2. Comunicação entre processos (Registry :duplicate como pub/sub, mensagens GenServer)
#   3. Persistência em disco (DETS) com recuperação na reinicialização
#   4. Expiração automática de registros (timers com tempo absoluto persistido)
#   5. Processamento independente por entidade (um processo por pedido)

# ---------------------------------------------------------------------------
# Pub/Sub baseado em Registry (somente biblioteca padrão)
# ---------------------------------------------------------------------------
defmodule OrderSystem.PubSub do
  @moduledoc "Publicação/assinatura entre processos usando Registry com chaves duplicadas."

  @registry :order_system_pubsub

  # Inscreve o processo chamador em um tópico. Ao morrer, a inscrição é removida automaticamente.
  def subscribe(topic), do: Registry.register(@registry, topic, nil)

  # Remove a inscrição do processo chamador.
  def unsubscribe(topic), do: Registry.unregister(@registry, topic)

  # Envia a mensagem para todos os inscritos do tópico.
  def publish(topic, msg) do
    Registry.dispatch(@registry, topic, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:pubsub, topic, msg})
    end)
  end
end

# ---------------------------------------------------------------------------
# Persistência em DETS (armazenamento em disco da stdlib/OTP)
# ---------------------------------------------------------------------------
defmodule OrderSystem.Store do
  @moduledoc "Único dono da tabela DETS; serializa escritas e leituras dos pedidos."
  use GenServer

  @table :order_system_store

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def put(record), do: GenServer.call(__MODULE__, {:put, record})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
  def all(), do: GenServer.call(__MODULE__, :all)

  @impl true
  def init(opts) do
    path = opts |> Keyword.get(:path, "order_system.dets") |> String.to_charlist()

    case :dets.open_file(@table, file: path, type: :set) do
      {:ok, _} -> {:ok, nil}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, record}, _from, s) do
    :ok = :dets.insert(@table, {record.id, record})
    :ok = :dets.sync(@table)
    {:reply, :ok, s}
  end

  def handle_call({:get, id}, _from, s) do
    reply =
      case :dets.lookup(@table, id) do
        [{_, record}] -> record
        [] -> nil
      end

    {:reply, reply, s}
  end

  def handle_call({:delete, id}, _from, s) do
    :ok = :dets.delete(@table, id)
    :ok = :dets.sync(@table)
    {:reply, :ok, s}
  end

  def handle_call(:all, _from, s) do
    records = :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], @table)
    {:reply, records, s}
  end

  @impl true
  def terminate(_reason, _state), do: :dets.close(@table)
end

# ---------------------------------------------------------------------------
# Processo independente por pedido
# ---------------------------------------------------------------------------
defmodule OrderSystem.Order do
  @moduledoc """
  Um processo por pedido. Estado = registro persistido.
  Não é reiniciado pelo supervisor (:temporary): quem decide reiniciar é o Manager,
  que detecta a falha via monitor.
  """
  use GenServer, restart: :temporary

  alias OrderSystem.{PubSub, Store}

  @registry :order_system_orders

  def start_link(record) do
    GenServer.start_link(__MODULE__, record, name: {:via, Registry, {@registry, record.id}})
  end

  @impl true
  def init(record) do
    # Usa o instante absoluto de expiração: funciona mesmo após reinicialização.
    remaining = max(record.expires_at - System.os_time(:millisecond), 0)
    Process.send_after(self(), :expire, remaining)
    {:ok, record}
  end

  @impl true
  def handle_call(:get, _from, s), do: {:reply, {:ok, s}, s}

  def handle_call({:add_item, name, price}, _from, %{status: :open} = s) do
    s = %{s | items: [{name, price} | s.items], total: s.total + price}
    commit(s, {:item_added, s.id, name})
  end

  def handle_call(:pay, _from, %{status: :open} = s) do
    commit(%{s | status: :paid}, {:order_paid, s.id})
  end

  def handle_call(:cancel, _from, %{status: :open} = s) do
    commit(%{s | status: :cancelled}, {:order_cancelled, s.id})
  end

  # Qualquer outra operação em estado inválido
  def handle_call(_msg, _from, s), do: {:reply, {:error, :invalid_operation}, s}

  @impl true
  def handle_info(:expire, s) do
    :ok = Store.delete(s.id)
    PubSub.publish("orders", {:order_expired, s.id})
    {:stop, :normal, s}
  end

  # Persiste a mudança e notifica os interessados.
  defp commit(s, event) do
    :ok = Store.put(s)
    PubSub.publish("orders", event)
    PubSub.publish({:order, s.id}, event)
    {:reply, {:ok, s}, s}
  end
end

# ---------------------------------------------------------------------------
# Gerenciador: monitora pedidos, detecta falhas e recupera estado
# ---------------------------------------------------------------------------
defmodule OrderSystem.Manager do
  @moduledoc """
  Monitora cada processo de pedido. Em caso de encerramento anormal,
  reinicia o pedido a partir do estado persistido (até @max_restarts vezes).
  Na sua própria inicialização, recupera todos os pedidos do Store.
  """
  use GenServer
  require Logger

  alias OrderSystem.{Order, PubSub, Store}

  @sup OrderSystem.OrderSup
  @max_restarts 3

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def open(id, ttl_ms), do: GenServer.call(__MODULE__, {:open, id, ttl_ms})

  @impl true
  def init(nil) do
    # monitors: ref => id ; restarts: id => contagem de reinícios
    {:ok, %{monitors: %{}, restarts: %{}}, {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, st) do
    now = System.os_time(:millisecond)

    st =
      Enum.reduce(Store.all(), st, fn record, acc ->
        if record.expires_at <= now do
          # Expirou enquanto o sistema estava parado
          :ok = Store.delete(record.id)
          acc
        else
          start_order(acc, record)
        end
      end)

    {:noreply, st}
  end

  @impl true
  def handle_call({:open, id, ttl_ms}, _from, st) do
    if Store.get(id) do
      {:reply, {:error, :already_exists}, st}
    else
      record = %{
        id: id,
        status: :open,
        items: [],
        total: 0,
        expires_at: System.os_time(:millisecond) + ttl_ms
      }

      :ok = Store.put(record)
      st = start_order(st, record)
      PubSub.publish("orders", {:order_opened, id})
      {:reply, {:ok, record}, st}
    end
  end

  # Detecção de encerramento de processo de pedido
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, st) do
    case Map.pop(st.monitors, ref) do
      {nil, _} ->
        {:noreply, st}

      {id, monitors} ->
        st = %{st | monitors: monitors}

        if normal_exit?(reason) do
          {:noreply, st}
        else
          handle_crash(id, reason, st)
        end
    end
  end

  defp normal_exit?(:normal), do: true
  defp normal_exit?(:shutdown), do: true
  defp normal_exit?({:shutdown, _}), do: true
  defp normal_exit?(_), do: false

  defp handle_crash(id, reason, st) do
    Logger.warning("pedido #{inspect(id)} encerrou inesperadamente: #{inspect(reason)}")
    PubSub.publish("orders", {:order_crashed, id, reason})

    attempts = Map.get(st.restarts, id, 0) + 1
    st = %{st | restarts: Map.put(st.restarts, id, attempts)}

    if attempts > @max_restarts do
      PubSub.publish("orders", {:order_abandoned, id})
      {:noreply, st}
    else
      case Store.get(id) do
        nil ->
          {:noreply, st}

        record ->
          st = start_order(st, record)
          PubSub.publish("orders", {:order_restarted, id, attempts})
          {:noreply, st}
      end
    end
  end

  # Inicia (ou localiza, se já existir) o processo do pedido e o monitora
  defp start_order(st, record) do
    case DynamicSupervisor.start_child(@sup, {Order, record}) do
      {:ok, pid} ->
        monitor(st, pid, record.id)

      {:error, {:already_started, pid}} ->
        monitor(st, pid, record.id)

      {:error, reason} ->
        Logger.error("falha ao iniciar pedido #{inspect(record.id)}: #{inspect(reason)}")
        st
    end
  end

  defp monitor(st, pid, id) do
    ref = Process.monitor(pid)
    %{st | monitors: Map.put(st.monitors, ref, id)}
  end
end

# ---------------------------------------------------------------------------
# Supervisão da aplicação
# ---------------------------------------------------------------------------
defmodule OrderSystem.Supervisor do
  @moduledoc "Supervisor raiz. Estratégia :rest_for_one: se o Store cair, os dependentes reiniciam."
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: :order_system_orders},
      {Registry, keys: :duplicate, name: :order_system_pubsub},
      {OrderSystem.Store, opts},
      {DynamicSupervisor, strategy: :one_for_one, name: OrderSystem.OrderSup},
      OrderSystem.Manager
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------
defmodule OrderSystem do
  @moduledoc "Interface pública para gerenciamento de pedidos."

  alias OrderSystem.{Manager, PubSub}

  def start_link(opts \\ []), do: OrderSystem.Supervisor.start_link(opts)

  # Cria um pedido que expira após ttl_ms milissegundos
  def open_order(id, ttl_ms), do: Manager.open(id, ttl_ms)

  def get_order(id), do: call(id, :get)
  def add_item(id, name, price), do: call(id, {:add_item, name, price})
  def pay(id), do: call(id, :pay)
  def cancel(id), do: call(id, :cancel)

  # Assina eventos de um tópico ("orders" ou {:order, id})
  def subscribe(topic), do: PubSub.subscribe(topic)

  defp call(id, msg) do
    case Registry.lookup(:order_system_orders, id) do
      [{pid, _}] -> GenServer.call(pid, msg)
      [] -> {:error, :not_found}
    end
  end
end

# ---------------------------------------------------------------------------
# Demonstração (executada apenas se chamada explicitamente)
# ---------------------------------------------------------------------------
defmodule OrderSystem.Demo do
  @moduledoc "Demonstra crash/recuperação, expiração e persistência entre reinicializações."

  def run do
    path = "order_system_demo.dets"
    File.rm(path)

    {:ok, _} = OrderSystem.start_link(path: path)
    OrderSystem.subscribe("orders")

    {:ok, _} = OrderSystem.open_order("pedido-1", 60_000)
    {:ok, _} = OrderSystem.open_order("pedido-2", 1_000)

    OrderSystem.add_item("pedido-1", "teclado", 150)
    OrderSystem.add_item("pedido-1", "mouse", 80)
    IO.inspect(OrderSystem.get_order("pedido-1"), label: "antes do crash")

    # Simula encerramento inesperado do processo do pedido
    [{pid, _}] = Registry.lookup(:order_system_orders, "pedido-1")
    Process.exit(pid, :kill)
    Process.sleep(200)

    IO.inspect(OrderSystem.get_order("pedido-1"), label: "após recuperação")

    # Aguarda expiração automática do pedido-2
    Process.sleep(1_500)
    IO.inspect(OrderSystem.get_order("pedido-2"), label: "pedido-2 expirado")

    flush()

    # Simula reinicialização do sistema: o estado deve ser recuperado do DETS
    Supervisor.stop(OrderSystem.Supervisor)
    {:ok, _} = OrderSystem.start_link(path: path)
    IO.inspect(OrderSystem.get_order("pedido-1"), label: "após reinicialização")

    Supervisor.stop(OrderSystem.Supervisor)
    :ok
  end

  defp flush do
    receive do
      msg ->
        IO.inspect(msg, label: "evento")
        flush()
    after
      0 -> :ok
    end
  end
end
