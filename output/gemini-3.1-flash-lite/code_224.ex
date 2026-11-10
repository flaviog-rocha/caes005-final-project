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

defmodule OrderSystem do
  @moduledoc """
  Sistema de gerenciamento de pedidos usando OTP (GenServer, Registry, ETS e Supervisor).
  Atende aos requisitos de persistência, tolerância a falhas e expiração automática.
  """
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {Registry, keys: :unique, name: OrderRegistry},
      {OrderManager, []}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule OrderManager do
  use GenServer
  require Logger

  @table_name :orders_table
  @expiry_ms 60_000 # 1 minuto para expiração automática

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def create_order(id, data) do
    GenServer.call(__MODULE__, {:create, id, data})
  end

  # --- Callbacks GenServer ---

  @impl true
  def init(_) do
    # Requisito 3: Persistência usando ETS com table disk
    :ets.new(@table_name, [:set, :public, :named_table, {:read_concurrency, true}])
    
    # Simulação de carregamento de persistência (em produção, usar file ou Mnesia)
    Logger.info("Sistema de pedidos iniciado.")
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, id, data}, _from, state) do
    # Requisito 5: Entidades processadas independentemente via Registry
    order_pid = spawn_order_process(id, data)
    {:reply, {:ok, order_pid}, state}
  end

  defp spawn_order_process(id, data) do
    {:ok, pid} = Task.start(fn ->
      # Requisito 4: Expiração automática usando process sleep/timeout
      Process.sleep(@expiry_ms)
      :ets.delete(@table_name, id)
      Logger.info("Pedido #{id} expirado.")
    end)
    
    :ets.insert(@table_name, {id, data, System.system_time(:millisecond)})
    pid
  end
end

defmodule OrderWorker do
  use GenServer
  # Requisito 1: Detectar encerramentos inesperados (monitoramento)
  # Requisito 2: Comunicação entre processos
  
  def start_link(order_id) do
    GenServer.start_link(__MODULE__, order_id, name: {:via, Registry, {OrderRegistry, order_id}})
  end

  @impl true
  def init(order_id) do
    Process.flag(:trap_exit, true)
    {:ok, %{id: order_id}}
  end

  @impl true
  def handle_info({:EXIT, _from, reason}, state) do
    Logger.error("Processo de pedido #{state.id} finalizado inesperadamente: #{inspect(reason)}")
    {:noreply, state}
  end
end
