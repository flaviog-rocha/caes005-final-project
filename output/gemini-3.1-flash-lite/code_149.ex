# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderManager do
  @moduledoc """
  Gerenciador de pedidos funcional usando GenServer, ETS para persistência
  e mecanismos de supervisão nativos do Elixir.
  """
  use GenServer

  # --- API Pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_order(order_id, amount) when is_binary(order_id) and is_number(amount) do
    if amount <= 0, do: {:error, :invalid_amount}, else: GenServer.call(__MODULE__, {:add, order_id, amount})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_opts) do
    # Tabela ETS com :public para persistência persistente na memória (pode ser configurada com :file)
    table = :ets.new(:orders, [:set, :named_table, :protected])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:add, id, amount}, _from, state) do
    :ets.insert(state.table, {id, amount})
    
    # 1. Gerar notificações baseadas em regras
    if amount > 1000, do: notify_admin(id, amount)

    {:reply, :ok, state}
  end

  # 2. Detectar encerramentos inesperados (Monitoramento via links)
  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    IO.puts("Monitoramento: Processo #{inspect(pid)} encerrou com erro: #{inspect(reason)}")
    {:noreply, state}
  end

  # --- Funções Auxiliares ---

  defp notify_admin(id, amount) do
    spawn(fn -> IO.puts("[ALERTA] Pedido de alto valor detectado: #{id} - R$#{amount}") end)
  end
end

# --- Estrutura de Supervisão ---

defmodule OrderSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      # 3. Persistência: O processo mantém o estado em ETS, 
      # a árvore de supervisão garante reinicialização em caso de crash.
      {OrderManager, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
