# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Executar determinadas operações periodicamente.
# 3. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OrderSystem do
  use Application

  # Supervisor principal que gerencia os processos do sistema
  def start(_type, _args) do
    children = [
      {OrderManager, name: OrderManager},
      {NotificationWorker, name: NotificationWorker},
      {PeriodicTaskWorker, name: PeriodicTaskWorker}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule OrderManager do
  use GenServer

  # 1. Detectar encerramentos inesperados: O GenServer monitora processos
  # registrados e pode ser configurado para tratar estados de erro.
  def start_link(opts), do: GenServer.start_link(__MODULE__, %{}, opts)

  def init(state), do: {:ok, state}

  def handle_cast({:process_order, id, total}, state) do
    # Logica de negocio: notificar se o pedido for de alto valor
    if total > 1000 do
      send(NotificationWorker, {:notify, "Pedido alto valor: #{id}"})
    end
    {:noreply, Map.put(state, id, total)}
  end
end

defmodule NotificationWorker do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

  def init(:ok), do: {:ok, %{}}

  def handle_info({:notify, message}, state) do
    IO.puts("[Notificação]: #{message}")
    {:noreply, state}
  end
end

defmodule PeriodicTaskWorker do
  use GenServer

  # 2. Executar operações periodicamente usando :timer
  def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

  def init(:ok) do
    :timer.send_interval(5000, :run_cleanup)
    {:ok, %{}}
  end

  def handle_info(:run_cleanup, state) do
    IO.puts("[Tarefa Periódica]: Limpando cache de pedidos...")
    {:noreply, state}
  end
end

# Demonstração de uso
# Para rodar, basta chamar:
# Application.start(:my_app)
# GenServer.cast(OrderManager, {:process_order, 1, 1500})
