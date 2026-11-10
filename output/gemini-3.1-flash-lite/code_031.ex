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

  @doc """
  O Supervisor gerencia três processos distintos:
  1. O Notificador: Monitora eventos de pedidos via GenServer.
  2. O Worker de Limpeza: Executa tarefas periódicas.
  3. O Worker de Pedidos: Processo que pode falhar para demonstrar detecção de erro.
  """
  def start(_type, _args) do
    children = [
      {OrderNotifier, name: OrderNotifier},
      {CleanupWorker, interval: 60_000},
      {OrderProcessor, name: OrderProcessor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

defmodule OrderNotifier do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, :ok, opts)

  def notify(message) do
    GenServer.cast(__MODULE__, {:notify, message})
  end

  def init(:ok), do: {:ok, []}

  def handle_cast({:notify, msg}, state) do
    IO.puts("[NOTIFICAÇÃO] #{msg}")
    {:noreply, state}
  end
end

defmodule CleanupWorker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    interval = Keyword.fetch!(opts, :interval)
    schedule_cleanup(interval)
    {:ok, %{interval: interval}}
  end

  def handle_info(:work, state) do
    IO.puts("[LIMPEZA] Executando rotina periódica de manutenção...")
    schedule_cleanup(state.interval)
    {:noreply, state}
  end

  defp schedule_cleanup(interval), do: Process.send_after(self(), :work, interval)
end

defmodule OrderProcessor do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def init(:ok) do
    # Monitora a si mesmo ou poderia monitorar outros processos para detectar falhas
    Process.flag(:trap_exit, true)
    IO.puts("[PROCESSADOR] Iniciado.")
    {:ok, %{}}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    OrderNotifier.notify("Detectado encerramento inesperado: #{inspect(reason)}")
    {:noreply, state}
  end

  # Simulação de falha catastrófica após 5 segundos
  def handle_info(:simulate_crash, _state) do
    raise "Falha crítica no processamento!"
  end
end
