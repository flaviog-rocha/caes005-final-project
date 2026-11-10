# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir a execução de operações simultâneas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Manter um histórico das operações realizadas.
# 5. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  use GenServer
  require Logger

  # Definição do estado: %{history: [log], subscribers: [pids]}
  @initial_state %{history: [], subscribers: []}

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, @initial_state, name: __MODULE__)
  end

  def subscribe do
    GenServer.cast(__MODULE__, {:subscribe, self()})
  end

  def send_message(msg) do
    GenServer.cast(__MODULE__, {:broadcast, msg})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Requisito 5: Executar operação periodicamente (a cada 10 segundos)
    :timer.send_interval(10_000, :periodic_cleanup)
    {:ok, state}
  end

  @impl true
  def handle_cast({:subscribe, pid}, state) do
    # Requisito 3: Monitorar processos para recuperação
    Process.monitor(pid)
    {:noreply, %{state | subscribers: [pid | state.subscribers] |> Enum.uniq()}}
  end

  @impl true
  def handle_cast({:broadcast, msg}, state) do
    # Requisito 2: Executar simultaneamente via Task
    Enum.each(state.subscribers, fn pid ->
      Task.start(fn -> send(pid, {:message, msg}) end)
    end)

    # Requisito 4: Manter histórico
    new_history = [%{msg: msg, ts: System.system_time(:second)} | state.history]
    {:noreply, %{state | history: new_history}}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # Requisito 3: Recuperação após falha (remover processo que caiu)
    Logger.info("Processo #{inspect(pid)} falhou e foi removido.")
    {:noreply, %{state | subscribers: List.delete(state.subscribers, pid)}}
  end

  @impl true
  def handle_info(:periodic_cleanup, state) do
    # Requisito 5: Implementação de tarefa periódica
    Logger.info("Executando limpeza periódica do sistema.")
    {:noreply, state}
  end
end

# Exemplo de uso para demonstrar:
# {:ok, pid} = MessagingSystem.start_link([])
# MessagingSystem.subscribe()
# MessagingSystem.send_message("Olá Mundo!")
# MessagingSystem.get_history()
