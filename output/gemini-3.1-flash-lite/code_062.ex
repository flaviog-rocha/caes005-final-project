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

  @doc """
  O módulo utiliza um GenServer para gerenciar o estado (histórico).
  A supervisão é feita pelo Supervisor do OTP para garantir tolerância a falhas.
  """

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{history: []}, name: __MODULE__)
  end

  def send_message(from, to, content) do
    GenServer.cast(__MODULE__, {:log_message, from, to, content})
  end

  def get_history do
    GenServer.call(__MODULE__, :get_history)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Requisito 5: Iniciar operação periódica (ex: logar status a cada 60s)
    :timer.send_interval(60_000, :periodic_task)
    {:ok, state}
  end

  @impl true
  def handle_cast({:log_message, from, to, content}, state) do
    new_entry = %{timestamp: System.system_time(:second), from: from, to: to, content: content}
    # Requisito 4: Manter histórico
    {:noreply, %{state | history: [new_entry | state.history]}}
  end

  @impl true
  def handle_call(:get_history, _from, state) do
    {:reply, state.history, state}
  end

  @impl true
  def handle_info(:periodic_task, state) do
    IO.puts("[#{inspect(self())}] Executando manutenção periódica...")
    {:noreply, state}
  end
end

# --- Implementação do Supervisor para Tolerância a Falhas ---
# Requisito 3: Recuperar processos após falhas (Supervision Tree)

defmodule MessagingSystem.Supervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {MessagingSystem, []}
    ]

    # Requisito 2: Estratégia de supervisão permite concorrência e reinício
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# --- Demonstração de Uso ---

# Iniciar o sistema
MessagingSystem.Supervisor.start_link([])

# Requisito 1: Comunicação entre processos
# Requisito 2: Operações simultâneas
Task.start(fn -> MessagingSystem.send_message("UserA", "UserB", "Olá!") end)
Task.start(fn -> MessagingSystem.send_message("UserC", "UserD", "Como vai?") end)

# Aguarda processamento
Process.sleep(100)

# Recuperar histórico
IO.inspect(MessagingSystem.get_history())
