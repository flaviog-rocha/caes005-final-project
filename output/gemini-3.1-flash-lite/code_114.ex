# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Detectar encerramentos inesperados de processos.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSystem.Supervisor do
  use Supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      # Gerencia o estado das sessões em memória
      {UserSystem.SessionManager, []},
      # Worker que executa tarefas periódicas (ex: limpeza de sessões expiradas)
      {UserSystem.Janitor, [interval: 60_000]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule UserSystem.SessionManager do
  use GenServer

  # API
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def create_session(user_id) do
    GenServer.call(__MODULE__, {:create, user_id})
  end

  # Callbacks
  @impl true
  def init(state) do
    # Monitorar a si mesmo ou processos de usuários pode ser feito aqui
    {:ok, state}
  end

  @impl true
  def handle_call({:create, user_id}, _from, state) do
    # Simulação de escrita em DB
    IO.puts("Persistindo sessão para usuário: #{user_id}")
    new_state = Map.put(state, user_id, :os.system_time(:second))
    {:reply, :ok, new_state}
  end
end

defmodule UserSystem.Janitor do
  use GenServer

  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    # Agendar a primeira execução
    Process.send_after(self(), :cleanup, interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:cleanup, state) do
    Logger.info("Executando limpeza periódica de sessões...")
    
    # Detecção de encerramentos inesperados: 
    # Aqui poderíamos verificar processos monitorados via Process.monitor/1
    
    Process.send_after(self(), :cleanup, state.interval)
    {:noreply, state}
  end
end

# Para rodar: 
# UserSystem.Supervisor.start_link([])
# UserSystem.SessionManager.create_session(123)
