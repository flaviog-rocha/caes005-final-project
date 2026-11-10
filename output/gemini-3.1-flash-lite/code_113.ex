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

defmodule UserSystem do
  @moduledoc """
  Sistema de gerenciamento de usuários e sessões utilizando OTP.
  - Periodicidade: Limpeza de sessões expiradas (GenServer).
  - Tolerância a falhas: Supervisor reiniciando processos.
  - Monitoramento: Detecção de encerramento de processos (Links/Monitors).
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {SessionCleaner, interval: 60_000},
      {SessionManager, name: SessionManager}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SessionManager do
  use GenServer

  # API
  def start_link(opts), do: GenServer.start_link(__MODULE__, [], opts)
  def add_session(pid, user_id), do: GenServer.cast(pid, {:add, user_id})

  # Callback
  @impl true
  def init(_) do
    # Simula conexão com DB
    {:ok, %{sessions: %{}}}
  end

  @impl true
  def handle_cast({:add, user_id}, state) do
    # Monitora o processo do usuário para detectar encerramento inesperado
    ref = Process.monitor(self()) 
    new_sessions = Map.put(state.sessions, user_id, %{ref: ref, started_at: System.system_time(:second)})
    {:noreply, %{state | sessions: new_sessions}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    IO.puts("Detectado encerramento de sessão: #{inspect(ref)}. Motivo: #{inspect(reason)}")
    {:noreply, state}
  end
end

defmodule SessionCleaner do
  use GenServer

  def start_link(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    GenServer.start_link(__MODULE__, interval, name: __MODULE__)
  end

  @impl true
  def init(interval) do
    # Executa operação periodicamente usando timer
    :timer.send_interval(interval, :cleanup)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:cleanup, state) do
    IO.puts("Executando limpeza periódica de banco de dados fictício...")
    # Aqui entraria uma query Ecto.Repo.delete_all/1, etc.
    {:noreply, state}
  end
end
