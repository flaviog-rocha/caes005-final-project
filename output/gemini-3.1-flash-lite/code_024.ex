# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar expiração automática de registros.
# 3. Detectar encerramentos inesperados de processos.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Library.LoanManager do
  @moduledoc """
  Gerenciador de empréstimos utilizando processos independentes via DynamicSupervisor.
  Cada processo representa um empréstimo individual com expiração automática.
  """

  use DynamicSupervisor
  require Logger

  # --- API Pública ---

  def start_link(init_arg), do: DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)

  @doc "Inicia o rastreamento de um novo empréstimo"
  def start_loan(loan_id, duration_ms) do
    spec = {Library.LoanWorker, %{id: loan_id, duration: duration_ms}}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  # --- Callback do Supervisor ---

  @impl true
  def init(_init_arg), do: DynamicSupervisor.init(strategy: :one_for_one)
end

defmodule Library.LoanWorker do
  @moduledoc """
  Worker que gerencia um único empréstimo.
  Detecta encerramentos e gerencia o timeout.
  """
  use GenServer
  require Logger

  # --- API do Worker ---

  def start_link(state), do: GenServer.start_link(__MODULE__, state)

  @impl true
  def init(%{id: id, duration: duration}) do
    Logger.info("Empréstimo iniciado: #{id}. Expira em #{duration}ms.")
    # Agendar auto-expiração
    Process.send_after(self(), :expire, duration)
    
    # Simulação de monitoramento de banco de dados fictício
    # Em um cenário real, poderíamos salvar o estado aqui
    {:ok, %{id: id, status: :active}}
  end

  @impl true
  def handle_info(:expire, state) do
    Logger.info("Processo do empréstimo #{state.id} expirado. Limpando recursos...")
    # Aqui ocorreria a chamada para o banco de dados (ex: Repo.update)
    {:stop, :normal, state}
  end

  # --- Detecção de encerramento inesperado ---
  # Se o processo for encerrado bruscamente, o supervisor 
  # pode ser configurado para tratar logs ou reverter estados no DB.
  @impl true
  def terminate(reason, state) do
    if reason != :normal do
      Logger.error("Empréstimo #{state.id} encerrado inesperadamente: #{inspect(reason)}")
      # Lógica para marcar no banco fictício como 'inconsistente' ou 'pendente de revisão'
    end
    :ok
  end
end

# --- Exemplo de Uso ---
# Library.LoanManager.start_link([])
# Library.LoanManager.start_loan("livro-001", 5000)
