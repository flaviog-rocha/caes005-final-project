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
  Cada processo representa um empréstimo individual.
  """

  use DynamicSupervisor
  require Logger

  @doc "Inicia o supervisor de empréstimos"
  def start_link(_), do: DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc "Inicia o rastreamento de um novo empréstimo"
  def start_loan(loan_id, duration_ms) do
    spec = {Library.LoanWorker, {loan_id, duration_ms}}
    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end

defmodule Library.LoanWorker do
  @moduledoc """
  Worker que gerencia o ciclo de vida de um empréstimo.
  Implementa expiração automática e monitoramento de falhas.
  """

  use GenServer
  require Logger

  # Simulação de banco de dados fictício
  @db_mock :ets.new(:loans_db, [:set, :public, :named_table])

  def start_link({loan_id, duration_ms}) do
    GenServer.start_link(__MODULE__, {loan_id, duration_ms}, name: via_tuple(loan_id))
  end

  defp via_tuple(id), do: {:via, Registry, {Library.LoanRegistry, id}}

  @impl true
  def init({loan_id, duration_ms}) do
    Logger.info("Empréstimo #{loan_id} iniciado. Expira em #{duration_ms}ms.")
    # Persistência fictícia
    :ets.insert(@db_mock, {loan_id, :active})
    
    # Agendar expiração automática
    Process.send_after(self(), :expire, duration_ms)
    
    {:ok, %{id: loan_id}}
  end

  @impl true
  def handle_info(:expire, state) do
    Logger.info("Empréstimo #{state.id} expirou.")
    :ets.delete(@db_mock, state.id)
    {:stop, :normal, state}
  end

  @impl true
  def terminate(reason, state) do
    if reason != :normal do
      Logger.error("Empréstimo #{state.id} encerrou inesperadamente: #{inspect(reason)}")
      # Aqui poderia ser chamada uma lógica de rollback ou logging no Ecto
    end
    :ok
  end
end

# Setup de inicialização para o script autocontido
defmodule Library.App do
  def run do
    # Registrar processos para permitir busca global pelo DynamicSupervisor
    Registry.start_link(keys: :unique, name: Library.LoanRegistry)
    Library.LoanManager.start_link([])

    # Exemplo de uso:
    Library.LoanManager.start_loan("livro_123", 2000)
    Library.LoanManager.start_loan("livro_456", 5000)
  end
end

# Inicia a execução
Library.App.run()
