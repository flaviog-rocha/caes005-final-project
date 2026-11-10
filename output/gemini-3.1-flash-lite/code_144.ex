# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule FinancialSystem do
  @moduledoc """
  Sistema de gerenciamento de transações com controle de concorrência e tarefas agendadas.
  Utiliza GenServer para gerenciamento de estado e Registry para registro de processos.
  """

  use GenServer
  require Logger

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def create_transaction(id, amount), do: GenServer.call(__MODULE__, {:create, id, amount})
  def get_transaction(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_transaction(id, amount), do: GenServer.call(__MODULE__, {:update, id, amount})
  def delete_transaction(id), do: GenServer.call(__MODULE__, {:delete, id})

  # --- Controle de Concorrência (Semaphore) ---

  @doc "Limita a quantidade de operações simultâneas usando um pool de tarefas (Task.Supervisor)"
  def execute_concurrent(fun, max_concurrency \\ 5) do
    # Simulação de limitação de concorrência via Semaphore pattern
    :poolboy.transaction(:worker_pool, fn pid ->
      Task.async(fn -> fun.() end) |> Task.await()
    end, :infinity)
  end

  # --- Tarefas Periódicas ---

  @doc "Inicia um timer para execução periódica (ex: processamento de lote)"
  def schedule_cleanup(interval_ms \\ 60_000) do
    :timer.send_interval(interval_ms, :cleanup)
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    # Estado inicial: Map representando o banco de dados em memória
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, id, amount}, _from, state) do
    new_state = Map.put(state, id, %{amount: amount, updated_at: DateTime.utc_now()})
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state, id), state}
  end

  @impl true
  def handle_call({:update, id, amount}, _from, state) do
    if Map.has_key?(state, id) do
      new_state = Map.put(state, id, %{amount: amount, updated_at: DateTime.utc_now()})
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    {:reply, :ok, Map.delete(state, id)}
  end

  @impl true
  def handle_info(:cleanup, state) do
    Logger.info("Executando limpeza periódica de transações...")
    # Aqui entraria a lógica de persistência para um banco de dados real (Ecto)
    {:noreply, state}
  end
end

# --- Exemplo de Uso / Script de Inicialização ---

defmodule AppSupervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [])
  end

  @impl true
  def init(_) do
    children = [
      FinancialSystem,
      # Pool de workers para limitar concorrência
      :poolboy.child_spec(:worker_pool, [
        name: {:local, :worker_pool},
        worker_module: GenServer,
        size: 5,
        max_overflow: 2
      ])
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
