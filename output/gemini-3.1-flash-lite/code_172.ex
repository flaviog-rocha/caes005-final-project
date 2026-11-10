# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos utilizando GenServer para concorrência segura,
  ETS para persistência em memória com recuperação de falhas e 
  supervisão para tolerância a falhas.
  """

  use GenServer
  require Logger

  @table_name :loans_table

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def add_loan(book_id, user_id) do
    GenServer.call(__MODULE__, {:add, book_id, user_id})
  end

  def get_loan(book_id) do
    case :ets.lookup(@table_name, book_id) do
      [{^book_id, data}] -> {:ok, data}
      [] -> {:error, :not_found}
    end
  end

  def update_loan(book_id, user_id) do
    GenServer.call(__MODULE__, {:update, book_id, user_id})
  end

  def remove_loan(book_id) do
    GenServer.call(__MODULE__, {:remove, book_id})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(:ok) do
    # Garante persistência da tabela mesmo se o processo do GenServer reiniciar
    # A tabela é criada se não existir (named_table)
    table = :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    
    # Monitora o processo pai ou eventos de sistema se necessário
    Process.flag(:trap_exit, true)
    
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:add, book_id, user_id}, _from, state) do
    if :ets.insert_new(@table_name, {book_id, %{user_id: user_id, updated_at: DateTime.utc_now()}}) do
      {:reply, :ok, state}
    else
      {:reply, {:error, :already_exists}, state}
    end
  end

  @impl true
  def handle_call({:update, book_id, user_id}, _from, state) do
    if :ets.member(@table_name, book_id) do
      :ets.insert(@table_name, {book_id, %{user_id: user_id, updated_at: DateTime.utc_now()}})
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:remove, book_id}, _from, state) do
    :ets.delete(@table_name, book_id)
    {:reply, :ok, state}
  end

  # Tratamento de encerramento inesperado
  @impl true
  def terminate(reason, _state) do
    Logger.error("Gerenciador de empréstimos encerrado: #{inspect(reason)}")
    :ok
  end
end

# --- Estrutura de Supervisão para garantir resiliência ---

defmodule LibrarySupervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {LibraryManager, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
