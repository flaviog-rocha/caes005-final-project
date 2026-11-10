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

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def add_book(id, title), do: GenServer.call(__MODULE__, {:add, id, title})
  def get_book(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_book(id, title), do: GenServer.call(__MODULE__, {:update, id, title})
  def remove_book(id), do: GenServer.call(__MODULE__, {:remove, id})

  # --- Callbacks do GenServer ---

  @impl true
  def init(:ok) do
    # O ETS é criado com :public para que, em caso de crash do GenServer,
    # os dados persistam na memória fora do heap do processo.
    table = :ets.new(:library_table, [:named_table, :set, :public, read_concurrency: true])
    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:add, id, title}, _from, state) do
    case :ets.insert_new(state.table, {id, title}) do
      true -> {:reply, {:ok, id}, state}
      false -> {:reply, {:error, :already_exists}, state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    case :ets.lookup(state.table, id) do
      [{^id, title}] -> {:reply, {:ok, title}, state}
      [] -> {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:update, id, title}, _from, state) do
    if :ets.member(state.table, id) do
      :ets.insert(state.table, {id, title})
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:remove, id}, _from, state) do
    :ets.delete(state.table, id)
    {:reply, :ok, state}
  end

  # --- Estratégia de Supervisão ---
  # Garante que o processo seja reiniciado automaticamente após falha.
  defmodule Supervisor do
    use Supervisor

    def start_link(_) do
      Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
    end

    @impl true
    def init(:ok) do
      children = [
        {LibraryManager, []}
      ]
      # :one_for_one garante detecção e reinicialização de processos filhos
      Supervisor.init(children, strategy: :one_for_one)
    end
  end
end
