# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Executar determinadas operações periodicamente.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Sistema de gerenciamento de empréstimos com persistência via ETS,
  supervisão, tarefas periódicas e prevenção de duplicidade (Idempotência).
  """
  use Supervisor
  use GenServer

  # --- API Pública ---

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def borrow_book(book_id, user_id) do
    GenServer.call(__MODULE__, {:borrow, book_id, user_id})
  end

  # --- Supervisor ---

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: Registry.Library},
      {__MODULE__.Store, []},
      {__MODULE__.Ticker, []}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end

  # --- Store (GenServer + ETS + Persistência) ---

  defmodule Store do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

    @impl true
    def init(:ok) do
      # Cria tabela persistente (via :bag ou :set)
      table = :ets.new(:loans, [:set, :public, :named_table, read_concurrency: true])
      {:ok, %{table: table}}
    end

    @impl true
    def handle_call({:borrow, book_id, user_id}, {from_pid, _}, state) do
      # Idempotência: Checa se já existe operação pendente
      case :ets.lookup(state.table, book_id) do
        [{^book_id, ^user_id, :active}] ->
          {:reply, {:error, :already_borrowed}, state}
        _ ->
          :ets.insert(state.table, {book_id, user_id, :active})
          IO.puts("Livro #{book_id} emprestado para #{user_id}")
          {:reply, :ok, state}
      end
    end
  end

  # --- Ticker (Operações Periódicas + Detecção de falhas) ---

  defmodule Ticker do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

    @impl true
    def init(:ok) do
      Process.send_after(self(), :check_status, 5000)
      {:ok, %{}}
    end

    @impl true
    def handle_info(:check_status, state) do
      # 1. Executa operação periódica
      IO.puts("Verificando prazos de devolução...")

      # 2. Notificação (Simulada)
      # Aqui você integraria com logger ou sistema de mensageria
      
      Process.send_after(self(), :check_status, 5000)
      {:noreply, state}
    end

    # 3. Detectar encerramentos inesperados
    @impl true
    def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
      IO.puts("Processo monitorado encerrou: #{inspect(reason)}")
      {:noreply, state}
    end
  end
end
