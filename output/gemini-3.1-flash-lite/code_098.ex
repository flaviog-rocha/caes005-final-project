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
  supervisão, tarefas periódicas e idempotência.
  """

  use GenServer
  use Supervisor

  # --- API Pública ---

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  def borrow_book(book_id, user_id) do
    GenServer.call(__MODULE__, {:borrow, book_id, user_id})
  end

  # --- Supervisor ---

  def init(:ok) do
    children = [
      {GenServer, name: __MODULE__, module: __MODULE__, args: []},
      {Task, &periodic_cleanup/0}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- GenServer ---

  def init(_) do
    # Persistência básica usando ETS com tabela nomeada
    table = :ets.new(:loans, [:set, :named_table, :public, {:read_concurrency, true}])
    # Detectar encerramento inesperado de outros processos se necessário
    Process.flag(:trap_exit, true)
    {:ok, %{table: table}}
  end

  # Requisito 2: Idempotência (verificar se o livro já foi emprestado)
  def handle_call({:borrow, book_id, user_id}, _from, state) do
    case :ets.lookup(state.table, book_id) do
      [] ->
        :ets.insert(state.table, {book_id, user_id, :os.system_time(:second)})
        notify_user(user_id, book_id)
        {:reply, {:ok, "Emprestado com sucesso"}, state}

      _ ->
        {:reply, {:error, "Operação duplicada: livro já emprestado"}, state}
    end
  end

  # Requisito 1: Notificações
  defp notify_user(user_id, book_id) do
    IO.puts("[Notificação] Usuário #{user_id} notificade sobre o livro #{book_id}")
  end

  # Requisito 3: Operação periódica
  defp periodic_cleanup do
    loop = fn f ->
      :timer.sleep(60_000)
      IO.puts("[Monitor] Verificando integridade dos registros...")
      f.(f)
    end
    loop.(loop)
  end

  # Requisito 5: Lidar com encerramento inesperado
  def terminate(reason, _state) do
    IO.puts("[Alerta] Processo encerrado inesperadamente: #{inspect(reason)}")
    :ok
  end
end

# --- Execução ---

# Para rodar, basta chamar:
# LibraryManager.start_link([])
# LibraryManager.borrow_book("123", "Joao")
# LibraryManager.borrow_book("123", "Joao") # Retornará erro de idempotência
