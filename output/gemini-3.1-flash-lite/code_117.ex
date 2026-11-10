# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos de livros com persistência, validação,
  operações CRUD e tarefas periódicas usando Task.
  """
  use GenServer

  @file_path "library_data.dets"

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def add_book(id, title), do: GenServer.call(__MODULE__, {:add, id, title})
  def get_book(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_book(id, title), do: GenServer.call(__MODULE__, {:update, id, title})
  def remove_book(id), do: GenServer.call(__MODULE__, {:remove, id})

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    {:ok, table} = :dets.open_file(:library_db, file: @file_path)
    # Inicia a tarefa periódica de limpeza/manutenção
    spawn_maintenance_task()
    {:ok, table}
  end

  @impl true
  def handle_call({:add, id, title}, _from, table) when is_integer(id) and is_binary(title) do
    case :dets.insert_new(table, {id, title}) do
      true -> {:reply, :ok, table}
      false -> {:reply, {:error, :already_exists}, table}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, table) do
    {:reply, :dets.lookup(table, id), table}
  end

  @impl true
  def handle_call({:update, id, title}, _from, table) when is_binary(title) do
    :dets.insert(table, {id, title})
    {:reply, :ok, table}
  end

  @impl true
  def handle_call({:remove, id}, _from, table) do
    :dets.delete(table, id)
    {:reply, :ok, table}
  end

  @impl true
  def handle_call(_, _from, table), do: {:reply, {:error, :invalid_data}, table}

  # --- Lógica de Tarefas Periódicas ---

  defp spawn_maintenance_task do
    Task.start_link(fn ->
      loop_maintenance()
    end)
  end

  defp loop_maintenance do
    # Simula uma operação de manutenção (ex: sincronização de disco)
    :timer.sleep(60_000)
    :dets.sync(:library_db)
    IO.puts("Manutenção periódica executada: Dados sincronizados com disco.")
    loop_maintenance()
  end
end

# Exemplo de uso:
# {:ok, _pid} = LibraryManager.start_link(nil)
# LibraryManager.add_book(1, "O Senhor dos Anéis")
# LibraryManager.get_book(1)
