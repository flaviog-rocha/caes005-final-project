# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task.Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos usando Agent para estado persistente e 
  Task.Supervisor para processamento assíncrono.
  """
  use Agent
  use Supervisor

  @db_file "library_data.dets"

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Inicializa o armazenamento persistente (DETS)
    {:ok, _} = :dets.open_file(:library_db, file: ~c"#{@db_file}", type: :set)
    
    children = [
      {Agent, fn -> :dets.tab2list(:library_db) |> Map.new() end, name: :library_state},
      {Task.Supervisor, name: LibraryTaskSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # CRUD com persistência síncrona no DETS e atualização do Agent
  def create(id, book_name) do
    Task.Supervisor.start_child(LibraryTaskSupervisor, fn ->
      :dets.insert(:library_db, {id, book_name})
      Agent.update(:library_state, &Map.put(&1, id, book_name))
    end)
  end

  def get(id) do
    Agent.get(:library_state, &Map.get(&1, id))
  end

  def update(id, book_name) do
    Task.Supervisor.start_child(LibraryTaskSupervisor, fn ->
      :dets.insert(:library_db, {id, book_name})
      Agent.update(:library_state, &Map.put(&1, id, book_name))
    end)
  end

  def delete(id) do
    Task.Supervisor.start_child(LibraryTaskSupervisor, fn ->
      :dets.delete(:library_db, id)
      Agent.update(:library_state, &Map.delete(&1, id))
    end)
  end

  def list_all do
    Agent.get(:library_state, & &1)
  end
end

# Exemplo de uso:
# LibraryManager.start_link(nil)
# LibraryManager.create(1, "O Nome do Vento")
# IO.inspect(LibraryManager.get(1))
# LibraryManager.update(1, "O Temor do Sábio")
# LibraryManager.delete(1)
