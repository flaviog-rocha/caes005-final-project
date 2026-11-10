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

defmodule LibraryManager.Store do
  @moduledoc """
  Gerenciador de estado persistente usando Agent e ETS para recuperação de falhas.
  """
  use Agent

  @table_name :books_table

  def start_link(_opts) do
    # Garante que a tabela exista antes do Agent iniciar
    :ets.new(@table_name, [:set, :public, :named_table, {:read_concurrency, true}])
    
    # Carrega dados do disco se existirem
    if File.exists?("books.dat"), do: load_from_disk()

    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def add(book) do
    :ets.insert(@table_name, {book.id, book})
    persist()
  end

  def get(id), do: :ets.lookup(@table_name, id) |> hd() |> elem(1)

  def update(id, new_data) do
    :ets.insert(@table_name, {id, new_data})
    persist()
  end

  def delete(id) do
    :ets.delete(@table_name, id)
    persist()
  end

  defp persist do
    data = :ets.tab2list(@table_name)
    File.write!("books.dat", :erlang.term_to_binary(data))
  end

  defp load_from_disk do
    case File.read("books.dat") do
      {:ok, binary} ->
        data = :erlang.binary_to_term(binary)
        :ets.insert(@table_name, data)
      _ -> :ok
    end
  end
end

defmodule LibraryManager do
  @moduledoc """
  Supervisor principal e orquestrador via Task.Supervisor
  """
  use Supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    children = [
      {Task.Supervisor, name: LibraryTaskSupervisor},
      LibraryManager.Store
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Execução assíncrona usando Task.Supervisor conforme solicitado
  def execute_async(operation) do
    Task.Supervisor.start_child(LibraryTaskSupervisor, fn ->
      operation.()
    end)
  end
end

# Exemplo de uso:
# {:ok, _} = LibraryManager.start_link([])
#
# LibraryManager.execute_async(fn -> 
#   LibraryManager.Store.add(%{id: 1, title: "O Hobbit", author: "Tolkien"})
# end)
#
# IO.inspect(LibraryManager.Store.get(1))
