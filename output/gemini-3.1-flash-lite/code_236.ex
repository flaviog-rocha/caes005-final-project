# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciamento de livros com processamento em stream e resiliência.
  """

  # Utiliza um Agent como estado simples para simular um banco de dados em memória
  def start_link, do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  # 1. CRUD
  def create(id, title), do: Agent.update(__MODULE__, &Map.put(&1, id, title))
  def get(id), do: Agent.get(__MODULE__, &Map.get(&1, id))
  def update(id, title), do: Agent.update(__MODULE__, &Map.put(&1, id, title))
  def delete(id), do: Agent.update(__MODULE__, &Map.delete(&1, id))

  # 2. Novas tentativas após falhas (Retry pattern)
  def with_retry(func, retries \\ 3) do
    try do
      func.()
    rescue
      _ ->
        if retries > 0,
          do: (Process.sleep(100); with_retry(func, retries - 1)),
          else: {:error, :max_retries_reached}
    end
  end

  # 3. Processamento de grandes volumes sem carregar tudo na memória
  # Utiliza Stream para lazyness
  def process_large_data(data_stream) do
    data_stream
    |> Stream.chunk_every(100) # Processa em batches
    |> Stream.each(fn batch ->
      Enum.each(batch, fn {id, title} ->
        with_retry(fn -> create(id, title) end)
      end)
    end)
    |> Stream.run()
  end

  # 4. Operações simultâneas
  # Utiliza Task para processamento paralelo
  def process_concurrently(items) do
    items
    |> Enum.map(fn item ->
      Task.async(fn ->
        with_retry(fn -> create(elem(item, 0), elem(item, 1)) end)
      end)
    end)
    |> Enum.map(&Task.await/1)
  end
end

# Exemplo de uso:
# {:ok, _pid} = LibraryManager.start_link()
#
# # Stream infinito de dados (simulando grande volume)
# data = Stream.map(1..1000, fn i -> {i, "Livro #{i}"} end)
# LibraryManager.process_large_data(data)
#
# # Execução paralela
# LibraryManager.process_concurrently([{9999, "Concorrente 1"}, {8888, "Concorrente 2"}])
