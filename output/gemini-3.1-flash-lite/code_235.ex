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
  Gerenciador de empréstimos usando Agent para estado, 
  Streams para processamento de grandes volumes e Task.Supervisor para concorrência.
  """

  use Agent
  require Logger

  @doc "Inicia o gerenciador com um estado vazio."
  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc "CRUD: Cadastro e Atualização"
  def save_book(id, title, author), do: Agent.update(__MODULE__, &Map.put(&1, id, %{title: title, author: author}))

  @doc "CRUD: Consulta"
  def get_book(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  @doc "CRUD: Remoção"
  def delete_book(id), do: Agent.update(__MODULE__, &Map.delete(&1, id))

  @doc """
  Processa grandes volumes de dados de forma preguiçosa (Lazy).
  Recebe um Enumerable (ex: arquivo ou stream de banco) e aplica uma função de processamento.
  """
  def process_large_dataset(stream, process_fn) do
    stream
    |> Stream.map(fn item -> 
       # Implementação de Retry com limite de 3 tentativas
       retry_operation(fn -> process_fn.(item) end, 3) 
    end)
    |> Stream.run()
  end

  @doc "Concorrência usando Task.Supervisor"
  def process_concurrently(items, process_fn) do
    items
    |> Task.async_stream(fn item -> process_fn.(item) end, max_concurrency: System.schedulers_online())
    |> Stream.run()
  end

  defp retry_operation(fun, retries) do
    try do
      fun.()
    rescue
      _ ->
        if retries > 0 do
          Process.sleep(100) # Backoff simples
          retry_operation(fun, retries - 1)
        else
          Logger.error("Falha após múltiplas tentativas")
          {:error, :failed}
        end
    end
  end
end

# Exemplo de uso:
# {:ok, _} = LibraryManager.start_link([])
# LibraryManager.save_book(1, "O Nome do Vento", "Patrick Rothfuss")
#
# # Exemplo de processamento de grande volume (Lazy Stream)
# large_stream = Stream.map(1..1000000, &(&1))
# LibraryManager.process_large_dataset(large_stream, fn id -> 
#   LibraryManager.save_book(id, "Livro #{id}", "Autor #{id}")
# end)
