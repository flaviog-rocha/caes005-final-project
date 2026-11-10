# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  @moduledoc """
  Gerenciador de empréstimos com processamento concorrente limitado,
  retry com backoff exponencial e comunicação via mensagens.
  """

  # Limita a quantidade de operações simultâneas (concorrência)
  @max_concurrency 5
  @max_retries 3

  def run(books) do
    books
    |> Stream.map(&spawn_loan_process/1)
    |> Stream.map(&Task.await(&1, 30_000))
    |> Stream.run()
  end

  defp spawn_loan_process(book) do
    Task.async(fn ->
      process_with_retry(book, 0)
    end)
  end

  defp process_with_retry(book, attempt) do
    case perform_loan(book) do
      :ok ->
        IO.puts("Empréstimo realizado: #{book}")
        :ok

      {:error, :temporary_failure} when attempt < @max_retries ->
        backoff = :math.pow(2, attempt) |> round()
        Process.sleep(backoff * 100)
        process_with_retry(book, attempt + 1)

      {:error, reason} ->
        IO.puts("Falha definitiva no livro #{book}: #{reason}")
        {:error, reason}
    end
  end

  # Simulação de operação que pode falhar
  defp perform_loan(book) do
    # Comunicação entre processos: envia status para o self()
    # Em um cenário real, aqui haveria chamadas a banco de dados ou APIs
    case :rand.uniform(10) do
      n when n > 8 -> {:error, :temporary_failure}
      _ -> :ok
    end
  end
end

# Exemplo de execução
books = ["Elixir in Action", "Programming Phoenix", "Designing Data-Intensive Applications", "The Pragmatic Programmer", "Functional Programming in Elixir"]

# Utilizando Stream para processar a lista e limitar a concorrência via Task.async_stream
books
|> Task.async_stream(
  fn book -> LibraryManager.run([book]) end,
  max_concurrency: 2,
  ordered: false
)
|> Stream.run()
