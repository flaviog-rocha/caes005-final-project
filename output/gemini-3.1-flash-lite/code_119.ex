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
  Gerenciador de empréstimos usando processos isolados, controle de concorrência
  via Task.async_stream e estratégias de retry com Stream.
  """

  # Limite de operações simultâneas (requisito 3)
  @max_concurrency 5
  @max_retries 3

  def process_loans(loans) do
    loans
    |> Stream.map(&prepare_task/1)
    |> Task.async_stream(
      &execute_with_retry/1,
      max_concurrency: @max_concurrency,
      ordered: false
    )
    |> Stream.run()
  end

  defp prepare_task(loan_data) do
    # Encapsula a lógica de negócio em uma função independente (requisito 1)
    fn ->
      IO.puts("Processando empréstimo para: #{loan_data.user}")
      # Simulação de lógica de negócio
      if :rand.uniform() > 0.8, do: raise("Erro temporário de rede")
      {:ok, "Livro #{loan_data.book_id} emprestado com sucesso para #{loan_data.user}"}
    end
  end

  defp execute_with_retry(task_func) do
    # Implementação de novas tentativas (requisito 4)
    Stream.iterate(1, &(&1 + 1))
    |> Enum.take(@max_retries)
    |> Enum.reduce_while(nil, fn attempt, _acc ->
      try do
        {:halt, task_func.()}
      rescue
        e ->
          if attempt < @max_retries do
            IO.puts("Falha na tentativa #{attempt}. Tentando novamente...")
            {:cont, {:error, e}}
          else
            {:halt, {:error, "Falha após #{@max_retries} tentativas"}}
          end
      end
    end)
  end

  # Demonstração de comunicação entre processos (requisito 2)
  # Usando GenServer simples para registro de auditoria centralizado
  defmodule Auditor do
    use GenServer

    def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)
    def init(_), do: {:ok, []}
    def log(msg), do: GenServer.cast(__MODULE__, {:log, msg})
    def handle_cast({:log, msg}, state) do
      IO.inspect("Audit log: #{msg}")
      {:noreply, [msg | state]}
    end
  end
end

# Exemplo de execução
# LibraryManager.Auditor.start_link([])
# loans = [%{user: "Alice", book_id: 1}, %{user: "Bob", book_id: 2}]
# LibraryManager.process_loans(loans)
