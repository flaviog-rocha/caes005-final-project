# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LibraryManager do
  use Agent

  @type book :: %{id: String.t(), title: String.t(), borrowed: boolean()}
  @type validation_error :: {:validation_error, String.t()}
  @type operational_error :: {:operational_error, String.t()}

  # 1. & 2. Validação e Diferenciação de Erros
  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def borrow_book(id) when not is_binary(id), do: {:validation_error, "ID deve ser string"}

  def borrow_book(id) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, id) do
        nil -> {{:operational_error, "Livro inexistente"}, state}
        %{borrowed: true} -> {{:operational_error, "Livro já emprestado"}, state}
        book ->
          new_state = Map.put(state, id, %{book | borrowed: true})
          {:ok, new_state}
      end
    end)
  end

  def add_book(id, title) when is_binary(id) and is_binary(title) do
    Agent.update(__MODULE__, fn state ->
      Map.put(state, id, %{id: id, title: title, borrowed: false})
    end)
  end

  def add_book(_, _), do: {:validation_error, "Dados inválidos para cadastro"}

  # 3. Operação Periódica
  # Simula um job que reseta empréstimos ou faz auditoria
  def start_periodic_audit(interval_ms) do
    spawn(fn ->
      loop_audit(interval_ms)
    end)
  end

  defp loop_audit(interval) do
    Process.sleep(interval)
    IO.puts("[Audit] Executando auditoria periódica: #{inspect(Agent.get(__MODULE__, & &1))}")
    loop_audit(interval)
  end
end

# Exemplo de uso:
# LibraryManager.start_link([])
# LibraryManager.add_book("1", "Elixir in Action")
# LibraryManager.borrow_book("1")
# LibraryManager.start_periodic_audit(5000)
