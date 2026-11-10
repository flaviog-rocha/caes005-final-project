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
  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def borrow_book(id) when not is_binary(id), do: {:validation_error, "ID inválido"}

  def borrow_book(id) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.get(state, id) do
        nil -> 
          {{:operational_error, "Livro não encontrado"}, state}
        %{borrowed: true} -> 
          {{:operational_error, "Livro já está emprestado"}, state}
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

  def add_book(_, _), do: {:validation_error, "Dados de livro inválidos"}

  # 3. Operação Periódica
  # Simula um processo de limpeza/verificação de status periodicamente
  def start_periodic_cleanup(interval_ms) do
    spawn(fn ->
      loop_cleanup(interval_ms)
    end)
  end

  defp loop_cleanup(interval_ms) do
    :timer.sleep(interval_ms)
    
    Agent.update(__MODULE__, fn state ->
      IO.puts("[System] Executando manutenção periódica...")
      # Exemplo: Lógica que reseta livros esquecidos (dummy logic)
      state
    end)

    loop_cleanup(interval_ms)
  end
end

# Exemplo de uso:
# {:ok, _pid} = LibraryManager.start_link([])
# LibraryManager.add_book("1", "Elixir in Action")
# LibraryManager.borrow_book("1")
# LibraryManager.start_periodic_cleanup(5000)
