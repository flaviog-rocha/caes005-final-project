# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir e não adicione dependências externas. Inclua testes ExUnit que verifiquem os requisitos. Não escreva nada além do código (e comentários do código).

defmodule LibraryLoanHistory do
  defstruct history: [], next_id: 1

  def new, do: %__MODULE__{}

  def record(%__MODULE__{} = state, operation, details \\ %{})
      when is_atom(operation) and is_map(details) do
    entry = %{
      id: state.next_id,
      operation: operation,
      details: details,
      recorded_at: DateTime.utc_now()
    }

    %{state | history: [entry | state.history], next_id: state.next_id + 1}
  end

  def history(%__MODULE__{history: entries}), do: Enum.reverse(entries)
end

ExUnit.start()

defmodule LibraryLoanHistoryTest do
  use ExUnit.Case, async: true

  test "records operations in chronological order with their details" do
    state =
      LibraryLoanHistory.new()
      |> LibraryLoanHistory.record(:loan_created, %{book_id: 12, member_id: 7})
      |> LibraryLoanHistory.record(:loan_returned, %{book_id: 12, member_id: 7})

    entries = LibraryLoanHistory.history(state)

    assert Enum.map(entries, & &1.operation) == [:loan_created, :loan_returned]
    assert Enum.map(entries, & &1.id) == [1, 2]
    assert hd(entries).details == %{book_id: 12, member_id: 7}
    assert Enum.all?(entries, &match?(%DateTime{}, &1.recorded_at))
  end

  test "recording an operation returns a new state without changing the original" do
    original = LibraryLoanHistory.new()
    updated = LibraryLoanHistory.record(original, :loan_created)

    assert LibraryLoanHistory.history(original) == []
    assert length(LibraryLoanHistory.history(updated)) == 1
  end
end
