# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Fazer uso de macros.
# 4. Permitir limitar a quantidade de operações simultâneas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessagingSystem do
  @moduledoc """
  Sistema de troca de mensagens utilizando Agent e semáforo para controle de concorrência.
  """

  # Macro para validação de dados
  defmacro validate_presence(fields) do
    quote do
      Enum.each(unquote(fields), fn field ->
        unless Map.has_key?(params, field) do
          raise ArgumentError, "Campo obrigatório ausente: #{field}"
        end
      end)
    end
  end

  def start_link(max_concurrency) do
    # Agent para armazenar as mensagens
    {:ok, _} = Agent.start_link(fn -> %{} end, name: __MODULE__.Store)
    # Agent para controle de concorrência (semáforo)
    {:ok, _} = Agent.start_link(fn -> max_concurrency end, name: __MODULE__.Semaphore)
  end

  # Wrapper para limitar operações simultâneas
  defp with_concurrency_limit(func) do
    Agent.get_and_update(__MODULE__.Semaphore, fn
      n when n > 0 -> {func.(), n - 1}
      n -> {:error, n}
    end)
    |> tap(fn _ -> Agent.update(__MODULE__.Semaphore, &(&1 + 1)) end)
  end

  def create_message(params) do
    require MessagingSystem
    validate_presence([:id, :sender, :content])

    with_concurrency_limit(fn ->
      Agent.update(__MODULE__.Store, &Map.put(&1, params.id, params))
    end)
  end

  def get_message(id) do
    Agent.get(__MODULE__.Store, &Map.get(&1, id))
  end

  def update_message(id, new_content) do
    with_concurrency_limit(fn ->
      Agent.update(__MODULE__.Store, fn store ->
        Map.update!(store, id, &Map.put(&1, :content, new_content))
      end)
    end)
  end

  def delete_message(id) do
    with_concurrency_limit(fn ->
      Agent.update(__MODULE__.Store, &Map.delete(&1, id))
    end)
  end
end

# Exemplo de uso:
# MessagingSystem.start_link(2) # Limite de 2 operações simultâneas
# MessagingSystem.create_message(%{id: 1, sender: "Alice", content: "Olá!"})
# MessagingSystem.get_message(1)
# MessagingSystem.update_message(1, "Olá, Elixir!")
# MessagingSystem.delete_message(1)
