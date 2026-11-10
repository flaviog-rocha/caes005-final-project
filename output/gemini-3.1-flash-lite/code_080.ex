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
  Sistema de troca de mensagens utilizando Agents, concorrência controlada
  e validação via macros.
  """

  use Agent

  # Macro para validação de campos obrigatórios
  defmacro validate_required(params, fields) do
    quote do
      Enum.each(unquote(fields), fn field ->
        unless Map.has_key?(unquote(params), field) and Map.get(unquote(params), field) != nil do
          raise ArgumentError, "Campo obrigatório ausente: #{field}"
        end
      end)
    end
  end

  # Limite de concorrência usando um Semaphore simples via Task.async_stream
  @max_concurrency 5

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def create(id, data) do
    validate_required(data, [:sender, :content])
    Agent.update(__MODULE__, &Map.put(&1, id, data))
  end

  def get(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  def update(id, data) do
    validate_required(data, [:sender, :content])
    Agent.update(__MODULE__, fn state ->
      if Map.has_key?(state, id), do: Map.put(state, id, data), else: state
    end)
  end

  def delete(id), do: Agent.update(__MODULE__, &Map.delete(&1, id))

  # Execução simultânea limitada
  def process_batch(messages) do
    messages
    |> Task.async_stream(
      fn {id, data} ->
        try do
          create(id, data)
          {:ok, id}
        rescue
          e -> {:error, id, e.message}
        end
      end,
      max_concurrency: @max_concurrency
    )
    |> Enum.to_list()
  end
end

# Exemplo de uso:
# {:ok, _} = MessagingSystem.start_link([])
# MessagingSystem.create(1, %{sender: "Alice", content: "Olá!"})
# IO.inspect(MessagingSystem.get(1))
# MessagingSystem.update(1, %{sender: "Alice", content: "Olá, mundo!"})
# MessagingSystem.delete(1)
