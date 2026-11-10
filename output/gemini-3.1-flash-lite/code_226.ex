# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Gerar notificações quando determinadas condições forem atingidas.
# 5. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSystem do
  @moduledoc """
  Sistema de gerenciamento de usuários e sessões utilizando Agents para estado isolado
  e concorrente, garantindo atomicidade e isolamento.
  """

  use Agent

  # --- API Pública ---

  def start_link(_opts) do
    # Armazena estado como um Map de mapas: %{user_id => %{name: ..., session: ...}}
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc "Cadastrar usuário"
  def create_user(id, name) do
    Agent.update(__MODULE__, fn state ->
      if Map.has_key?(state, id), do: state, else: Map.put(state, id, %{name: name, session: nil})
    end)
  end

  @doc "Consultar usuário"
  def get_user(id), do: Agent.get(__MODULE__, &Map.get(&1, id))

  @doc "Atualizar registro e garantir integridade (Atômico)"
  def update_user(id, new_data) do
    Agent.get_and_update(__MODULE__, fn state ->
      case Map.fetch(state, id) do
        {:ok, user} ->
          updated_user = Map.merge(user, new_data)
          new_state = Map.put(state, id, updated_user)
          check_notifications(id, updated_user)
          {:ok, new_state}
        :error ->
          {:error, state}
      end
    end)
  end

  @doc "Remover registro"
  def delete_user(id) do
    Agent.update(__MODULE__, &Map.delete(&1, id))
  end

  # --- Lógica de Negócio / Notificações ---

  defp check_notifications(id, %{session: session}) when not is_nil(session) do
    # Simula notificação disparada ao atingir condição de sessão ativa
    IO.puts("[NOTIFICAÇÃO] Usuário #{id} iniciou sessão: #{session}")
  end
  defp check_notifications(_id, _user), do: :ok

  # --- Exemplo de Execução Concorrente ---

  def run_demo do
    {:ok, _pid} = start_link([])

    # Operações simultâneas usando Task
    tasks = [
      Task.async(fn -> create_user(1, "Alice") end),
      Task.async(fn -> create_user(2, "Bob") end)
    ]
    Enum.each(tasks, &Task.await/1)

    # Atualização atômica
    update_user(1, %{session: "session_abc_123"})

    IO.inspect(get_user(1), label: "Usuário 1")
    IO.inspect(get_user(2), label: "Usuário 2")

    delete_user(2)
    IO.inspect(get_user(2), label: "Usuário 2 após deleção")
  end
end
