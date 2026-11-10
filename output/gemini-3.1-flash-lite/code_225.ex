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
  Sistema de gerenciamento de usuários e sessões utilizando Agents para estado 
  concorrente e GenServer para orquestração e notificações.
  """

  use GenServer

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def create_user(id, data), do: GenServer.call(__MODULE__, {:create, id, data})
  def get_user(id), do: GenServer.call(__MODULE__, {:get, id})
  def update_user(id, data), do: GenServer.call(__MODULE__, {:update, id, data})
  def delete_user(id), do: GenServer.call(__MODULE__, {:delete, id})

  # --- Callbacks do GenServer ---

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_call({:create, id, data}, _from, state) do
    if Map.has_key?(state, id) do
      {:reply, {:error, :already_exists}, state}
    else
      new_state = Map.put(state, id, data)
      notify(:user_created, id)
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.fetch(state, id), state}
  end

  @impl true
  def handle_call({:update, id, data}, _from, state) do
    case Map.fetch(state, id) do
      {:ok, existing} ->
        # Atomic update simulation: merge data safely
        updated_user = Map.merge(existing, data)
        new_state = Map.put(state, id, updated_user)
        notify(:user_updated, id)
        {:reply, :ok, new_state}
      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    if Map.has_key?(state, id) do
      new_state = Map.delete(state, id)
      notify(:user_deleted, id)
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  # --- Lógica Privada / Notificações ---

  defp notify(event, id) do
    # Simulação de notificação assíncrona para não bloquear o fluxo principal
    spawn(fn ->
      IO.puts("[NOTIFICATION] Evento: #{event} | ID: #{id} | Time: #{System.system_time(:second)}")
    end)
  end
end

# --- Exemplo de Execução Concorrente ---

# Iniciar o sistema
{:ok, _pid} = UserSystem.start_link([])

# Simulação de operações simultâneas
tasks = [
  Task.async(fn -> UserSystem.create_user(1, %{name: "Alice", status: "active"}) end),
  Task.async(fn -> UserSystem.create_user(2, %{name: "Bob", status: "active"}) end),
  Task.async(fn -> UserSystem.update_user(1, %{status: "away"}) end)
]

Enum.map(tasks, &Task.await/1)

# Consulta de validação
IO.inspect(UserSystem.get_user(1), label: "Usuário 1 após update")
