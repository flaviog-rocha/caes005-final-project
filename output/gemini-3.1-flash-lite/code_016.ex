# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir a execução de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSessionManager do
  @moduledoc """
  Gerenciador de usuários e sessões utilizando ETS para persistência em memória
  e um pool de processos GenServer para concorrência controlada.
  """

  use GenServer

  @table_name :user_sessions
  @max_concurrent_ops 10

  # --- API Pública ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def create_session(user_id, token) when is_binary(user_id) and is_binary(token) do
    # Comunicação via GenServer garante serialização controlada
    GenServer.call(__MODULE__, {:execute, fn -> 
      :ets.insert(@table_name, {user_id, token})
      :ok
    end})
  end

  def get_session(user_id) when is_binary(user_id) do
    case :ets.lookup(@table_name, user_id) do
      [{^user_id, token}] -> {:ok, token}
      [] -> {:error, :not_found}
    end
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(:ok) do
    :ets.new(@table_name, [:set, :public, :named_table, read_concurrency: true])
    # Semáforo simples usando um contador de processos ativos
    {:ok, %{active_ops: 0}}
  end

  @impl true
  def handle_call({:execute, operation}, _from, state) do
    if state.active_ops < @max_concurrent_ops do
      # Executa a operação de forma concorrente em um novo processo
      spawn(fn -> operation.() end)
      {:reply, :ok, %{state | active_ops: state.active_ops + 1}}
    else
      {:reply, {:error, :too_many_requests}, state}
    end
  end

  # --- Exemplo de uso ---
  # {:ok, _} = UserSessionManager.start_link([])
  # UserSessionManager.create_session("user123", "abc-123-token")
  # UserSessionManager.get_session("user123")
end
