# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Recuperar processos após falhas inesperadas.
# 3. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule UserSystem do
  @moduledoc """
  Sistema de gerenciamento de usuários e sessões utilizando Agents.
  
  Estratégia de Recuperação:
  - Supervisionamento hierárquico para garantir que processos sejam reiniciados.
  - O estado é persistido no Agent, que atua como um storage em memória.
  """

  use Supervisor

  # --- Tipos e Estruturas ---
  defstruct id: nil, username: nil, status: :active
  
  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      {Agent, fn -> %{users: %{}, sessions: %{}} end, name: UserStore},
      {UserSessionSupervisor, []}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Lógica de Negócio ---

  def create_user(username) when is_binary(username) and byte_size(username) > 3 do
    # Simulação de interação com banco de dados
    Agent.update(UserStore, fn state ->
      id = System.unique_integer([:positive])
      user = %UserSystem{id: id, username: username}
      %{state | users: Map.put(state.users, id, user)}
    end)
  rescue
    _ -> raise OperationalError, message: "Falha ao gravar no banco de dados persistente"
  end

  def create_user(_), do: raise(ValidationError, message: "Username inválido")

  def create_session(user_id) do
    state = Agent.get(UserStore, & &1)
    
    if Map.has_key?(state.users, user_id) do
      token = :crypto.strong_rand_bytes(16) |> Base.encode16()
      Agent.update(UserStore, fn s -> 
        %{s | sessions: Map.put(s.sessions, token, user_id)} 
      end)
      token
    else
      raise OperationalError, message: "Usuário não encontrado"
    end
  end
end

defmodule UserSessionSupervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok)

  def init(:ok) do
    # Garante a resiliência: se um processo de sessão cair, o supervisor o reinicia
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: SessionManager}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# --- Exemplo de Uso / Teste ---
# UserSystem.start_link([])
# try do
#   UserSystem.create_user("Alice")
#   token = UserSystem.create_session(1)
#   IO.puts("Sessão criada: #{token}")
# rescue
#   e in UserSystem.ValidationError -> IO.puts("Erro de validação: #{e.message}")
#   e in UserSystem.OperationalError -> IO.puts("Falha operacional: #{e.message}")
# end
