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
  - Utilizamos um Supervisor que gerencia o estado dos Agents. Se um Agent falhar, 
    o Supervisor reinicia o processo garantindo a disponibilidade.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl true
  def init(_) do
    children = [
      {Agent, fn -> %{users: %{}, sessions: %{}} end, name: :state_store}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Domínio e Validações ---

  defmodule ValidationError, do: defexception [:message]
  defmodule OperationalError, do: defexception [:message]

  # --- API Pública ---

  def create_user(email, password) when is_binary(email) and byte_size(password) > 6 do
    Agent.get_and_update(:state_store, fn state ->
      if Map.has_key?(state.users, email) do
        { {:error, %ValidationError{message: "Usuário já existe"}}, state }
      else
        new_users = Map.put(state.users, email, %{password: password})
        { :ok, %{state | users: new_users} }
      end
    end)
  end
  def create_user(_, _), do: {:error, %ValidationError{message: "Dados inválidos"}}

  def login(email, password) do
    state = Agent.get(:state_store, & &1)
    
    case Map.get(state.users, email) do
      nil -> {:error, %ValidationError{message: "Credenciais inválidas"}}
      user when user.password == password ->
        session_token = :crypto.strong_rand_bytes(16) |> Base.encode64()
        
        # Simulação de escrita em DB (Operacional)
        try do
          simulate_db_write!(email, session_token)
          Agent.update(:state_store, fn s -> 
            put_in(s, [:sessions, session_token], email) 
          end)
          {:ok, session_token}
        rescue
          _ -> {:error, %OperationalError{message: "Falha na persistência da sessão"}}
        end
        
      _ -> {:error, %ValidationError{message: "Credenciais inválidas"}}
    end
  end

  # --- Helpers Internos ---

  defp simulate_db_write!(_email, _token) do
    # Simula falha aleatória para demonstrar o tratamento de falhas operacionais
    if :rand.uniform() > 0.95, do: raise("DB Timeout")
  end
end

# Exemplo de uso:
# UserSystem.start_link([])
# UserSystem.create_user("teste@exemplo.com", "senha123")
# UserSystem.login("teste@exemplo.com", "senha123")
