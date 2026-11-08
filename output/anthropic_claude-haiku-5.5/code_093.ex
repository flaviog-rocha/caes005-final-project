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

# Arquivo único: gerenciamento de usuários e sessões em Elixir.
# Requisitos atendidos:
#   1. Banco de dados fictício (Auth.Repo) com snapshot em disco, sem depender de Ecto.
#   2. Recuperação de processos: Supervisor reinicia Agents que falham e as chamadas
#      são repetidas com backoff enquanto o processo reinicia.
#   3. Erros de validação ({:validation, _}) são distintos de falhas operacionais
#      ({:operational, _}).
#
# Uso sugerido (em iex, após compilar este arquivo):
#   {:ok, _} = Auth.Supervisor.start_link(path: "/tmp/auth_repo.snapshot")
#   {:ok, user} = Auth.register(%{email: "ana@exemplo.com", password: "senha1234", name: "Ana"})
#   {:ok, token} = Auth.login(%{email: "ana@exemplo.com", password: "senha1234"})
#   {:ok, session} = Auth.authenticate(token)
#   :ok = Auth.logout(token)

defmodule Auth.Repo do
  @moduledoc false
  # Banco de dados fictício mantido em um Agent.
  # Cada escrita é persistida em snapshot (escrita em arquivo temporário + rename,
  # garantindo atomicidade). O estado só é alterado se a persistência tiver sucesso,
  # então uma falha de disco não deixa a memória inconsistente com o armazenamento.

  use Agent
  require Logger

  @name __MODULE__

  def start_link(opts) do
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "auth_repo.snapshot"))
    Agent.start_link(fn -> %{path: path, users: load_snapshot(path)} end, name: @name)
  end

  @doc "Insere um usuário. Retorna {:ok, user}, {:error, :email_taken} ou {:error, {:persistence, reason}}."
  def insert_user(user) do
    Agent.get_and_update(@name, fn state ->
      if Map.has_key?(state.users, user.email) do
        {{:error, :email_taken}, state}
      else
        new_state = %{state | users: Map.put(state.users, user.email, user)}

        case persist(new_state) do
          :ok ->
            {{:ok, user}, new_state}

          {:error, reason} ->
            Logger.error("falha ao persistir usuário: #{inspect(reason)}")
            {{:error, {:persistence, reason}}, state}
        end
      end
    end)
  end

  @doc "Busca um usuário pelo e-mail. Retorna {:ok, user} ou {:error, :not_found}."
  def fetch_user_by_email(email) do
    Agent.get(@name, fn state ->
      case Map.fetch(state.users, email) do
        {:ok, user} -> {:ok, user}
        :error -> {:error, :not_found}
      end
    end)
  end

  defp persist(%{path: path, users: users}) do
    tmp = path <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(users)) do
      File.rename(tmp, path)
    end
  end

  defp load_snapshot(path) do
    case File.read(path) do
      {:ok, bin} -> decode_snapshot(bin, path)
      {:error, :enoent} -> %{}
      # Falha de leitura (ex.: permissão) é operacional: levanta erro e o supervisor decide.
      {:error, reason} -> raise "não foi possível ler o snapshot: #{inspect(reason)}"
    end
  end

  defp decode_snapshot(bin, path) do
    :erlang.binary_to_term(bin, [:safe])
  rescue
    ArgumentError ->
      # Snapshot corrompido: move para quarentena e inicia com estado vazio.
      Logger.error("snapshot corrompido; movendo para quarentena")
      _ = File.rename(path, path <> ".corrupt")
      %{}
  end
end

defmodule Auth.SessionStore do
  @moduledoc false
  # Sessões ativas mantidas em um Agent (armazenamento volátil).
  # Se este processo falhar, o supervisor o reinicia com estado vazio:
  # os usuários precisarão autenticar novamente. Isso é aceitável para sessões
  # e evita persistir tokens em disco.

  use Agent

  @name __MODULE__

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: @name)
  end

  def put(token, session), do: Agent.update(@name, &Map.put(&1, token, session))

  def get(token) do
    Agent.get(@name, fn sessions ->
      case Map.fetch(sessions, token) do
        {:ok, session} -> {:ok, session}
        :error -> {:error, :not_found}
      end
    end)
  end

  def delete(token), do: Agent.update(@name, &Map.delete(&1, token))
end

defmodule Auth.Supervisor do
  @moduledoc false
  # Supervisiona os processos de estado. Com :one_for_one, a falha de um Agent
  # não derruba o outro; o processo falho é reiniciado automaticamente.

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Auth.Repo, Keyword.take(opts, [:path])},
      Auth.SessionStore
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Auth do
  @moduledoc false
  # Camada de serviço: regras de validação, autenticação e sessões.
  # Retornos padronizados:
  #   {:ok, resultado}
  #   {:error, {:validation, %{campo => mensagem}}}  -> entrada inválida do cliente
  #   {:error, {:operational, motivo}}               -> falha do sistema

  require Logger

  @session_ttl_seconds 30 * 60
  @min_password_length 8
  @email_regex ~r/^[^@\s]+@[^@\s]+\.[^@\s]+$/
  @max_attempts 3
  @backoff_ms 100

  @doc "Cadastra um usuário."
  def register(attrs) do
    with {:ok, input} <- validate_registration(attrs),
         {:ok, user} <- store_user(build_user(input)) do
      {:ok, public_user(user)}
    end
  end

  @doc "Autentica credenciais e devolve um token de sessão."
  def login(attrs) do
    with {:ok, input} <- validate_login(attrs),
         {:ok, user} <- find_user(input.email),
         :ok <- verify_password(user, input.password),
         {:ok, token} <- create_session(user) do
      {:ok, token}
    end
  end

  @doc "Valida um token e devolve os dados da sessão."
  def authenticate(token) when is_binary(token) do
    with {:ok, session} <- fetch_session(token),
         :ok <- ensure_active(token, session) do
      {:ok, Map.take(session, [:user_id, :email])}
    end
  end

  def authenticate(_), do: invalid_session()

  @doc "Encerra uma sessão."
  def logout(token) when is_binary(token) do
    with_recovery(fn -> Auth.SessionStore.delete(token) end)
  end

  def logout(_), do: invalid_session()

  # ---------- Validação ----------

  defp validate_registration(%{} = attrs) do
    email = attrs |> Map.get(:email) |> to_str() |> String.trim() |> String.downcase()
    name = attrs |> Map.get(:name) |> to_str() |> String.trim()
    password = attrs |> Map.get(:password) |> to_str()

    errors =
      []
      |> check(not Regex.match?(@email_regex, email), :email, "formato inválido")
      |> check(name == "", :name, "não pode ser vazio")
      |> check(
        String.length(password) < @min_password_length,
        :password,
        "mínimo de #{@min_password_length} caracteres"
      )

    case errors do
      [] -> {:ok, %{email: email, name: name, password: password}}
      _ -> validation_error(errors)
    end
  end

  defp validate_registration(_), do: validation_error([{:input, "deve ser um mapa"}])

  defp validate_login(%{} = attrs) do
    email = attrs |> Map.get(:email) |> to_str() |> String.trim() |> String.downcase()
    password = attrs |> Map.get(:password) |> to_str()

    errors =
      []
      |> check(email == "", :email, "obrigatório")
      |> check(password == "", :password, "obrigatória")

    case errors do
      [] -> {:ok, %{email: email, password: password}}
      _ -> validation_error(errors)
    end
  end

  defp validate_login(_), do: validation_error([{:input, "deve ser um mapa"}])

  defp check(errors, true, field, message), do: [{field, message} | errors]
  defp check(errors, false, _field, _message), do: errors

  defp to_str(value) when is_binary(value), do: value
  defp to_str(_), do: ""

  defp validation_error(errors), do: {:error, {:validation, Map.new(errors)}}

  defp invalid_credentials,
    do: {:error, {:validation, %{credentials: "e-mail ou senha inválidos"}}}

  defp invalid_session,
    do: {:error, {:validation, %{token: "sessão inválida ou expirada"}}}

  # ---------- Operações de armazenamento (com recuperação) ----------

  defp store_user(user) do
    case with_recovery(fn -> Auth.Repo.insert_user(user) end) do
      {:ok, user} ->
        {:ok, user}

      {:error, :email_taken} ->
        validation_error([{:email, "já cadastrado"}])

      {:error, {:persistence, reason}} ->
        {:error, {:operational, {:persistence_failed, reason}}}

      {:error, {:operational, _}} = error ->
        error
    end
  end

  defp find_user(email) do
    case with_recovery(fn -> Auth.Repo.fetch_user_by_email(email) end) do
      {:ok, user} -> {:ok, user}
      {:error, :not_found} -> invalid_credentials()
      {:error, {:operational, _}} = error -> error
    end
  end

  defp create_session(user) do
    token = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

    session = %{
      user_id: user.id,
      email: user.email,
      expires_at: now() + @session_ttl_seconds
    }

    with :ok <- with_recovery(fn -> Auth.SessionStore.put(token, session) end) do
      {:ok, token}
    end
  end

  defp fetch_session(token) do
    case with_recovery(fn -> Auth.SessionStore.get(token) end) do
      {:ok, session} -> {:ok, session}
      {:error, :not_found} -> invalid_session()
      {:error, {:operational, _}} = error -> error
    end
  end

  defp ensure_active(token, session) do
    if session.expires_at > now() do
      :ok
    else
      _ = with_recovery(fn -> Auth.SessionStore.delete(token) end)
      invalid_session()
    end
  end

  # Executa a operação; se o processo estiver indisponível (saída/exit),
  # aguarda o supervisor reiniciá-lo e tenta novamente com backoff linear.
  defp with_recovery(fun, attempts \\ @max_attempts)

  defp with_recovery(fun, attempts) do
    fun.()
  catch
    :exit, reason when attempts > 1 ->
      Logger.warning("processo indisponível (#{inspect(reason)}); tentando novamente")
      Process.sleep(@backoff_ms * (@max_attempts - attempts + 1))
      with_recovery(fun, attempts - 1)

    :exit, _reason ->
      {:error, {:operational, :service_unavailable}}
  end

  # ---------- Utilitários ----------

  defp build_user(%{email: email, name: name, password: password}) do
    salt = :crypto.strong_rand_bytes(16)

    %{
      id: Base.encode16(:crypto.strong_rand_bytes(8), case: :lower),
      email: email,
      name: name,
      salt: salt,
      password_hash: hash_password(password, salt)
    }
  end

  # Observação: SHA-256 com salt é suficiente para fins didáticos.
  # Em produção, use um KDF dedicado (PBKDF2, Argon2, bcrypt) via biblioteca.
  defp hash_password(password, salt), do: :crypto.hash(:sha256, salt <> password)

  defp verify_password(user, password) do
    if :crypto.hash_equals(hash_password(password, user.salt), user.password_hash) do
      :ok
    else
      invalid_credentials()
    end
  end

  defp public_user(user), do: Map.take(user, [:id, :email, :name])

  defp now, do: System.system_time(:second)
end
