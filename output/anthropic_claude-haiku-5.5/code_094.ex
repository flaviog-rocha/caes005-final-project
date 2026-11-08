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

# Gerenciamento de usuários e sessões com Agent, recuperação via Supervisor
# e separação entre erros de validação e falhas operacionais.
#
# Exemplo de uso (comentado):
#   {:ok, _sup} = Auth.Supervisor.start_link(path: "/tmp/auth_db.term")
#   {:ok, user} = Auth.Accounts.register(%{name: "Ana", email: "ana@ex.com", password: "senha12345"})
#   {:ok, session} = Auth.Accounts.authenticate("ana@ex.com", "senha12345")
#   {:ok, _} = Auth.Accounts.validate_session(session.token)
#   Auth.FakeDB.simulate_crash()   # o Supervisor reinicia o Agent recarregando do disco
#   {:ok, _} = Auth.Accounts.validate_session(session.token)
#
# Contrato de erros:
#   {:error, {:validation, detalhes}}  -> entrada inválida ou regra de negócio violada (não repetir)
#   {:error, {:operational, motivo}}   -> falha de infraestrutura (banco indisponível, IO etc.)

defmodule Auth.Crypto do
  @moduledoc false
  # Hash com SHA-256 e sal. Adequado apenas para fins didáticos;
  # em produção use PBKDF2, Argon2 ou bcrypt.

  # Gera um hash com sal aleatório para a senha informada.
  def hash_password(password) do
    salt = :crypto.strong_rand_bytes(16)
    {salt, digest(salt, password)}
  end

  # Compara em tempo constante (requer OTP 25+).
  def verify_password?(password, {salt, expected}) do
    :crypto.hash_equals(digest(salt, password), expected)
  end

  def random_token(bytes \\ 32) do
    bytes
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  def random_id, do: random_token(12)

  defp digest(salt, password), do: :crypto.hash(:sha256, salt <> password)
end

defmodule Auth.Validation do
  @moduledoc false
  # Validações puras. Apenas produzem erros do tipo :validation.

  @email_regex ~r/^[^\s@]+@[^\s@]+\.[^\s@]+$/

  # Valida e normaliza os dados de cadastro.
  def register(attrs) when is_map(attrs) do
    input = %{
      name: attrs |> field(:name) |> String.trim(),
      email: attrs |> field(:email) |> String.trim() |> String.downcase(),
      password: field(attrs, :password)
    }

    errors =
      %{}
      |> maybe_put(input.name == "", :name, "é obrigatório")
      |> maybe_put(not Regex.match?(@email_regex, input.email), :email, "formato inválido")
      |> maybe_put(String.length(input.password) < 8, :password, "deve ter ao menos 8 caracteres")

    if map_size(errors) == 0,
      do: {:ok, input},
      else: {:error, {:validation, errors}}
  end

  def register(_), do: {:error, {:validation, %{attrs: "deve ser um mapa"}}}

  # Valida e normaliza um e-mail.
  def email(email) when is_binary(email) do
    normalized = email |> String.trim() |> String.downcase()

    if Regex.match?(@email_regex, normalized),
      do: {:ok, normalized},
      else: {:error, {:validation, %{email: "formato inválido"}}}
  end

  def email(_), do: {:error, {:validation, %{email: "deve ser texto"}}}

  defp field(attrs, key) do
    value = Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))
    if is_binary(value), do: value, else: ""
  end

  defp maybe_put(errors, true, key, msg), do: Map.put(errors, key, msg)
  defp maybe_put(errors, false, _key, _msg), do: errors
end

defmodule Auth.FakeDB do
  @moduledoc false
  # Banco fictício mantido em um Agent e persistido em disco (term_to_binary).
  # Ao reiniciar, o Agent recarrega o estado do arquivo, garantindo recuperação.
  # Não depende de Ecto; uma migração poderia ser adicionada futuramente.

  use Agent

  @name __MODULE__
  @empty %{users: %{}, sessions: %{}}

  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "auth_fake_db.term"))
    Agent.start_link(fn -> %{path: path, data: load(path)} end, name: @name)
  end

  def insert_user(user) do
    transact(fn data ->
      if Enum.any?(Map.values(data.users), &(&1.email == user.email)) do
        {:read, {:error, {:validation, %{email: "já cadastrado"}}}}
      else
        {:write, {:ok, user}, put_in(data, [:users, user.id], user)}
      end
    end)
  end

  def find_user_by_email(email) do
    transact(fn data ->
      case Enum.find(Map.values(data.users), &(&1.email == email)) do
        nil -> {:read, {:error, :not_found}}
        user -> {:read, {:ok, user}}
      end
    end)
  end

  def insert_session(session) do
    transact(fn data ->
      {:write, {:ok, session}, put_in(data, [:sessions, session.token], session)}
    end)
  end

  def fetch_session(token) do
    transact(fn data ->
      case Map.fetch(data.sessions, token) do
        {:ok, session} -> {:read, {:ok, session}}
        :error -> {:read, {:error, :not_found}}
      end
    end)
  end

  def delete_session(token) do
    transact(fn data ->
      {:write, :ok, update_in(data, [:sessions], &Map.delete(&1, token))}
    end)
  end

  # Provoca uma falha no processo para testar a recuperação pelo Supervisor.
  def simulate_crash do
    Agent.cast(@name, fn _state -> raise "falha simulada no processo do banco" end)
  end

  # Executa a operação dentro do Agent. Escritas só são aplicadas em memória
  # após persistência bem-sucedida; caso contrário, o estado é preservado.
  defp transact(fun) do
    Agent.get_and_update(@name, fn %{path: path, data: data} = state ->
      case fun.(data) do
        {:read, reply} ->
          {reply, state}

        {:write, reply, new_data} ->
          case persist(path, new_data) do
            :ok -> {reply, %{state | data: new_data}}
            {:error, reason} -> {{:error, {:operational, {:persist_failed, reason}}}, state}
          end
      end
    end)
  catch
    # Agent indisponível (reiniciando ou morto): falha operacional, recuperável.
    :exit, reason -> {:error, {:operational, {:db_unavailable, reason}}}
  end

  defp load(path) do
    case File.read(path) do
      {:ok, binary} -> :erlang.binary_to_term(binary, [:safe])
      {:error, :enoent} -> @empty
      {:error, reason} -> raise "falha ao ler #{path}: #{inspect(reason)}"
    end
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp persist(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end

defmodule Auth.Accounts do
  @moduledoc false
  # API pública de usuários e sessões.

  @session_ttl_seconds 3_600
  @max_attempts 3

  # Cadastra um usuário. Retorna o usuário sem a senha.
  def register(attrs) do
    with {:ok, input} <- Auth.Validation.register(attrs) do
      user = %{
        id: Auth.Crypto.random_id(),
        name: input.name,
        email: input.email,
        password: Auth.Crypto.hash_password(input.password),
        inserted_at: now()
      }

      case with_retry(fn -> Auth.FakeDB.insert_user(user) end) do
        {:ok, saved} -> {:ok, Map.delete(saved, :password)}
        error -> error
      end
    end
  end

  # Autentica por e-mail e senha e cria uma sessão.
  def authenticate(email, password) when is_binary(password) do
    with {:ok, email} <- Auth.Validation.email(email) do
      case with_retry(fn -> Auth.FakeDB.find_user_by_email(email) end) do
        {:ok, user} ->
          if Auth.Crypto.verify_password?(password, user.password),
            do: create_session(user),
            else: invalid_credentials()

        {:error, :not_found} ->
          invalid_credentials()

        {:error, {:operational, _}} = error ->
          error
      end
    end
  end

  def authenticate(_email, _password),
    do: {:error, {:validation, %{password: "deve ser texto"}}}

  # Verifica se o token existe e não expirou.
  def validate_session(token) when is_binary(token) do
    case with_retry(fn -> Auth.FakeDB.fetch_session(token) end) do
      {:ok, %{expires_at: expires_at} = session} ->
        if expires_at > now(), do: {:ok, session}, else: expire(token)

      {:error, :not_found} ->
        {:error, {:validation, %{session: "inválida"}}}

      {:error, {:operational, _}} = error ->
        error
    end
  end

  def validate_session(_token),
    do: {:error, {:validation, %{session: "deve ser texto"}}}

  # Encerra uma sessão.
  def logout(token) when is_binary(token) do
    with_retry(fn -> Auth.FakeDB.delete_session(token) end)
  end

  defp create_session(user) do
    session = %{
      token: Auth.Crypto.random_token(),
      user_id: user.id,
      expires_at: now() + @session_ttl_seconds
    }

    with_retry(fn -> Auth.FakeDB.insert_session(session) end)
  end

  # Remove sessão expirada (limpeza best-effort; o resultado não altera a resposta).
  defp expire(token) do
    _ = with_retry(fn -> Auth.FakeDB.delete_session(token) end)
    {:error, {:validation, %{session: "expirada"}}}
  end

  defp invalid_credentials,
    do: {:error, {:validation, %{credentials: "e-mail ou senha inválidos"}}}

  # Repete somente falhas operacionais, com backoff linear.
  defp with_retry(fun), do: do_retry(fun, 1)

  defp do_retry(fun, attempt) do
    case fun.() do
      {:error, {:operational, _}} when attempt < @max_attempts ->
        Process.sleep(50 * attempt)
        do_retry(fun, attempt + 1)

      result ->
        result
    end
  end

  defp now, do: System.system_time(:second)
end

defmodule Auth.Supervisor do
  @moduledoc false
  # Supervisor responsável por reiniciar o banco fictício após falhas.

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Auth.FakeDB, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end
