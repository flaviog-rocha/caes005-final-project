# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de usuários e sessões em Elixir (somente biblioteca padrão).
#
# Uso básico:
#   {:ok, _pid} = Auth.Manager.start_link()
#   :ok = Auth.Manager.subscribe()                 # recebe notificações no processo atual
#   {:ok, user} = Auth.Manager.register_user(%{name: "Ana", email: "ana@x.com", password: "senha1234"})
#   {:ok, token} = Auth.Manager.login("ana@x.com", "senha1234")
#   {:ok, user_id} = Auth.Manager.validate_session(token)
#   receive do
#     {:auth_notification, tipo, payload} -> IO.inspect({tipo, payload})
#   end

defmodule Auth.Validator do
  @moduledoc """
  Validação pura dos dados de entrada. Nenhuma operação com estado é executada
  antes que estas funções retornem `{:ok, dados_normalizados}`.
  """

  @email_regex ~r/^[^@\s]+@[^@\s]+\.[^@\s]+$/
  @min_password_length 8
  @max_password_length 256
  @max_name_length 100
  @max_token_length 128

  @doc "Valida e normaliza os atributos de um novo usuário."
  def user_attrs(attrs) when is_map(attrs) do
    # Normaliza antes de validar: remove espaços e padroniza o e-mail
    name = attrs |> Map.get(:name) |> trim_string()
    email = attrs |> Map.get(:email) |> normalize_email()
    password = Map.get(attrs, :password)

    # Cada regra é {condição_satisfeita?, campo, mensagem_de_erro}
    rules = [
      {is_binary(name) and name != "" and String.length(name) <= @max_name_length, :name,
       "é obrigatório e deve ter até #{@max_name_length} caracteres"},
      {is_binary(email) and Regex.match?(@email_regex, email), :email,
       "formato de e-mail inválido"},
      {is_binary(password) and String.length(password) >= @min_password_length and
         String.length(password) <= @max_password_length, :password,
       "deve ter entre #{@min_password_length} e #{@max_password_length} caracteres"}
    ]

    case failed_rules(rules) do
      [] -> {:ok, %{name: name, email: email, password: password}}
      errors -> {:error, errors}
    end
  end

  def user_attrs(_), do: {:error, [{:attrs, "deve ser um mapa"}]}

  @doc "Valida credenciais de login."
  def credentials(email, password) do
    email = normalize_email(email)

    rules = [
      {is_binary(email) and Regex.match?(@email_regex, email), :email,
       "formato de e-mail inválido"},
      {is_binary(password) and password != "" and
         String.length(password) <= @max_password_length, :password,
       "é obrigatório e deve ter no máximo #{@max_password_length} caracteres"}
    ]

    case failed_rules(rules) do
      [] -> {:ok, %{email: email, password: password}}
      errors -> {:error, errors}
    end
  end

  @doc "Valida o formato de um token de sessão."
  def token(token)
      when is_binary(token) and byte_size(token) > 0 and byte_size(token) <= @max_token_length,
      do: {:ok, token}

  def token(_), do: {:error, [{:token, "formato inválido"}]}

  @doc "Valida que todos os valores de configuração são inteiros positivos."
  def config(config) do
    case for({key, value} <- config, not (is_integer(value) and value > 0), do: key) do
      [] -> :ok
      invalid_keys -> {:error, invalid_keys}
    end
  end

  defp failed_rules(rules), do: for({ok?, field, msg} <- rules, not ok?, do: {field, msg})

  defp normalize_email(email) when is_binary(email),
    do: email |> String.trim() |> String.downcase()

  defp normalize_email(other), do: other

  defp trim_string(value) when is_binary(value), do: String.trim(value)
  defp trim_string(other), do: other
end

defmodule Auth.Manager do
  @moduledoc """
  Gerenciador de usuários e sessões com:

    * Validação de dados antes de qualquer operação.
    * Expiração automática de sessões e bloqueios de conta (varredura periódica
      e verificação preguiçosa no acesso).
    * Notificações enviadas aos processos inscritos via `subscribe/2`, nos casos:
      - `:session_expiring`  -> sessão prestes a expirar (dentro da janela de aviso)
      - `:session_expired`   -> sessão expirada e removida
      - `:session_evicted`   -> sessão removida por exceder o limite por usuário
      - `:login_failed`      -> tentativa de login inválida
      - `:account_locked`    -> conta bloqueada por excesso de tentativas
      - `:account_unlocked`  -> bloqueio expirou
  """

  use GenServer

  @default_config %{
    session_ttl_ms: 30 * 60_000,
    expiry_warning_ms: 5 * 60_000,
    sweep_interval_ms: 1_000,
    max_sessions_per_user: 3,
    max_failed_logins: 5,
    lockout_ms: 15 * 60_000
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def register_user(server \\ __MODULE__, attrs),
    do: GenServer.call(server, {:register_user, attrs})

  def login(server \\ __MODULE__, email, password),
    do: GenServer.call(server, {:login, email, password})

  def validate_session(server \\ __MODULE__, token),
    do: GenServer.call(server, {:validate_session, token})

  def logout(server \\ __MODULE__, token),
    do: GenServer.call(server, {:logout, token})

  def subscribe(server \\ __MODULE__, pid \\ self()),
    do: GenServer.call(server, {:subscribe, pid})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    config = Map.merge(@default_config, Map.new(opts))

    case Auth.Validator.config(config) do
      :ok ->
        schedule_sweep(config.sweep_interval_ms)

        {:ok,
         %{
           config: config,
           users: %{},
           emails: %{},
           sessions: %{},
           failures: %{},
           locks: %{},
           subscribers: %{}
         }}

      {:error, keys} ->
        {:stop, {:invalid_config, keys}}
    end
  end

  @impl true
  def handle_call({:register_user, attrs}, _from, state) do
    case Auth.Validator.user_attrs(attrs) do
      {:error, errors} ->
        {:reply, {:error, {:validation, errors}}, state}

      {:ok, data} ->
        if Map.has_key?(state.emails, data.email) do
          {:reply, {:error, :email_taken}, state}
        else
          id = random_id()
          salt = :crypto.strong_rand_bytes(16)

          user = %{
            id: id,
            name: data.name,
            email: data.email,
            salt: salt,
            password_hash: hash_password(salt, data.password),
            created_at: now()
          }

          state = %{
            state
            | users: Map.put(state.users, id, user),
              emails: Map.put(state.emails, data.email, id)
          }

          {:reply, {:ok, Map.take(user, [:id, :name, :email])}, state}
        end
    end
  end

  def handle_call({:login, email, password}, _from, state) do
    case Auth.Validator.credentials(email, password) do
      {:error, errors} ->
        {:reply, {:error, {:validation, errors}}, state}

      {:ok, %{email: email, password: password}} ->
        do_login(email, password, state)
    end
  end

  def handle_call({:validate_session, token}, _from, state) do
    case Auth.Validator.token(token) do
      {:error, errors} ->
        {:reply, {:error, {:validation, errors}}, state}

      {:ok, token} ->
        case Map.fetch(state.sessions, token) do
          :error ->
            {:reply, {:error, :not_found}, state}

          {:ok, session} ->
            # Verificação preguiçosa: não aceita sessão vencida mesmo antes da varredura
            if expired?(session, now()) do
              {:reply, {:error, :expired}, expire(state, token)}
            else
              {:reply, {:ok, session.user_id}, state}
            end
        end
    end
  end

  def handle_call({:logout, token}, _from, state) do
    case Auth.Validator.token(token) do
      {:error, errors} ->
        {:reply, {:error, {:validation, errors}}, state}

      {:ok, token} ->
        {:reply, :ok, %{state | sessions: Map.delete(state.sessions, token)}}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) when is_pid(pid) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)
      {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, pid, ref)}}
    end
  end

  def handle_call({:subscribe, _invalid}, _from, state),
    do: {:reply, {:error, {:validation, [{:pid, "deve ser um pid"}]}}, state}

  @impl true
  def handle_info(:sweep, state) do
    now = now()

    state =
      state
      |> expire_sessions(now)
      |> warn_expiring_sessions(now)
      |> release_locks(now)

    schedule_sweep(state.config.sweep_interval_ms)
    {:noreply, state}
  end

  # Remove inscrições de processos que terminaram
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Login e bloqueio
  # ---------------------------------------------------------------------------

  defp do_login(email, password, state) do
    now = now()

    case Map.fetch(state.emails, email) do
      :error ->
        # Mesma resposta de credencial inválida para não revelar contas existentes
        {:reply, {:error, :invalid_credentials}, state}

      {:ok, id} ->
        cond do
          locked?(state, email, now) ->
            {:reply, {:error, :locked}, state}

          password_ok?(state.users[id], password) ->
            {token, state} = create_session(id, state, now)
            state = %{state | failures: Map.delete(state.failures, email)}
            {:reply, {:ok, token}, state}

          true ->
            state = register_failure(state, email, now)
            {:reply, {:error, :invalid_credentials}, state}
        end
    end
  end

  defp locked?(state, email, now) do
    case Map.get(state.locks, email) do
      nil -> false
      until -> now < until
    end
  end

  defp register_failure(state, email, now) do
    count = Map.get(state.failures, email, 0) + 1

    # Condição: número máximo de tentativas falhas atingido -> bloqueio temporário
    if count >= state.config.max_failed_logins do
      until = now + state.config.lockout_ms
      notify(state, :account_locked, %{email: email, locked_until: until})

      %{
        state
        | failures: Map.delete(state.failures, email),
          locks: Map.put(state.locks, email, until)
      }
    else
      notify(state, :login_failed, %{email: email, attempts: count})
      %{state | failures: Map.put(state.failures, email, count)}
    end
  end

  # ---------------------------------------------------------------------------
  # Sessões
  # ---------------------------------------------------------------------------

  defp create_session(user_id, state, now) do
    config = state.config

    active =
      state.sessions
      |> Enum.map(fn {_token, s} -> s end)
      |> Enum.filter(&(&1.user_id == user_id))
      |> Enum.sort_by(& &1.created_at)

    # Condição: limite de sessões por usuário atingido -> remove as mais antigas
    excess = length(active) - config.max_sessions_per_user + 1

    state =
      active
      |> Enum.take(max(excess, 0))
      |> Enum.reduce(state, fn session, acc ->
        notify(acc, :session_evicted, %{user_id: user_id, reason: :max_sessions_reached})
        %{acc | sessions: Map.delete(acc.sessions, session.token)}
      end)

    token = random_token()

    session = %{
      token: token,
      user_id: user_id,
      created_at: now,
      expires_at: now + config.session_ttl_ms,
      warned: false
    }

    {token, %{state | sessions: Map.put(state.sessions, token, session)}}
  end

  defp expired?(session, now), do: session.expires_at <= now

  defp expire(state, token) do
    case Map.pop(state.sessions, token) do
      {nil, _} ->
        state

      {session, sessions} ->
        notify(state, :session_expired, %{user_id: session.user_id})
        %{state | sessions: sessions}
    end
  end

  defp expire_sessions(state, now) do
    state.sessions
    |> Enum.filter(fn {_token, s} -> expired?(s, now) end)
    |> Enum.reduce(state, fn {token, _}, acc -> expire(acc, token) end)
  end

  # Condição: sessão entrou na janela de aviso de expiração (notifica uma única vez)
  defp warn_expiring_sessions(state, now) do
    window = state.config.expiry_warning_ms

    sessions =
      Map.new(state.sessions, fn {token, s} ->
        remaining = s.expires_at - now

        if not s.warned and remaining > 0 and remaining <= window do
          notify(state, :session_expiring, %{user_id: s.user_id, expires_in_ms: remaining})
          {token, %{s | warned: true}}
        else
          {token, s}
        end
      end)

    %{state | sessions: sessions}
  end

  # ---------------------------------------------------------------------------
  # Expiração de bloqueios
  # ---------------------------------------------------------------------------

  defp release_locks(state, now) do
    {expired, active} = Enum.split_with(state.locks, fn {_email, until} -> now >= until end)

    Enum.each(expired, fn {email, _} ->
      notify(state, :account_unlocked, %{email: email})
    end)

    %{state | locks: Map.new(active)}
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  defp notify(state, type, payload) do
    for {pid, _ref} <- state.subscribers do
      send(pid, {:auth_notification, type, payload})
    end

    :ok
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)

  defp now, do: System.system_time(:millisecond)

  defp random_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)

  defp random_token, do: :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)

  # Observação: SHA-256 com salt é suficiente para demonstração. Em produção,
  # utilize uma função de derivação de senhas dedicada (ex.: PBKDF2, Argon2, bcrypt).
  defp hash_password(salt, password), do: :crypto.hash(:sha256, [salt, password])

  defp password_ok?(nil, _password), do: false

  defp password_ok?(user, password) do
    # Comparação em tempo constante (disponível a partir do OTP 25)
    :crypto.hash_equals(user.password_hash, hash_password(user.salt, password))
  end
end
