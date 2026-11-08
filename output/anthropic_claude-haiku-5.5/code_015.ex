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

# Arquivo: user_session_manager.ex
#
# Gerenciamento de usuários e sessões com:
#   1. Validação de dados antes de qualquer operação.
#   2. Comunicação entre processos (assinaturas e eventos via mensagens).
#   3. Limite de operações simultâneas (semáforo atômico sobre ETS).
#   4. Execução de operações simultâneas (Task.async_stream limitado pelo semáforo).
#
# Dependências: somente a biblioteca padrão (ETS, GenServer, Task, :crypto).

defmodule UserSessionManager.Validator do
  @moduledoc false
  # Validações puras. Nenhuma operação do sistema é executada com dados inválidos.

  @email_regex ~r/^[^\s@]+@[^\s@]+\.[^\s@]+$/
  @token_regex ~r/^[A-Za-z0-9_-]{43}$/

  @doc "Valida atributos de criação de usuário. Retorna dados normalizados."
  def validate_user(attrs) when is_map(attrs) do
    results = [
      check(attrs, :name, &valid_name/1),
      check(attrs, :email, &valid_email/1),
      check(attrs, :password, &valid_password/1)
    ]

    case errors(results) do
      [] ->
        {:ok,
         %{
           name: String.trim(attrs.name),
           email: normalize_email(attrs.email),
           password: attrs.password
         }}

      errs ->
        {:error, errs}
    end
  end

  def validate_user(_), do: {:error, [{:attrs, :invalid_type}]}

  @doc "Valida credenciais de autenticação."
  def validate_credentials(email, password) do
    attrs = %{email: email, password: password}

    case errors([
           check(attrs, :email, &valid_email/1),
           check(attrs, :password, &valid_password/1)
         ]) do
      [] -> {:ok, %{email: normalize_email(email), password: password}}
      errs -> {:error, errs}
    end
  end

  @doc "Valida o formato de um token de sessão gerado pelo sistema."
  def validate_token(token) when is_binary(token) do
    if Regex.match?(@token_regex, token),
      do: {:ok, token},
      else: {:error, :invalid_token}
  end

  def validate_token(_), do: {:error, :invalid_token}

  # Helpers ---------------------------------------------------------------

  defp check(attrs, key, validator) do
    case Map.fetch(attrs, key) do
      {:ok, value} ->
        case validator.(value) do
          :ok -> :ok
          {:error, reason} -> {:error, {key, reason}}
        end

      :error ->
        {:error, {key, :missing}}
    end
  end

  defp errors(results), do: for({:error, e} <- results, do: e)

  defp valid_name(name) when is_binary(name) do
    len = name |> String.trim() |> String.length()
    if len in 2..100, do: :ok, else: {:error, :invalid_length}
  end

  defp valid_name(_), do: {:error, :invalid_type}

  defp valid_email(email) when is_binary(email) do
    cond do
      byte_size(email) > 254 -> {:error, :too_long}
      Regex.match?(@email_regex, String.trim(email)) -> :ok
      true -> {:error, :invalid_format}
    end
  end

  defp valid_email(_), do: {:error, :invalid_type}

  defp valid_password(password) when is_binary(password) do
    len = String.length(password)
    if len in 8..128, do: :ok, else: {:error, :invalid_length}
  end

  defp valid_password(_), do: {:error, :invalid_type}

  defp normalize_email(email), do: email |> String.trim() |> String.downcase()
end

defmodule UserSessionManager.Limiter do
  @moduledoc false
  # Semáforo de contagem sobre ETS. O contador é incrementado atomicamente
  # com :ets.update_counter/3, dispensando um processo centralizador.
  # Se o limite for excedido, o incremento é desfeito e a operação aguarda
  # até o timeout antes de desistir com {:error, :busy}.

  @table :usm_limiter
  @retry_ms 5

  @doc false
  def create_table(max) do
    :ets.new(@table, [:set, :named_table, :public, write_concurrency: true])
    :ets.insert(@table, [{:max, max}, {:active, 0}])
    :ok
  end

  @doc "Retorna o limite configurado de operações simultâneas."
  def max, do: :ets.lookup_element(@table, :max, 2)

  @doc "Retorna o número de operações ativas no momento."
  def active, do: :ets.lookup_element(@table, :active, 2)

  @doc "Executa `fun` adquirindo uma permissão do semáforo."
  def run(fun, timeout) when is_function(fun, 0) and is_integer(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    case acquire(deadline) do
      :ok ->
        try do
          fun.()
        after
          release()
        end

      :error ->
        {:error, :busy}
    end
  end

  defp acquire(deadline) do
    if :ets.update_counter(@table, :active, {2, 1}) <= max() do
      :ok
    else
      release()

      if System.monotonic_time(:millisecond) >= deadline do
        :error
      else
        Process.sleep(@retry_ms)
        acquire(deadline)
      end
    end
  end

  defp release, do: :ets.update_counter(@table, :active, {2, -1})
end

defmodule UserSessionManager do
  @moduledoc """
  API pública e processo coordenador do gerenciamento de usuários e sessões.

  Tabelas ETS:
    * `:usm_users`    - `:protected`, chave: email, valor: `{email, name, salt, hash}`
    * `:usm_sessions` - `:protected`, chave: token, valor: `{token, email, expires_at}`
    * `:usm_limiter`  - `:public`, contadores do semáforo de concorrência

  Escritas passam pelo GenServer (serialização das mutações). Leituras de
  sessão são feitas diretamente na ETS, sem gargalo de mensagens.

  Comunicação entre processos: processos podem se inscrever com `subscribe/0`
  e receber `{:usm_event, evento}` sempre que um usuário ou sessão for
  criado, encerrado ou revogado. Assinantes são monitorados e removidos
  automaticamente ao terminarem.
  """

  use GenServer

  alias UserSessionManager.{Limiter, Validator}

  @users :usm_users
  @sessions :usm_sessions
  @default_ttl :timer.hours(1)
  @default_sweep :timer.minutes(1)
  @default_timeout 5_000
  @hash_iterations 10_000

  # API pública ----------------------------------------------------------

  @doc """
  Inicia o gerenciador.

  Opções:
    * `:max_concurrency` - limite de operações simultâneas (padrão: 2 * schedulers)
    * `:session_ttl`     - duração da sessão em ms (padrão: 1 hora)
    * `:sweep_interval`  - intervalo de limpeza de sessões expiradas em ms
    * `:name`            - nome do processo (padrão: `__MODULE__`)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Cria um usuário após validação. Retorna `{:ok, %{name, email}}`."
  def create_user(attrs), do: GenServer.call(__MODULE__, {:create_user, attrs})

  @doc "Autentica e cria uma sessão. Retorna `{:ok, token}`."
  def authenticate(email, password),
    do: GenServer.call(__MODULE__, {:authenticate, email, password})

  @doc "Encerra uma sessão pelo token."
  def logout(token), do: GenServer.call(__MODULE__, {:logout, token})

  @doc """
  Valida uma sessão diretamente na ETS (leitura sem passar pelo GenServer).
  Retorna `{:ok, %{email, name}}` ou `{:error, motivo}`.
  """
  def validate_session(token) do
    with {:ok, token} <- Validator.validate_token(token),
         {:ok, email, expires_at} <- fetch_session(token),
         :ok <- check_expiry(expires_at),
         {:ok, name} <- fetch_user_name(email) do
      {:ok, %{email: email, name: name}}
    end
  end

  @doc "Inscreve o processo chamador para receber eventos `{:usm_event, evento}`."
  def subscribe, do: GenServer.call(__MODULE__, {:subscribe, self()})

  @doc "Remove a inscrição do processo chamador."
  def unsubscribe, do: GenServer.call(__MODULE__, {:unsubscribe, self()})

  @doc """
  Executa uma função respeitando o limite de operações simultâneas.
  Retorna `{:error, :busy}` se não houver permissão dentro do timeout.
  """
  def run_limited(fun, timeout \\ @default_timeout), do: Limiter.run(fun, timeout)

  @doc """
  Executa várias funções simultaneamente, respeitando o limite configurado.
  Retorna a lista de resultados na mesma ordem das funções.
  """
  def run_concurrently(funs, opts \\ []) when is_list(funs) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    funs
    |> Task.async_stream(
      fn fun -> Limiter.run(fun, timeout) end,
      max_concurrency: Limiter.max(),
      ordered: true,
      timeout: :infinity
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # Callbacks GenServer --------------------------------------------------

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online() * 2)
    ttl = Keyword.get(opts, :session_ttl, @default_ttl)
    sweep = Keyword.get(opts, :sweep_interval, @default_sweep)

    :ets.new(@users, [:set, :named_table, :protected, read_concurrency: true])
    :ets.new(@sessions, [:set, :named_table, :protected, read_concurrency: true])
    :ok = Limiter.create_table(max)

    schedule_sweep(sweep)

    {:ok, %{ttl: ttl, sweep: sweep, subscribers: %{}}}
  end

  @impl true
  def handle_call({:create_user, attrs}, _from, state) do
    reply =
      with {:ok, data} <- Validator.validate_user(attrs),
           :ok <- ensure_new_user(data.email) do
        salt = :crypto.strong_rand_bytes(16)
        hash = hash_password(data.password, salt)
        :ets.insert(@users, {data.email, data.name, salt, hash})
        broadcast(state, {:user_created, data.email})
        {:ok, %{name: data.name, email: data.email}}
      end

    {:reply, reply, state}
  end

  def handle_call({:authenticate, email, password}, _from, state) do
    reply =
      with {:ok, creds} <- Validator.validate_credentials(email, password),
           {:ok, salt, hash} <- fetch_credentials(creds.email),
           :ok <- verify_password(creds.password, salt, hash) do
        token = new_token()
        expires_at = now_ms() + state.ttl
        :ets.insert(@sessions, {token, creds.email, expires_at})
        broadcast(state, {:session_created, creds.email})
        {:ok, token}
      end

    {:reply, reply, state}
  end

  def handle_call({:logout, token}, _from, state) do
    reply =
      with {:ok, token} <- Validator.validate_token(token) do
        case :ets.take(@sessions, token) do
          [{^token, email, _expires_at}] ->
            broadcast(state, {:session_revoked, email})
            :ok

          [] ->
            {:error, :invalid_session}
        end
      end

    {:reply, reply, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    ref = Process.monitor(pid)
    subs = Map.put(state.subscribers, ref, pid)
    {:reply, :ok, %{state | subscribers: subs}}
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    {removed, kept} = Enum.split_with(state.subscribers, fn {_ref, p} -> p == pid end)

    Enum.each(removed, fn {ref, _} -> Process.demonitor(ref, [:flush]) end)

    {:reply, :ok, %{state | subscribers: Map.new(kept)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
  end

  def handle_info(:sweep, state) do
    now = now_ms()

    # Remove sessões cujo expires_at já passou.
    :ets.select_delete(@sessions, [{{:_, :_, :"$1"}, [{:<, :"$1", now}], [true]}])

    schedule_sweep(state.sweep)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Helpers privados -----------------------------------------------------

  defp ensure_new_user(email) do
    if :ets.member(@users, email), do: {:error, :email_taken}, else: :ok
  end

  defp fetch_credentials(email) do
    case :ets.lookup(@users, email) do
      [{^email, _name, salt, hash}] -> {:ok, salt, hash}
      [] -> {:error, :invalid_credentials}
    end
  end

  defp fetch_user_name(email) do
    case :ets.lookup(@users, email) do
      [{^email, name, _salt, _hash}] -> {:ok, name}
      [] -> {:error, :user_not_found}
    end
  end

  defp fetch_session(token) do
    case :ets.lookup(@sessions, token) do
      [{^token, email, expires_at}] -> {:ok, email, expires_at}
      [] -> {:error, :invalid_session}
    end
  end

  defp check_expiry(expires_at) do
    if now_ms() < expires_at, do: :ok, else: {:error, :expired}
  end

  defp verify_password(password, salt, hash) do
    if :crypto.hash_equals(hash_password(password, salt), hash),
      do: :ok,
      else: {:error, :invalid_credentials}
  end

  # Derivação de chave simplificada: SHA-256 iterado com salt.
  defp hash_password(password, salt) do
    Enum.reduce(1..@hash_iterations, password, fn _, acc ->
      :crypto.hash(:sha256, [salt, acc])
    end)
  end

  defp new_token, do: :crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false)

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp broadcast(state, event) do
    Enum.each(state.subscribers, fn {_ref, pid} -> send(pid, {:usm_event, event}) end)
  end

  defp schedule_sweep(interval), do: Process.send_after(self(), :sweep, interval)
end
