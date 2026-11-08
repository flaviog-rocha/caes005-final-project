# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Implementar expiração automática de registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Sessoes.Gerenciador do
  @moduledoc """
  Gerenciamento de usuários e sessões em um único processo GenServer.

  Requisitos atendidos:

    1. Notificações: assinantes recebem mensagens `{:notificacao, tipo, dados}`
       quando condições são atingidas. Tipos emitidos:
         * `:limite_sessoes_atingido`   - login recusado por exceder o máximo de sessões
         * `:conta_bloqueada`           - conta bloqueada após falhas consecutivas de login
         * `:sessao_prestes_a_expirar`  - sessão entrou na janela de aviso de expiração
         * `:sessao_expirada`           - sessão expirou (por tempo ou por acesso tardio)
         * `:usuario_expirado`          - registro de usuário expirou por inatividade

    2. Validação: todos os dados de entrada são validados antes de qualquer
       alteração de estado. Erros retornam `{:error, {:validacao, [{campo, mensagem}]}}`.

    3. Expiração automática: sessões expiram por TTL deslizante (renovado a cada uso)
       e registros de usuário expiram após período de inatividade. Uma varredura
       periódica remove os registros vencidos.

  Uso básico:

      {:ok, srv} = Sessoes.Gerenciador.start_link(max_sessoes: 2, max_falhas: 3)
      :ok = Sessoes.Gerenciador.assinar(srv)
      {:ok, _} = Sessoes.Gerenciador.cadastrar_usuario(srv,
        %{nome: "Ana", email: "ana@exemplo.com", senha: "segredo123"})
      {:ok, token} = Sessoes.Gerenciador.login(srv, "ana@exemplo.com", "segredo123")
      {:ok, "ana@exemplo.com"} = Sessoes.Gerenciador.validar_sessao(srv, token)
      :ok = Sessoes.Gerenciador.logout(srv, token)
  """

  use GenServer

  # Valores padrão (sobrescrevíveis via opções de start_link/1)
  @ttl_sessao_padrao :timer.minutes(30)
  @ttl_usuario_padrao :timer.hours(24 * 30)
  @max_sessoes_padrao 3
  @max_falhas_padrao 5
  @bloqueio_padrao :timer.minutes(15)
  @aviso_expiracao_padrao :timer.minutes(5)
  @intervalo_varredura_padrao :timer.seconds(10)

  ## API pública

  @doc "Inicia o gerenciador. Aceita as opções de configuração e `:name`."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @doc "Cadastra um usuário a partir de um mapa com `:nome`, `:email` e `:senha`."
  def cadastrar_usuario(srv, attrs), do: GenServer.call(srv, {:cadastrar_usuario, attrs})

  @doc "Autentica e retorna um token de sessão."
  def login(srv, email, senha), do: GenServer.call(srv, {:login, email, senha})

  @doc "Encerra uma sessão pelo token."
  def logout(srv, token), do: GenServer.call(srv, {:logout, token})

  @doc "Valida um token e renova sua expiração. Retorna o email do dono da sessão."
  def validar_sessao(srv, token), do: GenServer.call(srv, {:validar_sessao, token})

  @doc "Remove um usuário e todas as suas sessões."
  def remover_usuario(srv, email), do: GenServer.call(srv, {:remover_usuario, email})

  @doc "Assina as notificações do gerenciador para o pid informado."
  def assinar(srv, pid \\ self()), do: GenServer.call(srv, {:assinar, pid})

  @doc "Cancela a assinatura de notificações do pid informado."
  def cancelar_assinatura(srv, pid \\ self()),
    do: GenServer.call(srv, {:cancelar_assinatura, pid})

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    config = %{
      ttl_sessao: Keyword.get(opts, :ttl_sessao, @ttl_sessao_padrao),
      ttl_usuario: Keyword.get(opts, :ttl_usuario, @ttl_usuario_padrao),
      max_sessoes: Keyword.get(opts, :max_sessoes, @max_sessoes_padrao),
      max_falhas: Keyword.get(opts, :max_falhas, @max_falhas_padrao),
      bloqueio: Keyword.get(opts, :bloqueio, @bloqueio_padrao),
      aviso_expiracao: Keyword.get(opts, :aviso_expiracao, @aviso_expiracao_padrao),
      intervalo_varredura: Keyword.get(opts, :intervalo_varredura, @intervalo_varredura_padrao)
    }

    agendar_varredura(config.intervalo_varredura)

    # assinantes: %{pid => referencia_do_monitor}
    {:ok, %{config: config, usuarios: %{}, sessoes: %{}, assinantes: %{}}}
  end

  @impl true
  def handle_call({:cadastrar_usuario, attrs}, _from, estado) do
    # Validação completa antes de qualquer alteração de estado
    with {:ok, dados} <- validar_cadastro(attrs),
         :ok <- verificar_email_livre(estado, dados.email) do
      usuario = novo_usuario(dados, agora(), estado.config.ttl_usuario)
      {:reply, {:ok, dados.email}, put_usuario(estado, usuario)}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:login, email, senha}, _from, estado) do
    agora = agora()

    with {:ok, email} <- validar_credenciais(email, senha),
         {:ok, usuario} <- buscar_usuario(estado, email) do
      autenticar(usuario, senha, agora, estado)
    else
      # Não revela se o email existe: ambos os casos retornam o mesmo erro
      {:error, :nao_encontrado} -> {:reply, {:error, :credenciais_invalidas}, estado}
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:logout, token}, _from, estado) do
    with :ok <- validar_token(token),
         {:ok, _sessao} <- buscar_sessao(estado, token) do
      {:reply, :ok, remover_sessao(estado, token)}
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:validar_sessao, token}, _from, estado) do
    agora = agora()

    with :ok <- validar_token(token),
         {:ok, sessao} <- buscar_sessao(estado, token) do
      if sessao.expira_em <= agora do
        # Expiração detectada no acesso: remove e notifica
        estado =
          estado
          |> remover_sessao(token)
          |> notificar(:sessao_expirada, %{email: sessao.email})

        {:reply, {:error, :sessao_expirada}, estado}
      else
        # TTL deslizante: cada uso renova a sessão e o registro do usuário
        renovada = %{
          sessao
          | expira_em: agora + estado.config.ttl_sessao,
            aviso_enviado: false
        }

        estado =
          estado
          |> put_sessao(renovada)
          |> renovar_usuario(sessao.email, agora)

        {:reply, {:ok, sessao.email}, estado}
      end
    else
      erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:remover_usuario, email}, _from, estado) do
    case validar_email(email) do
      {:ok, email} ->
        if Map.has_key?(estado.usuarios, email) do
          {:reply, :ok, remover_usuario_interno(estado, email)}
        else
          {:reply, {:error, :nao_encontrado}, estado}
        end

      erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call({:assinar, pid}, _from, estado) when is_pid(pid) do
    if Map.has_key?(estado.assinantes, pid) do
      {:reply, :ok, estado}
    else
      # Monitora o assinante para removê-lo automaticamente se morrer
      ref = Process.monitor(pid)
      {:reply, :ok, %{estado | assinantes: Map.put(estado.assinantes, pid, ref)}}
    end
  end

  def handle_call({:assinar, _}, _from, estado) do
    {:reply, {:error, {:validacao, [{:pid, "deve ser um pid"}]}}, estado}
  end

  def handle_call({:cancelar_assinatura, pid}, _from, estado) do
    case Map.pop(estado.assinantes, pid) do
      {nil, _} ->
        {:reply, :ok, estado}

      {ref, restantes} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, %{estado | assinantes: restantes}}
    end
  end

  @impl true
  def handle_info(:varrer, estado) do
    agora = agora()

    estado =
      estado
      |> varrer_sessoes(agora)
      |> varrer_usuarios(agora)

    agendar_varredura(estado.config.intervalo_varredura)
    {:noreply, estado}
  end

  def handle_info({:DOWN, _ref, :process, pid, _motivo}, estado) do
    {:noreply, %{estado | assinantes: Map.delete(estado.assinantes, pid)}}
  end

  def handle_info(_msg, estado), do: {:noreply, estado}

  ## Autenticação e regras de negócio

  # Conta ainda bloqueada: recusa sem verificar a senha
  defp autenticar(%{bloqueado_ate: ate}, _senha, agora, estado)
       when is_integer(ate) and ate > agora do
    {:reply, {:error, :bloqueado}, estado}
  end

  defp autenticar(usuario, senha, agora, estado) do
    # Comparação em tempo constante para evitar ataques de temporização
    if :crypto.hash_equals(usuario.senha_hash, hash_senha(senha, usuario.salt)) do
      abrir_sessao(usuario, agora, estado)
    else
      registrar_falha(usuario, agora, estado)
    end
  end

  defp abrir_sessao(usuario, agora, estado) do
    %{config: c} = estado
    ativas = sessoes_ativas(estado, usuario.email, agora)

    if length(ativas) >= c.max_sessoes do
      estado =
        notificar(estado, :limite_sessoes_atingido, %{
          email: usuario.email,
          limite: c.max_sessoes
        })

      {:reply, {:error, :limite_sessoes_atingido}, estado}
    else
      token = gerar_token()

      sessao = %{
        token: token,
        email: usuario.email,
        criada_em: agora,
        expira_em: agora + c.ttl_sessao,
        aviso_enviado: false
      }

      usuario = %{usuario | falhas: 0, bloqueado_ate: nil, expira_em: agora + c.ttl_usuario}

      estado =
        estado
        |> put_usuario(usuario)
        |> put_sessao(sessao)

      {:reply, {:ok, token}, estado}
    end
  end

  defp registrar_falha(usuario, agora, estado) do
    %{config: c} = estado
    falhas = usuario.falhas + 1

    if falhas >= c.max_falhas do
      bloqueado_ate = agora + c.bloqueio
      usuario = %{usuario | falhas: 0, bloqueado_ate: bloqueado_ate}

      estado =
        estado
        |> put_usuario(usuario)
        |> notificar(:conta_bloqueada, %{email: usuario.email, bloqueio_ms: c.bloqueio})

      {:reply, {:error, :credenciais_invalidas}, estado}
    else
      {:reply, {:error, :credenciais_invalidas}, put_usuario(estado, %{usuario | falhas: falhas})}
    end
  end

  ## Varredura de expiração

  defp varrer_sessoes(estado, agora) do
    %{config: c} = estado

    Enum.reduce(estado.sessoes, estado, fn {token, sessao}, acc ->
      restante = sessao.expira_em - agora

      cond do
        restante <= 0 ->
          acc
          |> remover_sessao(token)
          |> notificar(:sessao_expirada, %{email: sessao.email})

        not sessao.aviso_enviado and restante <= c.aviso_expiracao ->
          acc
          |> put_sessao(%{sessao | aviso_enviado: true})
          |> notificar(:sessao_prestes_a_expirar, %{
            email: sessao.email,
            restante_ms: restante
          })

        true ->
          acc
      end
    end)
  end

  defp varrer_usuarios(estado, agora) do
    expirados = for {email, u} <- estado.usuarios, u.expira_em <= agora, do: email

    Enum.reduce(expirados, estado, fn email, acc ->
      acc
      |> remover_usuario_interno(email)
      |> notificar(:usuario_expirado, %{email: email})
    end)
  end

  defp agendar_varredura(intervalo), do: Process.send_after(self(), :varrer, intervalo)

  ## Validação (funções puras)

  # Cada checagem é {campo, válido?, mensagem}. Retorna :ok ou o erro agregado.
  defp avaliar(checagens) do
    case for({campo, false, msg} <- checagens, do: {campo, msg}) do
      [] -> :ok
      erros -> {:error, {:validacao, erros}}
    end
  end

  defp validar_cadastro(attrs) when is_map(attrs) do
    nome = Map.get(attrs, :nome)
    email = Map.get(attrs, :email)
    senha = Map.get(attrs, :senha)

    checagens = [
      {:nome, is_binary(nome) and String.trim(nome) != "" and String.length(nome) <= 100,
       "obrigatório, até 100 caracteres"},
      {:email, email_valido?(email), "formato inválido"},
      {:senha, is_binary(senha) and String.length(senha) in 8..128,
       "deve ter entre 8 e 128 caracteres"}
    ]

    case avaliar(checagens) do
      :ok ->
        {:ok,
         %{
           nome: String.trim(nome),
           email: normalizar_email(email),
           senha: senha
         }}

      erro ->
        erro
    end
  end

  defp validar_cadastro(_), do: {:error, {:validacao, [{:attrs, "deve ser um mapa"}]}}

  defp validar_credenciais(email, senha) do
    checagens = [
      {:email, email_valido?(email), "formato inválido"},
      {:senha, is_binary(senha) and senha != "" and byte_size(senha) <= 128,
       "obrigatória, até 128 caracteres"}
    ]

    with :ok <- avaliar(checagens), do: {:ok, normalizar_email(email)}
  end

  defp validar_email(email) do
    if email_valido?(email),
      do: {:ok, normalizar_email(email)},
      else: {:error, {:validacao, [{:email, "formato inválido"}]}}
  end

  defp validar_token(token) do
    if is_binary(token) and byte_size(token) in 1..512,
      do: :ok,
      else: {:error, {:validacao, [{:token, "inválido"}]}}
  end

  defp email_valido?(email) do
    is_binary(email) and byte_size(email) <= 255 and
      Regex.match?(~r/^[^\s@]+@[^\s@]+\.[^\s]+$/, String.trim(email))
  end

  defp normalizar_email(email), do: email |> String.trim() |> String.downcase()

  ## Estado e acesso

  defp agora, do: System.monotonic_time(:millisecond)

  defp novo_usuario(dados, agora, ttl) do
    salt = gerar_salt()

    %{
      nome: dados.nome,
      email: dados.email,
      salt: salt,
      senha_hash: hash_senha(dados.senha, salt),
      falhas: 0,
      bloqueado_ate: nil,
      expira_em: agora + ttl
    }
  end

  defp verificar_email_livre(estado, email) do
    if Map.has_key?(estado.usuarios, email),
      do: {:error, {:validacao, [{:email, "já cadastrado"}]}},
      else: :ok
  end

  defp buscar_usuario(estado, email) do
    case Map.fetch(estado.usuarios, email) do
      {:ok, u} -> {:ok, u}
      :error -> {:error, :nao_encontrado}
    end
  end

  defp buscar_sessao(estado, token) do
    case Map.fetch(estado.sessoes, token) do
      {:ok, s} -> {:ok, s}
      :error -> {:error, :sessao_invalida}
    end
  end

  defp sessoes_ativas(estado, email, agora) do
    for {_token, s} <- estado.sessoes, s.email == email, s.expira_em > agora, do: s
  end

  defp put_usuario(estado, usuario) do
    %{estado | usuarios: Map.put(estado.usuarios, usuario.email, usuario)}
  end

  defp put_sessao(estado, sessao) do
    %{estado | sessoes: Map.put(estado.sessoes, sessao.token, sessao)}
  end

  defp remover_sessao(estado, token) do
    %{estado | sessoes: Map.delete(estado.sessoes, token)}
  end

  defp renovar_usuario(estado, email, agora) do
    case Map.fetch(estado.usuarios, email) do
      {:ok, u} -> put_usuario(estado, %{u | expira_em: agora + estado.config.ttl_usuario})
      :error -> estado
    end
  end

  defp remover_usuario_interno(estado, email) do
    sessoes = for {token, s} <- estado.sessoes, s.email != email, into: %{}, do: {token, s}
    %{estado | usuarios: Map.delete(estado.usuarios, email), sessoes: sessoes}
  end

  ## Notificações

  defp notificar(estado, tipo, dados) do
    mensagem = {:notificacao, tipo, dados}
    Enum.each(Map.keys(estado.assinantes), &send(&1, mensagem))
    estado
  end

  ## Criptografia (apenas OTP/crypto)

  # Observação: SHA-256 com salt é adequado para demonstração. Em produção,
  # prefira uma KDF lenta e dedicada (ex.: PBKDF2/Argon2 via dependência externa).
  defp gerar_salt, do: Base.encode64(:crypto.strong_rand_bytes(16))

  defp hash_senha(senha, salt), do: Base.encode64(:crypto.hash(:sha256, salt <> senha))

  defp gerar_token, do: Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
end
