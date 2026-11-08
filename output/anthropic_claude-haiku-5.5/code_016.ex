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

# Gerenciamento de usuários e sessões em Elixir (arquivo único, autocontido)
#
# Requisitos atendidos:
# 1. Validação dos dados antes de qualquer operação -> Gerenciador.Validador
# 2. Comunicação entre processos -> chamadas GenServer e pub/sub (inscrever/notificar)
# 3. Limite de operações simultâneas -> Gerenciador.Limitador (semáforo com monitores)
# 4. Execução de operações simultâneas -> Gerenciador.executar_lote/3 (Task.async_stream)
#
# Recurso ETS:
#   :gerenciador_usuarios -> id => usuário
#   :gerenciador_emails   -> email => id (índice de unicidade)
#   :gerenciador_sessoes  -> token => {token, user_id, expira_em}

defmodule Gerenciador do
  @moduledoc """
  Ponto de entrada do sistema. Inicia a árvore de supervisão e oferece
  utilitários para execução concorrente controlada.
  """

  alias Gerenciador.{Limitador, Sessoes}

  @doc """
  Inicia o supervisor com o limitador de operações e o gerenciador de sessões.

  Opções:
    * `:max_simultaneas` - número máximo de operações simultâneas (padrão: 10)
    * `:ttl` - tempo de vida das sessões em segundos (padrão: 3600)
    * `:intervalo_limpeza_ms` - intervalo de limpeza de sessões expiradas (padrão: 60_000)
  """
  def iniciar(opts \\ []) do
    children = [
      {Limitador, max_simultaneas: Keyword.get(opts, :max_simultaneas, 10)},
      {Sessoes,
       ttl: Keyword.get(opts, :ttl, 3600),
       intervalo_limpeza_ms: Keyword.get(opts, :intervalo_limpeza_ms, 60_000)}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Gerenciador.Supervisor)
  end

  @doc """
  Executa `fun` para cada item de `itens` de forma concorrente.
  Cada execução passa pelo limitador global, então o número de operações
  simultâneas nunca excede o configurado em `Limitador`.

  Opções:
    * `:concorrencia` - tarefas simultâneas neste lote (padrão: schedulers online)
    * `:timeout` - tempo máximo por item em ms (padrão: 5_000)
  """
  def executar_lote(itens, fun, opts \\ []) when is_list(itens) and is_function(fun, 1) do
    concorrencia = Keyword.get(opts, :concorrencia, System.schedulers_online())
    timeout = Keyword.get(opts, :timeout, 5_000)

    itens
    |> Task.async_stream(
      fn item -> Limitador.executar(fn -> fun.(item) end) end,
      max_concurrency: concorrencia,
      timeout: timeout,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.map(fn
      {:ok, resultado} -> resultado
      {:exit, motivo} -> {:error, {:falha, motivo}}
    end)
  end
end

defmodule Gerenciador.Validador do
  @moduledoc """
  Validações puras dos dados de entrada. Nenhuma operação do sistema
  deve ocorrer com dados que não passaram por aqui.
  """

  @doc "Valida dados para cadastro de usuário."
  def usuario(attrs) when is_map(attrs) do
    with {:ok, nome} <- texto(attrs, :nome, 2, 100),
         {:ok, email} <- email(Map.get(attrs, :email)),
         {:ok, senha} <- senha(Map.get(attrs, :senha)) do
      {:ok, %{nome: nome, email: email, senha: senha}}
    end
  end

  def usuario(_), do: {:error, :dados_invalidos}

  @doc "Valida credenciais de login."
  def login(attrs) when is_map(attrs) do
    with {:ok, email} <- email(Map.get(attrs, :email)),
         {:ok, senha} <- senha(Map.get(attrs, :senha)) do
      {:ok, %{email: email, senha: senha}}
    end
  end

  def login(_), do: {:error, :dados_invalidos}

  @doc "Valida o formato de um token de sessão."
  def token(token) when is_binary(token) and byte_size(token) in 20..200, do: {:ok, token}
  def token(_), do: {:error, {:token, :invalido}}

  # Helpers privados

  defp texto(attrs, campo, min, max) do
    case Map.get(attrs, campo) do
      valor when is_binary(valor) ->
        valor = String.trim(valor)
        tamanho = String.length(valor)

        if tamanho >= min and tamanho <= max,
          do: {:ok, valor},
          else: {:error, {campo, :tamanho_invalido}}

      _ ->
        {:error, {campo, :obrigatorio}}
    end
  end

  defp email(valor) when is_binary(valor) do
    valor = valor |> String.trim() |> String.downcase()

    if byte_size(valor) <= 254 and Regex.match?(~r/^[^@\s]+@[^@\s]+\.[^@\s]+$/, valor),
      do: {:ok, valor},
      else: {:error, {:email, :invalido}}
  end

  defp email(_), do: {:error, {:email, :obrigatorio}}

  defp senha(valor) when is_binary(valor) do
    cond do
      byte_size(valor) < 8 or byte_size(valor) > 128 ->
        {:error, {:senha, :tamanho_invalido}}

      not (valor =~ ~r/[A-Za-z]/ and valor =~ ~r/[0-9]/) ->
        {:error, {:senha, :precisa_letra_e_numero}}

      true ->
        {:ok, valor}
    end
  end

  defp senha(_), do: {:error, {:senha, :obrigatoria}}
end

defmodule Gerenciador.Limitador do
  @moduledoc """
  Semáforo que limita o número de operações executadas simultaneamente.

  Cada chamada a `executar/1` adquire uma vaga, executa a função no processo
  do chamador e libera a vaga ao final. Chamadas excedentes ficam em fila.
  Um monitor sobre o processo chamador garante que vagas de processos que
  morreram sejam recuperadas.
  """

  use GenServer

  @doc "Inicia o limitador. Opção: `:max_simultaneas`."
  def start_link(opts) do
    max = Keyword.get(opts, :max_simultaneas, 10)
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de operações simultâneas."
  def executar(fun) when is_function(fun, 0) do
    {:ok, ref} = GenServer.call(__MODULE__, :adquirir, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:liberar, ref})
    end
  end

  @doc "Retorna o estado atual do semáforo."
  def estado, do: GenServer.call(__MODULE__, :estado)

  # Callbacks GenServer

  @impl true
  def init(max) when is_integer(max) and max > 0 do
    {:ok, %{max: max, ativos: MapSet.new(), fila: :queue.new()}}
  end

  @impl true
  def handle_call(:adquirir, {pid, _} = from, s) do
    ref = Process.monitor(pid)

    if MapSet.size(s.ativos) < s.max do
      {:reply, {:ok, ref}, %{s | ativos: MapSet.put(s.ativos, ref)}}
    else
      {:noreply, %{s | fila: :queue.in({from, ref}, s.fila)}}
    end
  end

  def handle_call(:estado, _from, s) do
    info = %{maximo: s.max, ativos: MapSet.size(s.ativos), aguardando: :queue.len(s.fila)}
    {:reply, info, s}
  end

  @impl true
  def handle_cast({:liberar, ref}, s) do
    Process.demonitor(ref, [:flush])
    {:noreply, drenar(%{s | ativos: MapSet.delete(s.ativos, ref)})}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _motivo}, s) do
    if MapSet.member?(s.ativos, ref) do
      {:noreply, drenar(%{s | ativos: MapSet.delete(s.ativos, ref)})}
    else
      # Processo morreu enquanto aguardava na fila
      {:noreply, %{s | fila: :queue.filter(fn {_, r} -> r != ref end, s.fila)}}
    end
  end

  # Concede vagas livres aos próximos da fila
  defp drenar(s) do
    if MapSet.size(s.ativos) < s.max do
      case :queue.out(s.fila) do
        {{:value, {from, ref}}, resto} ->
          GenServer.reply(from, {:ok, ref})
          drenar(%{s | fila: resto, ativos: MapSet.put(s.ativos, ref)})

        {:empty, _} ->
          s
      end
    else
      s
    end
  end
end

defmodule Gerenciador.Sessoes do
  @moduledoc """
  Gerenciamento de usuários e sessões.

  - Leituras são feitas diretamente nas tabelas ETS pelo processo chamador.
  - Escritas são serializadas pelo GenServer, que garante unicidade de e-mail
    e a consistência entre as tabelas.
  - Eventos são publicados aos processos inscritos (pub/sub simples).
  """

  use GenServer

  alias Gerenciador.{Limitador, Validador}

  @tab_usuarios :gerenciador_usuarios
  @tab_emails :gerenciador_emails
  @tab_sessoes :gerenciador_sessoes

  @iteracoes_hash 10_000

  ## API pública

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um usuário após validar os dados."
  def cadastrar(attrs) do
    Limitador.executar(fn ->
      with {:ok, dados} <- Validador.usuario(attrs) do
        GenServer.call(__MODULE__, {:cadastrar, dados})
      end
    end)
  end

  @doc "Autentica um usuário e cria uma sessão. Retorna `{:ok, %{token: _, expira_em: _}}`."
  def autenticar(attrs) do
    Limitador.executar(fn ->
      with {:ok, %{email: email, senha: senha}} <- Validador.login(attrs),
           {:ok, usuario} <- buscar_por_email(email),
           :ok <- verificar_senha(usuario, senha) do
        GenServer.call(__MODULE__, {:criar_sessao, usuario.id})
      end
    end)
  end

  @doc "Valida um token e retorna o usuário associado, se a sessão ainda estiver ativa."
  def validar_sessao(token) do
    Limitador.executar(fn ->
      with {:ok, token} <- Validador.token(token) do
        case :ets.lookup(@tab_sessoes, token) do
          [{^token, user_id, expira_em}] ->
            cond do
              expira_em <= agora() ->
                {:error, :sessao_expirada}

              true ->
                case lookup_usuario(user_id) do
                  {:ok, usuario} -> {:ok, %{usuario: publico(usuario), expira_em: expira_em}}
                  _ -> {:error, :sessao_invalida}
                end
            end

          [] ->
            {:error, :sessao_invalida}
        end
      end
    end)
  end

  @doc "Encerra uma sessão pelo token."
  def encerrar_sessao(token) do
    Limitador.executar(fn ->
      with {:ok, token} <- Validador.token(token) do
        GenServer.call(__MODULE__, {:encerrar_sessao, token})
      end
    end)
  end

  @doc "Busca os dados públicos de um usuário pelo id."
  def buscar_usuario(id) when is_binary(id) do
    Limitador.executar(fn ->
      case lookup_usuario(id) do
        {:ok, usuario} -> {:ok, publico(usuario)}
        erro -> erro
      end
    end)
  end

  @doc "Inscreve um processo para receber eventos `{:gerenciador, evento}`."
  def inscrever(pid \\ self()), do: GenServer.call(__MODULE__, {:inscrever, pid})

  @doc "Cancela a inscrição de um processo."
  def cancelar_inscricao(pid \\ self()), do: GenServer.call(__MODULE__, {:cancelar, pid})

  ## Callbacks GenServer

  @impl true
  def init(opts) do
    ttl = Keyword.get(opts, :ttl, 3600)
    intervalo = Keyword.get(opts, :intervalo_limpeza_ms, 60_000)

    opcoes = [:set, :named_table, :protected, read_concurrency: true]
    :ets.new(@tab_usuarios, opcoes)
    :ets.new(@tab_emails, opcoes)
    :ets.new(@tab_sessoes, opcoes)

    Process.send_after(self(), :limpar_sessoes, intervalo)

    {:ok, %{ttl: ttl, intervalo: intervalo, inscritos: %{}}}
  end

  @impl true
  def handle_call({:cadastrar, %{email: email} = dados}, _from, s) do
    if :ets.member(@tab_emails, email) do
      {:reply, {:error, {:email, :ja_cadastrado}}, s}
    else
      id = gerar_token(16)
      salt = :crypto.strong_rand_bytes(16)

      usuario = %{
        id: id,
        nome: dados.nome,
        email: email,
        salt: salt,
        senha_hash: derivar(dados.senha, salt),
        criado_em: agora()
      }

      :ets.insert(@tab_usuarios, {id, usuario})
      :ets.insert(@tab_emails, {email, id})
      notificar(s, {:usuario_cadastrado, id})

      {:reply, {:ok, publico(usuario)}, s}
    end
  end

  def handle_call({:criar_sessao, user_id}, _from, s) do
    token = gerar_token(32)
    expira_em = agora() + s.ttl

    :ets.insert(@tab_sessoes, {token, user_id, expira_em})
    notificar(s, {:sessao_criada, user_id})

    {:reply, {:ok, %{token: token, expira_em: expira_em}}, s}
  end

  def handle_call({:encerrar_sessao, token}, _from, s) do
    case :ets.take(@tab_sessoes, token) do
      [{_, user_id, _}] ->
        notificar(s, {:sessao_encerrada, user_id})
        {:reply, :ok, s}

      [] ->
        {:reply, {:error, :sessao_invalida}, s}
    end
  end

  def handle_call({:inscrever, pid}, _from, s) do
    s =
      if Map.has_key?(s.inscritos, pid) do
        s
      else
        %{s | inscritos: Map.put(s.inscritos, pid, Process.monitor(pid))}
      end

    {:reply, :ok, s}
  end

  def handle_call({:cancelar, pid}, _from, s) do
    {ref, restante} = Map.pop(s.inscritos, pid)
    if ref, do: Process.demonitor(ref, [:flush])
    {:reply, :ok, %{s | inscritos: restante}}
  end

  @impl true
  def handle_info(:limpar_sessoes, s) do
    # Remove sessões expiradas em uma única passagem pela tabela
    :ets.select_delete(@tab_sessoes, [{{:_, :_, :"$1"}, [{:<, :"$1", agora()}], [true]}])
    Process.send_after(self(), :limpar_sessoes, s.intervalo)
    {:noreply, s}
  end

  def handle_info({:DOWN, _ref, :process, pid, _motivo}, s) do
    {:noreply, %{s | inscritos: Map.delete(s.inscritos, pid)}}
  end

  ## Helpers privados

  defp lookup_usuario(id) do
    case :ets.lookup(@tab_usuarios, id) do
      [{_, usuario}] -> {:ok, usuario}
      [] -> {:error, :nao_encontrado}
    end
  end

  # Mensagem genérica para não revelar se o e-mail existe
  defp buscar_por_email(email) do
    with [{_, id}] <- :ets.lookup(@tab_emails, email),
         {:ok, usuario} <- lookup_usuario(id) do
      {:ok, usuario}
    else
      _ -> {:error, :credenciais_invalidas}
    end
  end

  defp verificar_senha(usuario, senha) do
    # hash_equals/2 faz comparação em tempo constante (OTP 25+)
    if :crypto.hash_equals(derivar(senha, usuario.salt), usuario.senha_hash),
      do: :ok,
      else: {:error, :credenciais_invalidas}
  end

  # Derivação iterada com SHA-256 (exemplo didático).
  # Em produção, prefira um KDF dedicado como Argon2 ou PBKDF2 via biblioteca externa.
  defp derivar(senha, salt) do
    Enum.reduce(1..@iteracoes_hash, salt <> senha, fn _, acc ->
      :crypto.hash(:sha256, acc <> salt)
    end)
  end

  defp gerar_token(bytes) do
    bytes |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false)
  end

  defp publico(usuario), do: Map.take(usuario, [:id, :nome, :email])

  defp agora, do: System.system_time(:second)

  defp notificar(s, evento) do
    Enum.each(Map.keys(s.inscritos), &send(&1, {:gerenciador, evento}))
  end
end
