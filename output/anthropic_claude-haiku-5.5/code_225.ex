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

defmodule Plataforma do
  # Ponto de entrada da aplicação: supervisão, API pública e notificações.
  # Cada usuário e cada sessão é um processo independente, registrado no Registry.
  # Isso permite concorrência entre entidades e serialização das operações
  # dentro de cada entidade (um processo processa uma mensagem por vez).

  @registro Plataforma.Registro
  @notificacoes Plataforma.Notificacoes
  @sup Plataforma.Entidades

  ## Supervisão

  def start_link(_opts \\ []) do
    filhos = [
      # Registry de entidades (chave única por usuário/sessão)
      {Registry, keys: :unique, name: @registro},
      # Registry de assinantes de notificações (chave duplicada, vários ouvintes)
      {Registry, keys: :duplicate, name: @notificacoes},
      # Supervisor dinâmico que cria os processos de usuários e sessões sob demanda
      {DynamicSupervisor, strategy: :one_for_one, name: @sup}
    ]

    Supervisor.start_link(filhos, strategy: :one_for_one, name: __MODULE__)
  end

  ## API de usuários

  # Cadastra: valida tudo antes de criar o processo (tudo ou nada).
  def cadastrar_usuario(attrs) do
    with {:ok, dados} <- validar_usuario(attrs) do
      id = gerar_id()

      case DynamicSupervisor.start_child(@sup, {Plataforma.Usuario, Map.put(dados, :id, id)}) do
        {:ok, _pid} ->
          notificar({:usuario_cadastrado, id})
          {:ok, id}

        {:error, motivo} ->
          {:error, motivo}
      end
    end
  end

  def consultar_usuario(id), do: chamar({:usuario, id}, :obter)

  def atualizar_usuario(id, mudancas) when is_map(mudancas),
    do: chamar({:usuario, id}, {:atualizar, mudancas})

  def remover_usuario(id), do: chamar({:usuario, id}, :remover)

  ## API de sessões

  def abrir_sessao(usuario_id, attrs) when is_map(attrs),
    do: chamar({:usuario, usuario_id}, {:abrir_sessao, attrs})

  def consultar_sessao(id), do: chamar({:sessao, id}, :obter)

  def atualizar_sessao(id, mudancas) when is_map(mudancas),
    do: chamar({:sessao, id}, {:atualizar, mudancas})

  def listar_sessoes(usuario_id), do: chamar({:usuario, usuario_id}, :listar_sessoes)

  # Encerrar uma sessão é responsabilidade do usuário dono (mantém a contagem consistente).
  def encerrar_sessao(id) do
    with {:ok, %{usuario_id: uid}} <- consultar_sessao(id) do
      chamar({:usuario, uid}, {:encerrar_sessao, id})
    end
  end

  ## Notificações

  # Registra o processo chamador para receber {:plataforma, evento}.
  def assinar, do: Registry.register(@notificacoes, :eventos, nil)

  @doc false
  def notificar(evento) do
    Registry.dispatch(@notificacoes, :eventos, fn entradas ->
      for {pid, _} <- entradas, do: send(pid, {:plataforma, evento})
    end)

    :ok
  end

  ## Auxiliares internos (usados pelos submódulos)

  @doc false
  def via(chave), do: {:via, Registry, {@registro, chave}}

  @doc false
  def pid_de(chave) do
    case Registry.lookup(@registro, chave) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc false
  def encerrar_filho(pid), do: DynamicSupervisor.terminate_child(@sup, pid)

  @doc false
  def gerar_id, do: System.unique_integer([:positive, :monotonic])

  @doc false
  def validar_usuario(attrs) when is_map(attrs) do
    nome = Map.get(attrs, :nome)
    email = Map.get(attrs, :email)

    cond do
      not (is_binary(nome) and String.trim(nome) != "") ->
        {:error, :nome_invalido}

      not (is_binary(email) and Regex.match?(~r/^[^@\s]+@[^@\s]+$/, email)) ->
        {:error, :email_invalido}

      true ->
        {:ok, %{nome: nome, email: email}}
    end
  end

  def validar_usuario(_), do: {:error, :dados_invalidos}

  @doc false
  def validar_nova_sessao(attrs) when is_map(attrs) do
    dispositivo = Map.get(attrs, :dispositivo)
    ttl = Map.get(attrs, :ttl_ms)

    cond do
      not dispositivo_valido?(dispositivo) -> {:error, :dispositivo_invalido}
      not (is_integer(ttl) and ttl > 0) -> {:error, :ttl_invalido}
      true -> {:ok, %{dispositivo: dispositivo, ttl_ms: ttl}}
    end
  end

  def validar_nova_sessao(_), do: {:error, :dados_invalidos}

  @doc false
  def dispositivo_valido?(d), do: is_binary(d) and String.trim(d) != ""

  # Envia chamada síncrona à entidade; se ela não existir, retorna erro em vez de sair.
  defp chamar(chave, mensagem) do
    GenServer.call(via(chave), mensagem)
  catch
    :exit, _ -> {:error, :nao_encontrado}
  end
end

defmodule Plataforma.Usuario do
  # Processo de um usuário. Serializa todas as operações sobre o próprio usuário
  # e controla o limite de sessões ativas de forma atômica.

  @moduledoc false
  use GenServer, restart: :temporary

  @sup Plataforma.Entidades
  @max_sessoes 3
  @campos [:nome, :email]

  def start_link(%{id: id} = dados) do
    GenServer.start_link(__MODULE__, dados, name: Plataforma.via({:usuario, id}))
  end

  @impl true
  def init(dados), do: {:ok, Map.put(dados, :sessoes, MapSet.new())}

  @impl true
  def handle_call(:obter, _from, estado) do
    {:reply, {:ok, publico(estado)}, estado}
  end

  # Atualização tudo-ou-nada: o candidato é validado por completo antes de substituir o estado.
  def handle_call({:atualizar, mudancas}, _from, estado) do
    candidato = Map.merge(estado, Map.take(mudancas, @campos))

    case Plataforma.validar_usuario(candidato) do
      {:ok, validado} ->
        novo = Map.merge(estado, validado)

        if novo.email != estado.email do
          Plataforma.notificar({:email_alterado, estado.id})
        end

        {:reply, {:ok, publico(novo)}, novo}

      {:error, _} = erro ->
        {:reply, erro, estado}
    end
  end

  # Remoção: encerra as sessões do usuário e então o próprio processo.
  def handle_call(:remover, _from, estado) do
    Enum.each(estado.sessoes, fn sid ->
      case Plataforma.pid_de({:sessao, sid}) do
        {:ok, pid} -> Plataforma.encerrar_filho(pid)
        :error -> :ok
      end
    end)

    Plataforma.notificar({:usuario_removido, estado.id})
    {:stop, :normal, :ok, estado}
  end

  # Abertura de sessão: valida a entrada, verifica o limite e só então cria o processo.
  # Qualquer falha antes da criação deixa o estado intacto.
  def handle_call({:abrir_sessao, attrs}, _from, estado) do
    with {:ok, dados} <- Plataforma.validar_nova_sessao(attrs),
         :ok <- verificar_limite(estado),
         sid = Plataforma.gerar_id(),
         {:ok, _pid} <-
           DynamicSupervisor.start_child(
             @sup,
             {Plataforma.Sessao, Map.merge(dados, %{id: sid, usuario_id: estado.id})}
           ) do
      Plataforma.notificar({:sessao_aberta, estado.id, sid})
      {:reply, {:ok, sid}, %{estado | sessoes: MapSet.put(estado.sessoes, sid)}}
    else
      {:error, _} = erro -> {:reply, erro, estado}
    end
  end

  def handle_call({:encerrar_sessao, sid}, _from, estado) do
    if MapSet.member?(estado.sessoes, sid) do
      case Plataforma.pid_de({:sessao, sid}) do
        {:ok, pid} -> Plataforma.encerrar_filho(pid)
        :error -> :ok
      end

      Plataforma.notificar({:sessao_encerrada, estado.id, sid})
      {:reply, :ok, %{estado | sessoes: MapSet.delete(estado.sessoes, sid)}}
    else
      {:reply, {:error, :nao_encontrado}, estado}
    end
  end

  def handle_call(:listar_sessoes, _from, estado) do
    {:reply, {:ok, MapSet.to_list(estado.sessoes)}, estado}
  end

  # Mensagem assíncrona enviada pela própria sessão ao expirar.
  @impl true
  def handle_cast({:sessao_expirada, sid}, estado) do
    {:noreply, %{estado | sessoes: MapSet.delete(estado.sessoes, sid)}}
  end

  defp verificar_limite(estado) do
    ativas = Enum.count(estado.sessoes, fn sid -> match?({:ok, _}, Plataforma.pid_de({:sessao, sid})) end)

    if ativas >= @max_sessoes do
      Plataforma.notificar({:limite_sessoes_atingido, estado.id})
      {:error, :limite_sessoes_atingido}
    else
      :ok
    end
  end

  defp publico(estado), do: Map.take(estado, [:id, :nome, :email])
end

defmodule Plataforma.Sessao do
  # Processo de uma sessão. Opera de forma independente das demais sessões.
  # Expira sozinha após o TTL e notifica o usuário dono de forma assíncrona.

  @moduledoc false
  use GenServer, restart: :temporary

  def start_link(%{id: id} = dados) do
    GenServer.start_link(__MODULE__, dados, name: Plataforma.via({:sessao, id}))
  end

  @impl true
  def init(%{id: id, usuario_id: uid, dispositivo: disp, ttl_ms: ttl}) do
    Process.send_after(self(), :expirar, ttl)

    estado = %{
      id: id,
      usuario_id: uid,
      dispositivo: disp,
      token: :rand.bytes(24) |> Base.url_encode64(padding: false),
      criada_em: System.system_time(:second)
    }

    {:ok, estado}
  end

  @impl true
  def handle_call(:obter, _from, estado) do
    {:reply, {:ok, publico(estado)}, estado}
  end

  # Atualização tudo-ou-nada: só substitui o estado se o resultado for válido.
  def handle_call({:atualizar, mudancas}, _from, estado) do
    candidato = Map.merge(estado, Map.take(mudancas, [:dispositivo]))

    if Plataforma.dispositivo_valido?(candidato.dispositivo) do
      {:reply, {:ok, publico(candidato)}, candidato}
    else
      {:reply, {:error, :dispositivo_invalido}, estado}
    end
  end

  @impl true
  def handle_info(:expirar, estado) do
    Plataforma.notificar({:sessao_expirada, estado.id, estado.usuario_id})
    GenServer.cast(Plataforma.via({:usuario, estado.usuario_id}), {:sessao_expirada, estado.id})
    {:stop, :normal, estado}
  end

  defp publico(estado), do: Map.take(estado, [:id, :usuario_id, :dispositivo, :token, :criada_em])
end
