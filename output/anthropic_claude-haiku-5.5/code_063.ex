# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de usuários e sessões, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de usuários e sessões em Elixir.
#
# Uso (exemplo em comentário):
#   {:ok, _sup} = UsuariosSessoes.Supervisor.start_link(arquivo: "dados.dets")
#   UsuariosSessoes.assinar()
#   {:ok, u} = UsuariosSessoes.cadastrar_usuario("Ana", "ana@exemplo.com")
#   {:ok, token} = UsuariosSessoes.iniciar_sessao(u.id)
#   UsuariosSessoes.consultar_sessao(token)
#   UsuariosSessoes.atualizar_usuario(u.id, %{nome: "Ana Maria"})
#   UsuariosSessoes.encerrar_sessao(token)
#   UsuariosSessoes.remover_usuario(u.id)
#   UsuariosSessoes.historico()
#   # Notificações chegam como mensagens: {:notificacao, %{regra: _, mensagem: _, em: _}}

defmodule UsuariosSessoes.Macros do
  @moduledoc """
  Macros utilizadas pelo sistema:

    * `regra/3`     - declara uma regra de notificação (função avaliada a cada operação).
    * `transacao/3` - envolve uma operação de escrita, registrando-a no histórico,
                      disparando a avaliação de regras e persistindo o estado.
  """

  @doc """
  Declara uma regra de notificação.

  A regra recebe o estado do sistema através da variável informada e deve
  retornar uma lista de mensagens. Cada mensagem nova (que não estava ativa
  na operação anterior) gera uma notificação.

      regra(:nome_da_regra, estado, do: ...)
  """
  defmacro regra(nome, variavel, do: corpo) do
    quote do
      {unquote(nome), fn unquote(variavel) -> unquote(corpo) end}
    end
  end

  @doc """
  Executa um bloco que deve retornar `{:ok, resposta, novo_estado}` ou
  `{:error, motivo}`. Em ambos os casos a operação é registrada no histórico
  (com status `:ok` ou `:erro`) e o estado é processado pelo servidor
  (regras, notificações e persistência).
  """
  defmacro transacao(tipo, estado, do: bloco) do
    quote do
      case unquote(bloco) do
        {:ok, resposta, novo} ->
          novo = UsuariosSessoes.Servidor.pos_operacao(novo, unquote(tipo), :ok)
          {:reply, {:ok, resposta}, novo}

        {:error, _motivo} = erro ->
          st = UsuariosSessoes.Servidor.pos_operacao(unquote(estado), unquote(tipo), :erro)
          {:reply, erro, st}
      end
    end
  end
end

defmodule UsuariosSessoes.Servidor do
  @moduledoc """
  GenServer responsável por usuários, sessões, histórico e notificações.

  O estado é persistido em DETS a cada operação. Em caso de falha, o
  supervisor reinicia o processo e o estado é recarregado do arquivo.
  """

  use GenServer
  import UsuariosSessoes.Macros

  @tabela :usuarios_sessoes_dets
  @limite_sessoes 3
  @limite_usuarios 100

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def cadastrar_usuario(nome, email),
    do: GenServer.call(__MODULE__, {:cadastrar_usuario, nome, email})

  def consultar_usuario(id),
    do: GenServer.call(__MODULE__, {:consultar_usuario, id})

  def listar_usuarios,
    do: GenServer.call(__MODULE__, :listar_usuarios)

  def atualizar_usuario(id, attrs),
    do: GenServer.call(__MODULE__, {:atualizar_usuario, id, attrs})

  def remover_usuario(id),
    do: GenServer.call(__MODULE__, {:remover_usuario, id})

  def iniciar_sessao(usuario_id),
    do: GenServer.call(__MODULE__, {:iniciar_sessao, usuario_id})

  def consultar_sessao(token),
    do: GenServer.call(__MODULE__, {:consultar_sessao, token})

  def encerrar_sessao(token),
    do: GenServer.call(__MODULE__, {:encerrar_sessao, token})

  def historico,
    do: GenServer.call(__MODULE__, :historico)

  @doc "Inscreve o processo chamador para receber notificações."
  def assinar,
    do: GenServer.call(__MODULE__, {:assinar, self()})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, "usuarios_sessoes.dets")
    {:ok, _} = :dets.open_file(@tabela, file: String.to_charlist(arquivo), type: :set)

    # Recuperação: carrega o último estado persistido, se existir.
    base =
      case :dets.lookup(@tabela, :estado) do
        [{:estado, snapshot}] -> snapshot
        [] -> %{usuarios: %{}, sessoes: %{}, historico: [], proximo_id: 1,
                proximo_seq: 1, alertas_ativos: MapSet.new()}
      end

    # Assinantes não são persistidos (são processos vivos, não dados).
    {:ok, Map.put(base, :assinantes, %{})}
  end

  @impl true
  def handle_call({:cadastrar_usuario, nome, email}, _from, state) do
    transacao :cadastrar_usuario, state, do: cadastrar(state, nome, email)
  end

  def handle_call({:atualizar_usuario, id, attrs}, _from, state) do
    transacao :atualizar_usuario, state, do: atualizar(state, id, attrs)
  end

  def handle_call({:remover_usuario, id}, _from, state) do
    transacao :remover_usuario, state, do: remover(state, id)
  end

  def handle_call({:iniciar_sessao, usuario_id}, _from, state) do
    transacao :iniciar_sessao, state, do: iniciar(state, usuario_id)
  end

  def handle_call({:encerrar_sessao, token}, _from, state) do
    transacao :encerrar_sessao, state, do: encerrar(state, token)
  end

  def handle_call({:consultar_usuario, id}, _from, state) do
    resposta =
      case Map.fetch(state.usuarios, id) do
        {:ok, u} -> {:ok, u}
        :error -> {:error, :nao_encontrado}
      end

    {:reply, resposta, state}
  end

  def handle_call(:listar_usuarios, _from, state) do
    {:reply, {:ok, Map.values(state.usuarios)}, state}
  end

  def handle_call({:consultar_sessao, token}, _from, state) do
    resposta =
      case Map.fetch(state.sessoes, token) do
        {:ok, s} -> {:ok, s}
        :error -> {:error, :nao_encontrada}
      end

    {:reply, resposta, state}
  end

  def handle_call(:historico, _from, state) do
    {:reply, Enum.reverse(state.historico), state}
  end

  def handle_call({:assinar, pid}, _from, state) do
    ref = Process.monitor(pid)
    {:reply, :ok, %{state | assinantes: Map.put(state.assinantes, pid, ref)}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _motivo}, state) do
    {:noreply, %{state | assinantes: Map.delete(state.assinantes, pid)}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_motivo, _state) do
    :dets.close(@tabela)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Pós-processamento de operações (chamado pela macro `transacao`)
  # ---------------------------------------------------------------------------

  @doc false
  def pos_operacao(st, tipo, status) do
    entrada = %{seq: st.proximo_seq, operacao: tipo, status: status, em: agora()}
    st = %{st | historico: [entrada | st.historico], proximo_seq: st.proximo_seq + 1}

    atuais = avaliar_regras(st)
    novos = MapSet.difference(atuais, st.alertas_ativos)
    Enum.each(novos, fn {nome, msg} -> notificar(st, nome, msg) end)

    st = %{st | alertas_ativos: atuais}
    persistir(st)
    st
  end

  # ---------------------------------------------------------------------------
  # Regras de notificação (declaradas com a macro `regra`)
  # ---------------------------------------------------------------------------

  defp regras do
    [
      regra(:sessoes_excessivas, est, do: sessoes_excessivas(est)),
      regra(:limite_usuarios, est, do: limite_usuarios(est))
    ]
  end

  defp sessoes_excessivas(est) do
    est.sessoes
    |> Enum.group_by(fn {_token, s} -> s.usuario_id end)
    |> Enum.filter(fn {_uid, sessoes} -> length(sessoes) > @limite_sessoes end)
    |> Enum.map(fn {uid, sessoes} ->
      "usuário #{uid} possui #{length(sessoes)} sessões ativas"
    end)
  end

  defp limite_usuarios(est) do
    total = map_size(est.usuarios)
    if total >= @limite_usuarios, do: ["total de usuários atingiu #{total}"], else: []
  end

  defp avaliar_regras(st) do
    for {nome, fun} <- regras(), msg <- fun.(st), into: MapSet.new(), do: {nome, msg}
  end

  defp notificar(st, nome, msg) do
    notificacao = %{regra: nome, mensagem: msg, em: agora()}
    Enum.each(Map.keys(st.assinantes), &send(&1, {:notificacao, notificacao}))
  end

  # ---------------------------------------------------------------------------
  # Regras de negócio (funções puras: retornam {:ok, resposta, estado} | {:error, motivo})
  # ---------------------------------------------------------------------------

  defp cadastrar(st, nome, email) do
    cond do
      not valido_email?(email) ->
        {:error, :email_invalido}

      email_em_uso?(st, email, nil) ->
        {:error, :email_duplicado}

      true ->
        id = st.proximo_id
        usuario = %{id: id, nome: nome, email: email, criado_em: agora()}

        novo = %{st | usuarios: Map.put(st.usuarios, id, usuario), proximo_id: id + 1}
        {:ok, usuario, novo}
    end
  end

  defp atualizar(st, id, attrs) do
    case Map.fetch(st.usuarios, id) do
      :error ->
        {:error, :nao_encontrado}

      {:ok, usuario} ->
        mudancas = Map.take(attrs, [:nome, :email])

        cond do
          Map.has_key?(mudancas, :email) and not valido_email?(mudancas.email) ->
            {:error, :email_invalido}

          Map.has_key?(mudancas, :email) and email_em_uso?(st, mudancas.email, id) ->
            {:error, :email_duplicado}

          true ->
            atualizado =
              usuario
              |> Map.merge(mudancas)
              |> Map.put(:atualizado_em, agora())

            {:ok, atualizado, %{st | usuarios: Map.put(st.usuarios, id, atualizado)}}
        end
    end
  end

  defp remover(st, id) do
    case Map.pop(st.usuarios, id) do
      {nil, _} ->
        {:error, :nao_encontrado}

      {usuario, usuarios} ->
        # Remove também as sessões do usuário removido.
        sessoes =
          st.sessoes
          |> Enum.reject(fn {_token, s} -> s.usuario_id == id end)
          |> Map.new()

        {:ok, usuario, %{st | usuarios: usuarios, sessoes: sessoes}}
    end
  end

  defp iniciar(st, usuario_id) do
    case Map.fetch(st.usuarios, usuario_id) do
      :error ->
        {:error, :nao_encontrado}

      {:ok, _} ->
        token = gerar_token()
        sessao = %{token: token, usuario_id: usuario_id, iniciada_em: agora()}
        {:ok, %{token: token}, %{st | sessoes: Map.put(st.sessoes, token, sessao)}}
    end
  end

  defp encerrar(st, token) do
    case Map.pop(st.sessoes, token) do
      {nil, _} -> {:error, :nao_encontrada}
      {sessao, sessoes} -> {:ok, sessao, %{st | sessoes: sessoes}}
    end
  end

  # ---------------------------------------------------------------------------
  # Utilitários
  # ---------------------------------------------------------------------------

  defp persistir(st) do
    snapshot =
      Map.take(st, [:usuarios, :sessoes, :historico, :proximo_id, :proximo_seq, :alertas_ativos])

    :ok = :dets.insert(@tabela, {:estado, snapshot})
    :ok = :dets.sync(@tabela)
  end

  defp valido_email?(email) when is_binary(email),
    do: String.match?(email, ~r/^[^@\s]+@[^@\s]+$/)

  defp valido_email?(_), do: false

  defp email_em_uso?(st, email, id_ignorado) do
    Enum.any?(st.usuarios, fn {uid, u} -> uid != id_ignorado and u.email == email end)
  end

  defp gerar_token do
    :crypto.strong_rand_bytes(24) |> Base.url_encode64(padding: false)
  end

  defp agora, do: DateTime.utc_now()
end

defmodule UsuariosSessoes.Supervisor do
  @moduledoc """
  Supervisor com estratégia :one_for_one. Se o servidor falhar inesperadamente,
  é reiniciado automaticamente e recarrega seu estado do DETS.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {UsuariosSessoes.Servidor, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end
