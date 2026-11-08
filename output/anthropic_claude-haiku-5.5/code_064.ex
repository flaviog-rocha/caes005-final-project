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

# Arquivo único: gerenciador de usuários e sessões em Elixir (somente biblioteca padrão).
# Requisitos atendidos:
#   1. Recuperação após falhas: Supervisor one_for_one + estado persistido em DETS.
#   2. Notificações: regras declaradas com macro, disparadas em transição (falso -> verdadeiro).
#   3. CRUD de usuários e abertura/encerramento de sessões.
#   4. Histórico de operações: cada operação definida com `defoperacao` é registrada.
#   5. Macros: `defoperacao` (operações com histórico) e `defregra` (regras de notificação).
#   Processo principal: GenServer.

# ---------------------------------------------------------------------------
# Macros de operação: geram funções puras `nome(estado, args...)` que retornam
# `{resposta, novo_estado}` e acrescentam automaticamente a entrada no histórico.
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.Macros do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import GerenciadorSessoes.Macros, only: [defoperacao: 3]
    end
  end

  @doc """
  Define uma operação pura. O corpo deve retornar `{resposta, novo_estado}`.
  A variável `estado` está disponível no corpo.
  """
  defmacro defoperacao(nome, args, do: corpo) do
    quote do
      def unquote(nome)(var!(estado), unquote_splicing(args)) do
        {resposta, novo_estado} = unquote(corpo)

        entrada = %{
          operacao: unquote(nome),
          argumentos: [unquote_splicing(args)],
          resposta: resposta,
          momento: DateTime.utc_now()
        }

        {resposta, GerenciadorSessoes.Macros.registrar(novo_estado, entrada)}
      end
    end
  end

  # Acrescenta uma entrada ao histórico (mais recente primeiro).
  def registrar(estado, entrada) do
    %{estado | historico: [entrada | estado.historico]}
  end
end

# ---------------------------------------------------------------------------
# Macros de regras: declaram condições de notificação como funções puras.
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.Regras do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import GerenciadorSessoes.Regras, only: [defregra: 3]
      Module.register_attribute(__MODULE__, :regras_nomes, accumulate: true)
      @before_compile GerenciadorSessoes.Regras
    end
  end

  @doc """
  Declara uma regra. `condicao` é avaliada sobre o `estado` e deve retornar booleano.
  """
  defmacro defregra(nome, mensagem, do: condicao) do
    quote do
      @regras_nomes unquote(nome)

      def avaliar_regra(unquote(nome), var!(estado)), do: unquote(condicao)
      def mensagem_regra(unquote(nome)), do: unquote(mensagem)
    end
  end

  # Gera a função que lista todas as regras declaradas no módulo.
  defmacro __before_compile__(env) do
    nomes = env.module |> Module.get_attribute(:regras_nomes) |> Enum.reverse()

    quote do
      def nomes_regras, do: unquote(nomes)
    end
  end
end

# ---------------------------------------------------------------------------
# Regras de negócio que geram notificações.
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.RegrasPadrao do
  @moduledoc false
  use GerenciadorSessoes.Regras

  defregra :limite_sessoes_atingido, "Número de sessões ativas atingiu o limite (5)", do: map_size(estado.sessoes) >= 5

  defregra :muitos_usuarios, "Quantidade de usuários cadastrados atingiu 100", do: map_size(estado.usuarios) >= 100
end

# ---------------------------------------------------------------------------
# Domínio: operações sobre o estado (funções puras, sem processos).
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.Dominio do
  @moduledoc false
  use GerenciadorSessoes.Macros

  def estado_inicial do
    %{
      usuarios: %{},
      sessoes: %{},
      historico: [],
      notificacoes: [],
      regras_ativas: MapSet.new(),
      assinantes: [],
      proximo_id: 1
    }
  end

  defoperacao :cadastrar_usuario, [nome, email] do
    if Enum.any?(estado.usuarios, fn {_id, u} -> u.email == email end) do
      {{:erro, :email_duplicado}, estado}
    else
      id = estado.proximo_id
      usuario = %{nome: nome, email: email}
      novo = %{estado | usuarios: Map.put(estado.usuarios, id, usuario), proximo_id: id + 1}
      {{:ok, id}, novo}
    end
  end

  defoperacao :consultar_usuario, [id] do
    case Map.fetch(estado.usuarios, id) do
      {:ok, usuario} -> {{:ok, usuario}, estado}
      :error -> {{:erro, :nao_encontrado}, estado}
    end
  end

  defoperacao :atualizar_usuario, [id, attrs] do
    case Map.fetch(estado.usuarios, id) do
      :error ->
        {{:erro, :nao_encontrado}, estado}

      {:ok, atual} ->
        email_em_uso? =
          case Map.get(attrs, :email) do
            nil -> false
            email -> Enum.any?(estado.usuarios, fn {uid, u} -> uid != id and u.email == email end)
          end

        if email_em_uso? do
          {{:erro, :email_duplicado}, estado}
        else
          atualizado = Map.merge(atual, Map.take(attrs, [:nome, :email]))
          novo = %{estado | usuarios: Map.put(estado.usuarios, id, atualizado)}
          {{:ok, atualizado}, novo}
        end
    end
  end

  defoperacao :remover_usuario, [id] do
    if Map.has_key?(estado.usuarios, id) do
      # Remove também as sessões do usuário removido.
      sessoes = for {token, s} <- estado.sessoes, s.usuario_id != id, into: %{}, do: {token, s}
      novo = %{estado | usuarios: Map.delete(estado.usuarios, id), sessoes: sessoes}
      {{:ok, :removido}, novo}
    else
      {{:erro, :nao_encontrado}, estado}
    end
  end

  defoperacao :abrir_sessao, [usuario_id] do
    if Map.has_key?(estado.usuarios, usuario_id) do
      id = estado.proximo_id
      token = "sessao-#{id}"
      sessao = %{usuario_id: usuario_id, inicio: DateTime.utc_now()}

      novo = %{
        estado
        | sessoes: Map.put(estado.sessoes, token, sessao),
          proximo_id: id + 1
      }

      {{:ok, token}, novo}
    else
      {{:erro, :nao_encontrado}, estado}
    end
  end

  defoperacao :encerrar_sessao, [token] do
    case Map.pop(estado.sessoes, token) do
      {nil, _} -> {{:erro, :nao_encontrada}, estado}
      {_sessao, restantes} -> {{:ok, :encerrada}, %{estado | sessoes: restantes}}
    end
  end

  defoperacao :assinar, [pid] do
    {{:ok, :assinado}, %{estado | assinantes: [pid | estado.assinantes]}}
  end

  defoperacao :historico, [] do
    {Enum.reverse(estado.historico), estado}
  end

  defoperacao :notificacoes, [] do
    {Enum.reverse(estado.notificacoes), estado}
  end
end

# ---------------------------------------------------------------------------
# GenServer: mantém o estado, persiste em DETS após cada operação, avalia
# regras e envia notificações.
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.Servidor do
  @moduledoc false
  use GenServer

  alias GerenciadorSessoes.{Dominio, RegrasPadrao}

  @tabela :gerenciador_sessoes_estado

  ## API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def cadastrar_usuario(nome, email), do: executar(:cadastrar_usuario, [nome, email])
  def consultar_usuario(id), do: executar(:consultar_usuario, [id])
  def atualizar_usuario(id, attrs), do: executar(:atualizar_usuario, [id, attrs])
  def remover_usuario(id), do: executar(:remover_usuario, [id])
  def abrir_sessao(usuario_id), do: executar(:abrir_sessao, [usuario_id])
  def encerrar_sessao(token), do: executar(:encerrar_sessao, [token])
  def historico, do: executar(:historico, [])
  def notificacoes, do: executar(:notificacoes, [])
  def assinar(pid \\ self()), do: executar(:assinar, [pid])

  # Provoca uma falha intencional para testar a recuperação pelo Supervisor.
  def simular_falha, do: GenServer.cast(__MODULE__, :falha)

  defp executar(op, args), do: GenServer.call(__MODULE__, {:executar, op, args})

  ## Callbacks do GenServer

  @impl true
  def init(opts) do
    arquivo = Keyword.get(opts, :arquivo, "gerenciador_sessoes.dets")

    # DETS sobrevive à morte do processo: o estado é recuperado no reinício.
    {:ok, @tabela} = :dets.open_file(@tabela, file: String.to_charlist(arquivo), type: :set)

    estado =
      case :dets.lookup(@tabela, :estado) do
        [{:estado, salvo}] -> salvo
        [] -> Dominio.estado_inicial()
      end

    {:ok, estado}
  end

  @impl true
  def handle_call({:executar, op, args}, _from, estado) do
    {resposta, novo} = apply(Dominio, op, [estado | args])
    novo = avaliar_regras(novo)
    :ok = persistir(novo)
    {:reply, resposta, novo}
  end

  @impl true
  def handle_cast(:falha, _estado) do
    raise "falha simulada para teste de recuperação"
  end

  @impl true
  def terminate(_motivo, _estado) do
    :dets.close(@tabela)
    :ok
  end

  ## Funções privadas

  # Dispara notificação somente quando a regra passa de falsa para verdadeira.
  defp avaliar_regras(estado) do
    Enum.reduce(RegrasPadrao.nomes_regras(), estado, fn nome, acc ->
      ativa = RegrasPadrao.avaliar_regra(nome, acc)
      ja_notificada = MapSet.member?(acc.regras_ativas, nome)

      cond do
        ativa and not ja_notificada ->
          notificar(acc, nome)

        not ativa and ja_notificada ->
          %{acc | regras_ativas: MapSet.delete(acc.regras_ativas, nome)}

        true ->
          acc
      end
    end)
  end

  defp notificar(estado, nome) do
    mensagem = RegrasPadrao.mensagem_regra(nome)

    for pid <- estado.assinantes do
      send(pid, {:notificacao, nome, mensagem})
    end

    registro = %{regra: nome, mensagem: mensagem, momento: DateTime.utc_now()}

    %{
      estado
      | regras_ativas: MapSet.put(estado.regras_ativas, nome),
        notificacoes: [registro | estado.notificacoes]
    }
  end

  defp persistir(estado) do
    true = :dets.insert(@tabela, {:estado, estado})
    :ok = :dets.sync(@tabela)
  end
end

# ---------------------------------------------------------------------------
# Supervisor: reinicia o GenServer automaticamente após falhas inesperadas.
# ---------------------------------------------------------------------------
defmodule GerenciadorSessoes.Raiz do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {GerenciadorSessoes.Servidor, opts}
    ]

    # one_for_one: somente o processo que falhou é reiniciado.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end
