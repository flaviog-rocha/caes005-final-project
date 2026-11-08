# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Implementar expiração automática de registros.
# 3. Recuperar processos após falhas inesperadas.
# 4. Fazer uso de macros.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento em Elixir.
# Requisitos atendidos:
#   1. Validação dos dados antes de qualquer operação (Entregas.Validacao / Entregas.Entrega)
#   2. Expiração automática de registros (varredura periódica no Entregas.Rastreador)
#   3. Recuperação após falhas (Entregas.Sup com :rest_for_one + tabelas ETS no Entregas.Guardiao)
#   4. Uso de macros (defregra, __using__, __before_compile__, com_historico)
#   5. Histórico das operações (Entregas.Historico)
#
# Uso básico (no iex, após compilar este arquivo):
#   {:ok, _} = Entregas.iniciar(intervalo_expiracao: 500)
#   Entregas.Rastreador.cadastrar(%{id: "E1", destinatario: "Ana", endereco: "Rua A, 10",
#                                    peso_kg: 2.5, ttl_ms: 60_000})
#   Entregas.Rastreador.atualizar_status("E1", :em_transito)
#   Entregas.Rastreador.consultar("E1")
#   Entregas.Rastreador.historico("E1")

# ---------------------------------------------------------------------------
# Macros de validação: permitem declarar regras de forma declarativa.
# Cada `defregra` gera uma função pública que retorna :ok ou {:error, {campo, msg}}
# e registra o nome da regra em um atributo acumulado. Ao final da compilação,
# `__before_compile__` gera `validar/1`, que executa todas as regras.
# ---------------------------------------------------------------------------
defmodule Entregas.Validacao do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import Entregas.Validacao, only: [defregra: 4]
      Module.register_attribute(__MODULE__, :regras, accumulate: true)
      @before_compile Entregas.Validacao
    end
  end

  # Declara uma regra de validação para um campo do mapa de entrada.
  defmacro defregra(nome, campo, mensagem, predicado) do
    quote do
      @regras unquote(nome)

      def unquote(nome)(dados) do
        valor = Map.get(dados, unquote(campo))

        if (unquote(predicado)).(valor),
          do: :ok,
          else: {:error, {unquote(campo), unquote(mensagem)}}
      end
    end
  end

  # Gera a função `validar/1` agregando todas as regras declaradas.
  defmacro __before_compile__(env) do
    regras = env.module |> Module.get_attribute(:regras) |> Enum.reverse()

    quote do
      def validar(dados) when is_map(dados) do
        erros =
          Enum.flat_map(unquote(regras), fn regra ->
            case apply(__MODULE__, regra, [dados]) do
              :ok -> []
              {:error, erro} -> [erro]
            end
          end)

        if erros == [], do: {:ok, dados}, else: {:error, erros}
      end

      def validar(_), do: {:error, [{:dados, "devem ser um mapa"}]}
    end
  end
end

# ---------------------------------------------------------------------------
# Regras de negócio da entrega: validação, criação de registro e transições.
# ---------------------------------------------------------------------------
defmodule Entregas.Entrega do
  use Entregas.Validacao

  defregra :valida_id, :id, "deve ser texto não vazio",
    &(is_binary(&1) and byte_size(&1) > 0)

  defregra :valida_destinatario, :destinatario, "deve ser texto não vazio",
    &(is_binary(&1) and byte_size(&1) > 0)

  defregra :valida_endereco, :endereco, "deve ser texto não vazio",
    &(is_binary(&1) and byte_size(&1) > 0)

  defregra :valida_peso, :peso_kg, "deve ser número positivo",
    &(is_number(&1) and &1 > 0)

  defregra :valida_ttl, :ttl_ms, "deve ser inteiro positivo em milissegundos",
    &(is_integer(&1) and &1 > 0)

  # Máquina de estados: transições permitidas por status de origem.
  @transicoes %{
    postada: [:em_transito, :cancelada],
    em_transito: [:entregue, :extraviada]
  }

  # Monta o registro persistido a partir de dados já validados.
  def novo(dados, agora) do
    %{
      id: dados.id,
      destinatario: dados.destinatario,
      endereco: dados.endereco,
      peso_kg: dados.peso_kg,
      status: :postada,
      criada_em: agora,
      expira_em: agora + dados.ttl_ms
    }
  end

  # Indica se o registro já expirou no instante informado.
  def expirada?(%{expira_em: expira_em}, agora), do: agora >= expira_em

  # Verifica se a transição de status é permitida.
  def transicao(de, para) do
    if para in Map.get(@transicoes, de, []),
      do: :ok,
      else: {:error, {:transicao_invalida, de, para}}
  end
end

# ---------------------------------------------------------------------------
# Histórico de operações: armazenado em ETS ordenada pela sequência monotônica.
# A macro `com_historico` executa um bloco e registra automaticamente a operação.
# ---------------------------------------------------------------------------
defmodule Entregas.Historico do
  @moduledoc false
  @tab :entregas_historico

  # Executa o bloco, registra a operação e devolve o resultado original.
  defmacro com_historico(operacao, id, do: bloco) do
    quote do
      resultado = unquote(bloco)
      Entregas.Historico.registrar(unquote(operacao), unquote(id), resultado)
      resultado
    end
  end

  # Registra uma operação no histórico.
  def registrar(operacao, id, resultado) do
    seq = System.unique_integer([:monotonic, :positive])

    registro = %{
      seq: seq,
      em: System.system_time(:millisecond),
      operacao: operacao,
      id: id,
      resultado: classificar(resultado)
    }

    true = :ets.insert(@tab, {seq, registro})
    resultado
  end

  # Lista todo o histórico ou apenas o de um id específico, em ordem cronológica.
  def listar(:todos), do: @tab |> :ets.tab2list() |> Enum.map(fn {_, r} -> r end)

  def listar(id) do
    for {_, r} <- :ets.tab2list(@tab), r.id == id, do: r
  end

  defp classificar({:error, motivo}), do: {:erro, motivo}
  defp classificar(_), do: :ok
end

# ---------------------------------------------------------------------------
# Guardião: dono das tabelas ETS. Como as tabelas pertencem a este processo,
# elas sobrevivem a falhas do Rastreador, permitindo sua recuperação.
# ---------------------------------------------------------------------------
defmodule Entregas.Guardiao do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Tabela de rastreio: acesso público para que o Rastreador possa ser reiniciado.
    :ets.new(:entregas_rastreio, [:set, :public, :named_table, read_concurrency: true])
    # Tabela de histórico: ordenada pela sequência monotônica.
    :ets.new(:entregas_historico, [:ordered_set, :public, :named_table])
    {:ok, nil}
  end
end

# ---------------------------------------------------------------------------
# Rastreador: API pública e lógica de negócio. Realiza expiração periódica.
# Em caso de falha, o Supervisor reinicia este processo, que recupera o estado
# a partir das tabelas ETS e reagenda a varredura de expiração.
# ---------------------------------------------------------------------------
defmodule Entregas.Rastreador do
  use GenServer
  require Entregas.Historico
  alias Entregas.{Entrega, Historico}

  @tab :entregas_rastreio
  @intervalo_padrao 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Valida os dados no cliente antes de chamar o servidor.
  # Falhas de validação também são registradas no histórico.
  def cadastrar(dados) do
    case Entrega.validar(dados) do
      {:ok, valido} ->
        GenServer.call(__MODULE__, {:cadastrar, valido})

      {:error, _motivos} = erro ->
        Historico.registrar(:cadastrar, id_de(dados), erro)
    end
  end

  # Valida o id e o status informado antes de solicitar a atualização.
  def atualizar_status(id, novo_status) do
    cond do
      not (is_binary(id) and byte_size(id) > 0) ->
        Historico.registrar(:status, id, {:error, :id_invalido})

      not is_atom(novo_status) ->
        Historico.registrar(:status, id, {:error, :status_invalido})

      true ->
        GenServer.call(__MODULE__, {:status, id, novo_status})
    end
  end

  # Consulta leitura direta em ETS, respeitando a expiração.
  def consultar(id) do
    agora = agora()

    case :ets.lookup(@tab, id) do
      [{^id, reg}] ->
        if Entrega.expirada?(reg, agora),
          do: {:error, :expirada},
          else: {:ok, reg}

      [] ->
        {:error, :nao_encontrada}
    end
  end

  # Retorna o histórico completo ou de um id específico.
  def historico(id \\ :todos), do: Historico.listar(id)

  # -------------------------------------------------------------------------
  # Callbacks do GenServer
  # -------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Verifica que a tabela do Guardião está disponível antes de iniciar.
    case :ets.info(@tab) do
      :undefined ->
        {:stop, :tabela_indisponivel}

      _ ->
        estado = %{intervalo: Keyword.get(opts, :intervalo_expiracao, @intervalo_padrao)}
        {:ok, estado, {:continue, :agendar}}
    end
  end

  @impl true
  def handle_continue(:agendar, estado) do
    agendar(estado)
    {:noreply, estado}
  end

  @impl true
  def handle_call({:cadastrar, dados}, _from, estado) do
    resposta =
      Historico.com_historico(:cadastrar, dados.id) do
        inserir(dados)
      end

    {:reply, resposta, estado}
  end

  def handle_call({:status, id, novo_status}, _from, estado) do
    resposta =
      Historico.com_historico(:status, id) do
        atualizar(id, novo_status)
      end

    {:reply, resposta, estado}
  end

  @impl true
  def handle_info(:expirar, estado) do
    varrer_expiradas()
    agendar(estado)
    {:noreply, estado}
  end

  # -------------------------------------------------------------------------
  # Funções privadas
  # -------------------------------------------------------------------------

  defp inserir(dados) do
    case :ets.lookup(@tab, dados.id) do
      [] ->
        reg = Entrega.novo(dados, agora())
        true = :ets.insert(@tab, {dados.id, reg})
        {:ok, reg}

      [_] ->
        {:error, [{:id, "já cadastrada"}]}
    end
  end

  defp atualizar(id, novo_status) do
    agora = agora()

    with [{^id, reg}] <- :ets.lookup(@tab, id),
         false <- Entrega.expirada?(reg, agora),
         :ok <- Entrega.transicao(reg.status, novo_status) do
      novo_reg = %{reg | status: novo_status}
      true = :ets.insert(@tab, {id, novo_reg})
      {:ok, novo_reg}
    else
      [] -> {:error, :nao_encontrada}
      true -> {:error, :expirada}
      {:error, _} = erro -> erro
    end
  end

  # Remove registros expirados e registra a expiração no histórico.
  defp varrer_expiradas do
    agora = agora()

    for {id, reg} <- :ets.tab2list(@tab), Entrega.expirada?(reg, agora) do
      true = :ets.delete(@tab, id)
      Historico.registrar(:expirar, id, :ok)
    end

    :ok
  end

  defp agendar(%{intervalo: intervalo}) do
    Process.send_after(self(), :expirar, intervalo)
  end

  defp agora, do: System.system_time(:millisecond)

  defp id_de(dados) when is_map(dados), do: Map.get(dados, :id)
  defp id_de(_), do: nil
end

# ---------------------------------------------------------------------------
# Supervisor: :rest_for_one garante que, se o Guardião (dono das tabelas) falhar,
# o Rastreador também seja reiniciado. Se apenas o Rastreador falhar, o Guardião
# permanece íntegro e preserva os dados.
# ---------------------------------------------------------------------------
defmodule Entregas.Sup do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      Entregas.Guardiao,
      {Entregas.Rastreador, opts}
    ]

    Supervisor.init(children, strategy: :rest_for_one, max_restarts: 5, max_seconds: 10)
  end
end

# ---------------------------------------------------------------------------
# Ponto de entrada da aplicação.
# ---------------------------------------------------------------------------
defmodule Entregas do
  @moduledoc false

  # Inicia a árvore de supervisão com as opções informadas
  # (ex.: intervalo_expiracao: em milissegundos).
  def iniciar(opts \\ []), do: Entregas.Sup.start_link(opts)
end
