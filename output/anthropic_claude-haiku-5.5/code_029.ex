# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Executar determinadas operações periodicamente.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de entregas e rastreamento.
#
# Arquitetura (somente biblioteca padrão do Elixir):
#   Entregas.Sistema          -> Supervisor raiz (Repo, Registry e DynamicSupervisor)
#   Entregas.Repo             -> "banco fictício" em memória (Agent). Pode ser trocado
#                                por um Ecto.Repo que implemente Entregas.Armazenamento
#   Entregas.Validador        -> validação pura dos dados (erros de validação)
#   Entregas.Transportadora   -> integração externa simulada (falhas operacionais)
#   Entregas.Rastreamento     -> GenServer por entrega, gerenciado pelo DynamicSupervisor,
#                                com tick periódico e recuperação de estado após falhas
#   Entregas.Gerenciador      -> API pública do domínio
#   Entregas.Demo            -> cenário de demonstração
#
# Convenção de erros:
#   {:error, {:validacao, [{campo, mensagem}]}}  -> dados inválidos (erro do chamador)
#   {:error, {:operacional, motivo}}             -> falha de infraestrutura (pode ser reintentada)

defmodule Entregas.Entrega do
  # Estrutura de dados de uma entrega.
  @enforce_keys [:id, :destino, :peso_kg]
  defstruct [:id, :destino, :peso_kg, status: :criada, eventos: [], atualizada_em: nil]
end

defmodule Entregas.Armazenamento do
  # Contrato de persistência. Qualquer implementação (em memória, Ecto.Repo com
  # Ecto.Migration criando a tabela "entregas", etc.) pode ser usada sem alterar o resto.
  @callback buscar(id :: String.t()) ::
              {:ok, Entregas.Entrega.t()}
              | {:error, :nao_encontrada}
              | {:error, {:operacional, term()}}

  @callback salvar(Entregas.Entrega.t()) :: :ok | {:error, {:operacional, term()}}
end

defmodule Entregas.Repo do
  # Banco fictício em memória. Como roda em processo separado (Agent), o estado
  # sobrevive à queda dos processos de rastreamento e permite a recuperação.
  @behaviour Entregas.Armazenamento
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @impl true
  def buscar(id) do
    case chamar(fn -> Agent.get(__MODULE__, &Map.fetch(&1, id)) end) do
      {:ok, entrega} -> {:ok, entrega}
      :error -> {:error, :nao_encontrada}
      {:error, _} = erro -> erro
    end
  end

  @impl true
  def salvar(%Entregas.Entrega{id: id} = entrega) do
    chamar(fn -> Agent.update(__MODULE__, &Map.put(&1, id, entrega)) end)
  end

  # Converte indisponibilidade do "banco" em erro operacional (não em crash do chamador).
  defp chamar(fun) do
    fun.()
  catch
    :exit, motivo -> {:error, {:operacional, {:banco_indisponivel, motivo}}}
  end
end

defmodule Entregas.Validador do
  # Validações puras. Nenhuma operação com efeito colateral ocorre antes de validar.
  alias Entregas.Entrega

  @status_validos [:criada, :em_transito, :saiu_para_entrega, :entregue, :cancelada]

  # Máquina de estados: transições permitidas.
  @transicoes %{
    criada: [:em_transito, :cancelada],
    em_transito: [:saiu_para_entrega, :cancelada],
    saiu_para_entrega: [:entregue],
    entregue: [],
    cancelada: []
  }

  @doc "Valida os parâmetros de criação de uma entrega."
  def criar(params) when is_map(params) do
    erros =
      []
      |> exigir(texto?(params[:id]), :id, "deve ser texto não vazio")
      |> exigir(texto?(params[:destino]), :destino, "deve ser texto não vazio")
      |> exigir(peso_valido?(params[:peso_kg]), :peso_kg, "deve ser número positivo")

    case erros do
      [] ->
        {:ok, %Entrega{id: params.id, destino: params.destino, peso_kg: params.peso_kg}}

      _ ->
        {:error, {:validacao, Enum.reverse(erros)}}
    end
  end

  def criar(_params), do: {:error, {:validacao, [{:params, "deve ser um mapa"}]}}

  @doc "Valida e aplica uma transição de status na máquina de estados."
  def transicao(%Entrega{status: atual} = entrega, novo) do
    cond do
      not (novo in @status_validos) ->
        {:error, {:validacao, [{:status, "status desconhecido: #{inspect(novo)}"}]}}

      novo in Map.fetch!(@transicoes, atual) ->
        {:ok, %{entrega | status: novo}}

      true ->
        {:error, {:validacao, [{:status, "transição de #{atual} para #{novo} não permitida"}]}}
    end
  end

  defp exigir(erros, true, _campo, _msg), do: erros
  defp exigir(erros, false, campo, msg), do: [{campo, msg} | erros]

  defp texto?(valor), do: is_binary(valor) and String.trim(valor) != ""
  defp peso_valido?(valor), do: is_number(valor) and valor > 0
end

defmodule Entregas.Transportadora do
  # Simula uma API externa de rastreamento. Pode falhar aleatoriamente
  # (taxa configurável) para exercitar o tratamento de falhas operacionais.

  @doc "Consulta o próximo status reportado pela transportadora."
  def consultar_status(%Entregas.Entrega{status: atual}) do
    taxa = Application.get_env(:entregas, :taxa_falha_transportadora, 0.0)

    if :rand.uniform() < taxa do
      {:error, :timeout}
    else
      {:ok, proximo(atual)}
    end
  end

  defp proximo(:criada), do: :em_transito
  defp proximo(:em_transito), do: :saiu_para_entrega
  defp proximo(:saiu_para_entrega), do: :entregue
  # nil indica que não há novidade a sincronizar
  defp proximo(_), do: nil
end

defmodule Entregas.Rastreamento do
  # Um processo por entrega, supervisionado pelo DynamicSupervisor.
  # - Recuperação: o estado é sempre persistido antes de responder; ao ser
  #   reiniciado após uma falha, o processo recarrega o último estado do Repo.
  # - Periódico: a cada intervalo, sincroniza o status com a transportadora.
  use GenServer
  require Logger

  alias Entregas.{Entrega, Repo, Transportadora, Validador}

  def start_link({%Entrega{id: id}, _opts} = arg) do
    GenServer.start_link(__MODULE__, arg, name: via(id))
  end

  def via(id), do: {:via, Registry, {Entregas.Registro, id}}

  @impl true
  def init({entrega, opts}) do
    intervalo = Keyword.get(opts, :intervalo_ms, 5_000)
    estado = %{intervalo: intervalo, entrega: nil}
    # Carregamento do armazenamento feito fora do init, para não bloquear o supervisor.
    {:ok, estado, {:continue, {:carregar, entrega}}}
  end

  @impl true
  def handle_continue({:carregar, entrega}, estado) do
    case Repo.buscar(entrega.id) do
      {:ok, persistida} ->
        Logger.info("Entrega #{entrega.id} carregada do armazenamento (inicialização/recuperação)")
        {:noreply, %{estado | entrega: persistida} |> agendar()}

      {:error, :nao_encontrada} ->
        {:noreply, %{estado | entrega: persistir!(entrega)} |> agendar()}

      {:error, {:operacional, motivo}} ->
        # Falha operacional: encerra com motivo operacional; o supervisor reinicia.
        {:stop, {:operacional, motivo}, estado}
    end
  end

  @impl true
  def handle_call(:consultar, _from, estado) do
    {:reply, {:ok, estado.entrega}, estado}
  end

  def handle_call({:atualizar_status, novo}, _from, %{entrega: entrega} = estado) do
    case Validador.transicao(entrega, novo) do
      {:ok, nova} ->
        nova = registrar(nova, "status alterado manualmente para #{novo}")
        estado = %{estado | entrega: persistir!(nova)}
        {:reply, {:ok, estado.entrega}, estado}

      {:error, _} = erro ->
        {:reply, erro, estado}
    end
  end

  @impl true
  def handle_info(:tick, estado) do
    {:noreply, estado |> sincronizar() |> agendar()}
  end

  # Operação periódica: sincroniza com a transportadora.
  defp sincronizar(%{entrega: entrega} = estado) do
    case Transportadora.consultar_status(entrega) do
      {:ok, nil} ->
        estado

      {:ok, novo} ->
        case Validador.transicao(entrega, novo) do
          {:ok, atualizada} ->
            atualizada = registrar(atualizada, "sincronizado com transportadora: #{novo}")
            %{estado | entrega: persistir!(atualizada)}

          {:error, {:validacao, detalhes}} ->
            # Dado inconsistente vindo de fora: ignora sem derrubar o processo.
            Logger.warning("Transição ignorada para #{entrega.id}: #{inspect(detalhes)}")
            estado
        end

      {:error, motivo} ->
        # Falha operacional: mantém o estado e tenta novamente no próximo ciclo.
        Logger.warning(
          "Falha operacional ao consultar transportadora (#{entrega.id}): " <>
            "#{inspect(motivo)}. Nova tentativa no próximo ciclo."
        )

        estado
    end
  end

  defp agendar(estado) do
    Process.send_after(self(), :tick, estado.intervalo)
    estado
  end

  # Persistência obrigatória: se o armazenamento falhar, o processo encerra
  # e será reiniciado pelo supervisor a partir do último estado válido.
  defp persistir!(entrega) do
    case Repo.salvar(entrega) do
      :ok -> entrega
      {:error, motivo} -> exit({:operacional, motivo})
    end
  end

  defp registrar(%Entrega{} = entrega, descricao) do
    agora = DateTime.utc_now()
    %{entrega | eventos: [{agora, descricao} | entrega.eventos], atualizada_em: agora}
  end
end

defmodule Entregas.Gerenciador do
  # API pública do domínio. Valida antes de qualquer operação e separa
  # erros de validação de falhas operacionais.
  alias Entregas.{Repo, Rastreamento, Validador}

  @supervisor Entregas.SupervisorEntregas

  @doc "Cria uma entrega e inicia seu processo de rastreamento."
  def criar(params, opts \\ []) do
    with {:ok, entrega} <- Validador.criar(params),
         :ok <- garantir_inexistente(entrega.id),
         :ok <- Repo.salvar(entrega),
         {:ok, _pid} <- iniciar(entrega, opts) do
      {:ok, entrega.id}
    end
  end

  @doc "Altera o status de uma entrega com validação da máquina de estados."
  def atualizar_status(id, novo_status) do
    chamar(id, {:atualizar_status, novo_status})
  end

  @doc "Consulta o estado persistido de uma entrega."
  def consultar(id) do
    case Repo.buscar(id) do
      {:ok, entrega} -> {:ok, entrega}
      {:error, :nao_encontrada} -> {:error, nao_encontrada()}
      {:error, _} = erro -> erro
    end
  end

  @doc "Simula uma falha inesperada no processo de rastreamento (para testes de recuperação)."
  def simular_falha(id) do
    case Registry.lookup(Entregas.Registro, id) do
      [{pid, _}] ->
        Process.exit(pid, :kill)
        :ok

      [] ->
        {:error, nao_encontrada()}
    end
  end

  defp garantir_inexistente(id) do
    case Repo.buscar(id) do
      {:ok, _} -> {:error, {:validacao, [{:id, "já cadastrada"}]}}
      {:error, :nao_encontrada} -> :ok
      {:error, _} = erro -> erro
    end
  end

  defp iniciar(entrega, opts) do
    # O DynamicSupervisor reinicia o filho em caso de falha (restart padrão :permanent).
    case DynamicSupervisor.start_child(@supervisor, {Rastreamento, {entrega, opts}}) do
      {:ok, pid} -> {:ok, pid}
      {:error, motivo} -> {:error, {:operacional, {:falha_ao_iniciar_rastreamento, motivo}}}
    end
  end

  defp chamar(id, mensagem) do
    GenServer.call(Rastreamento.via(id), mensagem)
  catch
    :exit, {:noproc, _} ->
      # Processo inexistente: pode ser id inválido ou processo em recuperação.
      case Repo.buscar(id) do
        {:ok, _} -> {:error, {:operacional, :processo_em_recuperacao}}
        {:error, :nao_encontrada} -> {:error, nao_encontrada()}
        {:error, _} = erro -> erro
      end

    :exit, motivo ->
      {:error, {:operacional, {:falha_na_chamada, motivo}}}
  end

  defp nao_encontrada, do: {:validacao, [{:id, "entrega não encontrada"}]}
end

defmodule Entregas.Sistema do
  # Supervisor raiz. Se o Repo ou o Registry caírem, são reiniciados;
  # os processos de rastreamento são recuperados a partir do Repo.
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Entregas.Repo,
      {Registry, keys: :unique, name: Entregas.Registro},
      {DynamicSupervisor, strategy: :one_for_one, name: Entregas.SupervisorEntregas}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Entregas.Demo do
  # Cenário de demonstração: validação, operação periódica, falha e recuperação.
  alias Entregas.Gerenciador

  def executar do
    {:ok, _} = Entregas.Sistema.start_link()
    Application.put_env(:entregas, :taxa_falha_transportadora, 0.3)

    # Caso de sucesso
    params = %{id: "ENT-001", destino: "Rua A, 100", peso_kg: 2.5}
    IO.inspect(Gerenciador.criar(params, intervalo_ms: 500), label: "criação")

    # Erro de validação (não é falha operacional)
    IO.inspect(
      Gerenciador.criar(%{id: "", destino: nil, peso_kg: -1}),
      label: "validação"
    )

    # Transição inválida (erro de validação)
    IO.inspect(Gerenciador.atualizar_status("ENT-001", :entregue), label: "transição inválida")

    # Operação periódica em andamento (com falhas simuladas da transportadora)
    Process.sleep(2_500)

    # Falha inesperada e recuperação automática pelo DynamicSupervisor
    IO.inspect(Gerenciador.simular_falha("ENT-001"), label: "falha simulada")
    Process.sleep(100)

    IO.inspect(Gerenciador.consultar("ENT-001"), label: "após recuperação")
    IO.inspect(Gerenciador.atualizar_status("ENT-001", :cancelada), label: "cancelamento")
  end
end
