# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Validar os dados recebidos antes de realizar operações.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento em Elixir.
#
# Requisitos atendidos:
#   1. Validação dos dados em Entregas.Validador antes de qualquer operação.
#   2. Persistência em um banco fictício (Entregas.BancoFalso, baseado em Agent).
#      A persistência é em memória e não depende de Ecto. Para usar Ecto no futuro,
#      basta trocar as funções do BancoFalso por chamadas a um Repo e criar as
#      migrations correspondentes.
#   3. Recuperação após falhas: cada entrega é um processo Entregas.Rastreador
#      supervisionado por um DynamicSupervisor. Ao reiniciar, o processo recarrega
#      seu estado do banco fictício. Como o estado é persistido antes de responder
#      a cada evento, nenhuma transição confirmada é perdida.

defmodule Entregas.Validador do
  @moduledoc """
  Funções puras de validação. Nenhuma operação do sistema deve ocorrer
  sem passar por estas funções.
  """

  @status_validos [:criada, :em_transito, :entregue, :falhou]

  # Transições permitidas entre status da entrega.
  @transicoes %{
    criada: [:em_transito, :falhou],
    em_transito: [:entregue, :falhou],
    entregue: [],
    falhou: [:em_transito]
  }

  @doc "Valida os atributos para criação de uma entrega."
  def entrega(attrs) when is_map(attrs) do
    with {:ok, codigo} <- codigo(Map.get(attrs, :codigo)),
         {:ok, destinatario} <- texto(attrs, :destinatario),
         {:ok, peso} <- peso(Map.get(attrs, :peso_kg)),
         {:ok, endereco} <- endereco(Map.get(attrs, :endereco)) do
      {:ok, %{codigo: codigo, destinatario: destinatario, peso_kg: peso, endereco: endereco}}
    end
  end

  def entrega(_), do: {:error, :atributos_invalidos}

  @doc "Valida o código de rastreio (2 a 20 caracteres alfanuméricos maiúsculos)."
  def codigo(codigo) when is_binary(codigo) do
    if Regex.match?(~r/^[A-Z0-9]{2,20}$/, codigo),
      do: {:ok, codigo},
      else: {:error, {:campo_invalido, :codigo}}
  end

  def codigo(_), do: {:error, {:campo_invalido, :codigo}}

  @doc "Valida um evento de rastreamento considerando o status atual da entrega."
  def evento(attrs, status_atual) when is_map(attrs) do
    with {:ok, status} <- status(attrs),
         :ok <- transicao(status_atual, status),
         {:ok, local} <- texto(attrs, :local) do
      {:ok, %{status: status, local: local, em: DateTime.utc_now()}}
    end
  end

  def evento(_, _), do: {:error, :atributos_invalidos}

  @doc "Verifica se a transição de status é permitida."
  def transicao(de, para) do
    if para in Map.get(@transicoes, de, []),
      do: :ok,
      else: {:error, {:transicao_invalida, de, para}}
  end

  # --- Helpers privados ---

  defp texto(attrs, campo) do
    case Map.get(attrs, campo) do
      valor when is_binary(valor) ->
        case String.trim(valor) do
          "" -> {:error, {:campo_invalido, campo}}
          limpo -> {:ok, limpo}
        end

      _ ->
        {:error, {:campo_invalido, campo}}
    end
  end

  defp peso(valor) when is_number(valor) and valor > 0, do: {:ok, valor}
  defp peso(_), do: {:error, {:campo_invalido, :peso_kg}}

  defp status(attrs) do
    case Map.get(attrs, :status) do
      s when s in @status_validos -> {:ok, s}
      _ -> {:error, {:campo_invalido, :status}}
    end
  end

  defp endereco(endereco) when is_map(endereco) do
    with {:ok, rua} <- texto(endereco, :rua),
         {:ok, cidade} <- texto(endereco, :cidade),
         {:ok, cep} <- cep(Map.get(endereco, :cep)) do
      {:ok, %{rua: rua, cidade: cidade, cep: cep}}
    end
  end

  defp endereco(_), do: {:error, {:campo_invalido, :endereco}}

  defp cep(valor) when is_binary(valor) do
    if Regex.match?(~r/^\d{5}-?\d{3}$/, valor),
      do: {:ok, valor},
      else: {:error, {:campo_invalido, :cep}}
  end

  defp cep(_), do: {:error, {:campo_invalido, :cep}}
end

defmodule Entregas.BancoFalso do
  @moduledoc """
  Banco de dados fictício em memória. Um Agent supervisionado guarda os
  registros por código de rastreio. Se o Agent cair, o supervisor do sistema
  o reinicia (ver Entregas.Sistema).
  """
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def buscar(codigo), do: Agent.get(__MODULE__, &Map.get(&1, codigo))

  def salvar(codigo, estado), do: Agent.update(__MODULE__, &Map.put(&1, codigo, estado))

  def codigos, do: Agent.get(__MODULE__, &Map.keys/1)
end

defmodule Entregas.Rastreador do
  @moduledoc """
  Processo que representa uma entrega individual.

  - Ao iniciar, recarrega o estado do banco fictício (recuperação após falhas).
  - Cada evento é validado, persistido e só então confirmado ao chamador.
  - `restart: :transient`: reinicia apenas em caso de falha; encerramento
    normal não gera reinício.
  """
  use GenServer, restart: :transient

  alias Entregas.{BancoFalso, Validador}

  def start_link(codigo) do
    GenServer.start_link(__MODULE__, codigo, name: via(codigo))
  end

  def via(codigo), do: {:via, Registry, {Entregas.Registro, codigo}}

  def registrar_evento(codigo, attrs), do: chamar(codigo, {:evento, attrs})

  def consultar(codigo), do: chamar(codigo, :consultar)

  # Provoca uma falha intencional para testar a recuperação.
  def simular_falha(codigo), do: GenServer.cast(via(codigo), :falha_simulada)

  # --- Callbacks ---

  @impl true
  def init(codigo) do
    # Recuperação: carrega o último estado persistido.
    # Se não existir registro, não inicia o processo.
    case BancoFalso.buscar(codigo) do
      nil -> :ignore
      estado -> {:ok, estado}
    end
  end

  @impl true
  def handle_call({:evento, attrs}, _from, estado) do
    case Validador.evento(attrs, estado.status) do
      {:ok, evento} ->
        novo = aplicar_evento(estado, evento)
        # Persiste ANTES de responder: se falhar aqui, o processo reinicia
        # com o estado anterior consistente.
        :ok = BancoFalso.salvar(novo.codigo, novo)
        {:reply, {:ok, novo}, novo}

      erro ->
        {:reply, erro, estado}
    end
  end

  def handle_call(:consultar, _from, estado) do
    {:reply, {:ok, estado}, estado}
  end

  @impl true
  def handle_cast(:falha_simulada, _estado) do
    raise "falha simulada no rastreador"
  end

  # --- Helpers privados ---

  defp aplicar_evento(estado, evento) do
    %{
      estado
      | status: evento.status,
        historico: [evento | estado.historico]
    }
  end

  defp chamar(codigo, mensagem) do
    GenServer.call(via(codigo), mensagem)
  catch
    :exit, _ -> {:error, :indisponivel}
  end
end

defmodule Entregas.Sistema do
  @moduledoc """
  Árvore de supervisão do sistema.

  - BancoFalso: estado persistido (fictício).
  - Registry: localização dos rastreadores por código.
  - DynamicSupervisor: supervisiona um processo Rastreador por entrega.

  Estratégia :rest_for_one: se o banco cair, os demais processos são
  reiniciados para evitar estado inconsistente.
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      Entregas.BancoFalso,
      {Registry, keys: :unique, name: Entregas.Registro},
      {DynamicSupervisor,
       strategy: :one_for_one, name: Entregas.Dinamico, max_restarts: 5, max_seconds: 10}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule Entregas do
  @moduledoc """
  API pública do sistema de entregas e rastreamento.
  """

  alias Entregas.{BancoFalso, Rastreador, Validador}

  @dinamico Entregas.Dinamico
  @registro Entregas.Registro

  @doc "Inicia a árvore de supervisão e recupera as entregas persistidas."
  def iniciar do
    with {:ok, pid} <- Entregas.Sistema.start_link([]) do
      recuperar_todas()
      {:ok, pid}
    end
  end

  @doc "Cria uma nova entrega após validação e a torna ativa."
  def criar(attrs) do
    with {:ok, dados} <- Validador.entrega(attrs),
         :ok <- nao_existe(dados.codigo),
         estado = estado_inicial(dados),
         :ok <- BancoFalso.salvar(dados.codigo, estado),
         {:ok, _pid} <- iniciar_rastreador(dados.codigo) do
      {:ok, estado}
    end
  end

  @doc "Registra um evento de rastreamento para a entrega informada."
  def registrar_evento(codigo, attrs) do
    with {:ok, codigo} <- Validador.codigo(codigo),
         {:ok, _pid} <- garantir_ativo(codigo) do
      Rastreador.registrar_evento(codigo, attrs)
    end
  end

  @doc "Consulta o estado atual de uma entrega."
  def consultar(codigo) do
    with {:ok, codigo} <- Validador.codigo(codigo),
         {:ok, _pid} <- garantir_ativo(codigo) do
      Rastreador.consultar(codigo)
    end
  end

  @doc "Provoca uma falha no rastreador (uso para testes de recuperação)."
  def simular_falha(codigo) do
    with {:ok, codigo} <- Validador.codigo(codigo),
         {:ok, _pid} <- garantir_ativo(codigo) do
      Rastreador.simular_falha(codigo)
    end
  end

  @doc "Reinicia os rastreadores de todas as entregas persistidas. Retorna a quantidade."
  def recuperar_todas do
    codigos = BancoFalso.codigos()

    Enum.each(codigos, fn codigo ->
      iniciar_rastreador(codigo)
    end)

    length(codigos)
  end

  # --- Helpers privados ---

  defp estado_inicial(dados) do
    Map.merge(dados, %{status: :criada, historico: []})
  end

  defp nao_existe(codigo) do
    case BancoFalso.buscar(codigo) do
      nil -> :ok
      _ -> {:error, :ja_existe}
    end
  end

  # Garante que o rastreador esteja ativo; se não estiver (ex.: após reinício
  # do sistema), ele é recriado a partir do banco fictício.
  defp garantir_ativo(codigo) do
    case Registry.lookup(@registro, codigo) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        if BancoFalso.buscar(codigo) do
          iniciar_rastreador(codigo)
        else
          {:error, :nao_encontrada}
        end
    end
  end

  defp iniciar_rastreador(codigo) do
    case DynamicSupervisor.start_child(@dinamico, {Rastreador, codigo}) do
      {:ok, :undefined} -> {:error, :nao_encontrada}
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, motivo} -> {:error, motivo}
    end
  end
end

# Exemplo de uso (para execução no iex):
#
# {:ok, _} = Entregas.iniciar()
#
# {:ok, _} =
#   Entregas.criar(%{
#     codigo: "BR123456789",
#     destinatario: "Maria Silva",
#     peso_kg: 2.5,
#     endereco: %{rua: "Rua A, 100", cidade: "São Paulo", cep: "01001-000"}
#   })
#
# {:ok, _} = Entregas.registrar_evento("BR123456789", %{status: :em_transito, local: "Centro de Distribuição"})
#
# # Simula falha: o DynamicSupervisor reinicia o rastreador e recarrega o estado.
# Entregas.simular_falha("BR123456789")
# {:ok, estado} = Entregas.consultar("BR123456789")
#
# # Validação: transição inválida é rejeitada.
# Entregas.registrar_evento("BR123456789", %{status: :criada, local: "Origem"})
# #=> {:error, {:transicao_invalida, :em_transito, :criada}}
