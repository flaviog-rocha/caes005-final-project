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

# Arquivo único: gerenciamento de entregas e rastreamento em Elixir.
# Executar com: elixir entregas.exs (ou carregar em um projeto Mix).

defmodule Entregas.Validador do
  # Funções puras de validação. Retornam {:ok, dados} ou {:error, {:validacao, mensagens}}.
  # Erros de validação indicam entrada incorreta do cliente e não são reexecutáveis.

  @status_evento [:coletada, :em_transito, :entregue, :extraviada]
  @prazo_padrao 86_400

  def validar_id(id) when is_binary(id) and byte_size(id) > 0, do: {:ok, id}
  def validar_id(_), do: {:error, {:validacao, ["id deve ser texto não vazio"]}}

  def validar_criacao(attrs) when is_map(attrs) do
    prazo = Map.get(attrs, :prazo_segundos, @prazo_padrao)

    erros =
      []
      |> exigir(attrs, :id, &texto?/1, "id deve ser texto não vazio")
      |> exigir(attrs, :destino, &texto?/1, "destino deve ser texto não vazio")
      |> exigir(attrs, :peso_kg, &(is_number(&1) and &1 > 0), "peso_kg deve ser número positivo")

    erros =
      if is_integer(prazo) and prazo > 0,
        do: erros,
        else: ["prazo_segundos deve ser inteiro positivo" | erros]

    case Enum.reverse(erros) do
      [] ->
        {:ok,
         %{
           id: attrs.id,
           destino: attrs.destino,
           peso_kg: attrs.peso_kg,
           prazo_segundos: prazo
         }}

      mensagens ->
        {:error, {:validacao, mensagens}}
    end
  end

  def validar_criacao(_), do: {:error, {:validacao, ["dados devem ser um mapa"]}}

  def validar_evento(attrs) when is_map(attrs) do
    status = Map.get(attrs, :status)
    descricao = Map.get(attrs, :descricao, "")

    erros =
      if status in @status_evento,
        do: [],
        else: ["status deve ser um de #{inspect(@status_evento)}"]

    erros =
      if is_binary(descricao),
        do: erros,
        else: ["descricao deve ser texto" | erros]

    case Enum.reverse(erros) do
      [] -> {:ok, %{status: status, descricao: descricao}}
      mensagens -> {:error, {:validacao, mensagens}}
    end
  end

  def validar_evento(_), do: {:error, {:validacao, ["evento deve ser um mapa"]}}

  defp exigir(erros, attrs, campo, predicado, mensagem) do
    case Map.fetch(attrs, campo) do
      {:ok, valor} -> if predicado.(valor), do: erros, else: [mensagem | erros]
      :error -> ["#{campo} é obrigatório" | erros]
    end
  end

  defp texto?(valor), do: is_binary(valor) and byte_size(valor) > 0
end

defmodule Entregas.Banco do
  # Banco fictício em memória, implementado com Agent.
  # Pode ser substituído por um repositório real (ex.: Ecto) sem alterar os demais módulos.
  # Falhas de acesso (processo indisponível) são convertidas em erro operacional.
  use Agent

  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def buscar(id) do
    executar(fn ->
      case Agent.get(__MODULE__, &Map.fetch(&1, id)) do
        {:ok, registro} -> {:ok, registro}
        :error -> {:error, {:validacao, ["entrega #{id} não encontrada"]}}
      end
    end)
  end

  def inserir(registro) do
    executar(fn ->
      Agent.get_and_update(__MODULE__, fn db ->
        if Map.has_key?(db, registro.id) do
          {{:error, {:validacao, ["entrega #{registro.id} já existe"]}}, db}
        else
          {:ok, Map.put(db, registro.id, registro)}
        end
      end)
    end)
  end

  def salvar(registro) do
    executar(fn ->
      Agent.update(__MODULE__, &Map.put(&1, registro.id, registro))
    end)
  end

  defp executar(fun) do
    fun.()
  catch
    :exit, motivo -> {:error, {:operacional, {:banco_indisponivel, motivo}}}
  end
end

defmodule Entregas.Rastreador do
  # Um processo por entrega. Ao ser reiniciado pelo DynamicSupervisor,
  # recarrega seu estado a partir do banco (recuperação após falhas).
  # Executa verificação periódica (:tick) de atraso.
  use GenServer, restart: :permanent

  @finais [:entregue, :extraviada]

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: via(id))
  end

  def registrar_evento(pid, evento), do: chamar(pid, {:evento, evento})
  def consultar(pid), do: chamar(pid, :consultar)

  defp via(id), do: {:via, Registry, {Entregas.Registry, id}}

  defp chamar(pid, mensagem) do
    GenServer.call(pid, mensagem)
  catch
    :exit, motivo -> {:error, {:operacional, {:rastreador_indisponivel, motivo}}}
  end

  @impl true
  def init(id) do
    case Entregas.Banco.buscar(id) do
      {:ok, registro} ->
        agendar_tick()
        {:ok, registro}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call(:consultar, _from, registro) do
    {:reply, {:ok, registro}, registro}
  end

  def handle_call({:evento, evento}, _from, registro) do
    if registro.status in @finais do
      {:reply, {:error, {:validacao, ["entrega já finalizada (#{registro.status})"]}}, registro}
    else
      novo = %{registro | status: evento.status, eventos: [evento | registro.eventos]}

      case Entregas.Banco.salvar(novo) do
        :ok -> {:reply, {:ok, novo}, novo}
        {:error, _} = erro -> {:reply, erro, registro}
      end
    end
  end

  @impl true
  def handle_info(:tick, registro) do
    agendar_tick()

    agora = System.os_time(:second)
    finalizada? = registro.status in @finais
    atrasada? = not finalizada? and agora > registro.criada_em + registro.prazo_segundos

    novo = %{registro | atrasada?: atrasada?, ultima_verificacao: agora}

    case Entregas.Banco.salvar(novo) do
      :ok ->
        {:noreply, novo}

      # Falha operacional: encerra o processo anormalmente para que o
      # supervisor reinicie e recupere o estado a partir do banco.
      {:error, motivo} ->
        {:stop, {:falha_operacional, motivo}, registro}
    end
  end

  defp agendar_tick do
    intervalo = Application.get_env(:entregas, :intervalo_tick_ms, 5_000)
    Process.send_after(self(), :tick, intervalo)
  end
end

defmodule Entregas.Dinamico do
  # DynamicSupervisor que gerencia os processos Rastreador sob demanda.
  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end

  def iniciar_rastreador(id) do
    case DynamicSupervisor.start_child(__MODULE__, {Entregas.Rastreador, id}) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, {tipo, _} = erro} when tipo in [:validacao, :operacional] -> {:error, erro}
      {:error, motivo} -> {:error, {:operacional, {:falha_ao_iniciar, motivo}}}
    end
  end
end

defmodule Entregas.Application do
  # Árvore de supervisão principal.
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Entregas.Registry},
      Entregas.Banco,
      Entregas.Dinamico
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end

defmodule Entregas do
  # API pública do sistema.
  alias Entregas.{Banco, Dinamico, Rastreador, Validador}

  def iniciar(opts \\ []), do: Entregas.Application.start_link(opts)

  def criar(attrs) do
    with {:ok, dados} <- Validador.validar_criacao(attrs),
         registro = novo_registro(dados),
         :ok <- Banco.inserir(registro),
         {:ok, _pid} <- Dinamico.iniciar_rastreador(registro.id) do
      {:ok, registro.id}
    end
  end

  def registrar_evento(id, attrs) do
    with {:ok, id} <- Validador.validar_id(id),
         {:ok, evento} <- Validador.validar_evento(attrs),
         evento = Map.put(evento, :em, DateTime.to_iso8601(DateTime.utc_now())),
         {:ok, pid} <- garantir_rastreador(id) do
      Rastreador.registrar_evento(pid, evento)
    end
  end

  def consultar(id) do
    with {:ok, id} <- Validador.validar_id(id),
         {:ok, pid} <- garantir_rastreador(id) do
      Rastreador.consultar(pid)
    end
  end

  # Garante que o processo da entrega esteja ativo; se não estiver (ex.: após falha),
  # ele é iniciado e recupera seu estado do banco.
  defp garantir_rastreador(id) do
    case Registry.lookup(Entregas.Registry, id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        with {:ok, _registro} <- Banco.buscar(id) do
          Dinamico.iniciar_rastreador(id)
        end
    end
  end

  defp novo_registro(dados) do
    %{
      id: dados.id,
      destino: dados.destino,
      peso_kg: dados.peso_kg,
      prazo_segundos: dados.prazo_segundos,
      criada_em: System.os_time(:second),
      status: :criada,
      eventos: [],
      atrasada?: false,
      ultima_verificacao: nil
    }
  end
end
