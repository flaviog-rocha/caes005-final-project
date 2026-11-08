# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de entregas e rastreamento.
#
# Requisitos atendidos:
# 1. Histórico: cada criação e transição de status grava um evento sequencial
#    na tabela DETS, consultável por entrega.
# 2. Limite de concorrência: `executar_limitado/2` usa um semáforo controlado
#    pelo GenServer. Chamadas excedentes ficam em fila FIFO sem consumir slot.
# 3. Consistência: todas as escritas passam pelo GenServer, que é o único
#    processo acessando a tabela DETS. Cada entrega tem um campo `versao` para
#    controle otimista (compare-and-swap) e as transições de status são validadas.
#    Um processo que morre com slot ativo libera o slot via monitor.
# 4. Banco de dados: a persistência é feita diretamente em DETS (sem Ecto),
#    que é suficiente para este domínio e não exige dependências externas.
#
# Exemplo de uso:
#
#   {:ok, _} = Entregas.Rastreador.start_link(arquivo: "entregas.dets", max_concorrencia: 3)
#   {:ok, e} = Entregas.Rastreador.criar_entrega("Rua A, 10", "PED-1")
#   {:ok, e} = Entregas.Rastreador.atualizar_status(e.id, :em_transito, e.versao)
#   Entregas.Rastreador.executar_limitado(fn -> processar_algo() end)
#   Entregas.Rastreador.historico(e.id)

defmodule Entregas.Rastreador do
  @moduledoc """
  Serviço de rastreamento de entregas baseado em GenServer e DETS.
  """

  use GenServer

  # Transições de status permitidas (máquina de estados).
  @transicoes %{
    criada: [:em_transito, :cancelada],
    em_transito: [:entregue, :extraviada],
    entregue: [],
    cancelada: [],
    extraviada: []
  }

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o rastreador. Opções: :name, :arquivo, :tabela, :max_concorrencia."
  def start_link(opts \\ []) do
    nome = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: nome)
  end

  @doc "Cria uma nova entrega com status inicial :criada."
  def criar_entrega(srv \\ __MODULE__, destino, pedido) do
    GenServer.call(srv, {:criar, destino, pedido})
  end

  @doc """
  Atualiza o status de uma entrega. Só é aceito se `versao_esperada` for igual
  à versão atual (controle otimista) e se a transição for válida.
  """
  def atualizar_status(srv \\ __MODULE__, id, novo_status, versao_esperada) do
    GenServer.call(srv, {:atualizar, id, novo_status, versao_esperada})
  end

  @doc "Consulta uma entrega pelo id."
  def consultar(srv \\ __MODULE__, id) do
    GenServer.call(srv, {:consultar, id})
  end

  @doc "Retorna o histórico de eventos de uma entrega, em ordem cronológica."
  def historico(srv \\ __MODULE__, id) do
    GenServer.call(srv, {:historico, id})
  end

  @doc """
  Executa `fun` respeitando o limite de operações simultâneas. Se o limite
  estiver atingido, o chamador aguarda na fila. O slot é liberado ao final,
  inclusive em caso de exceção.
  """
  def executar_limitado(srv \\ __MODULE__, fun) when is_function(fun, 0) do
    case GenServer.call(srv, :adquirir, :infinity) do
      :ok ->
        try do
          fun.()
        after
          GenServer.call(srv, :liberar, :infinity)
        end

      {:error, motivo} ->
        {:error, motivo}
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Necessário para que terminate/2 feche a tabela DETS corretamente.
    Process.flag(:trap_exit, true)

    tabela = Keyword.get(opts, :tabela, :entregas_dets)
    arquivo = Keyword.get(opts, :arquivo, "entregas.dets")
    max = Keyword.get(opts, :max_concorrencia, 4)

    case :dets.open_file(tabela, file: String.to_charlist(arquivo), type: :set) do
      {:ok, ^tabela} ->
        # Contadores persistentes; insert_new não sobrescreve valores existentes.
        :dets.insert_new(tabela, {{:meta, :entrega_id}, 0})
        :dets.insert_new(tabela, {{:meta, :hist_seq}, 0})

        {:ok,
         %{
           tab: tabela,
           max: max,
           # Mapa pid => ref de monitor, para quem detém um slot.
           ativos: %{},
           # Fila FIFO de {from, ref} aguardando slot.
           fila: :queue.new()
         }}

      {:error, motivo} ->
        {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:criar, destino, pedido}, _from, st) do
    tab = st.tab
    id = :dets.update_counter(tab, {:meta, :entrega_id}, 1)
    agora = DateTime.utc_now()

    entrega = %{
      id: id,
      destino: destino,
      pedido: pedido,
      status: :criada,
      versao: 1,
      criada_em: agora,
      atualizada_em: agora
    }

    :ok = :dets.insert(tab, {{:entrega, id}, entrega})
    registrar(tab, id, :criada, 1)
    sincronizar(tab)

    {:reply, {:ok, entrega}, st}
  end

  def handle_call({:atualizar, id, novo, versao_esperada}, _from, st) do
    tab = st.tab

    resultado =
      case :dets.lookup(tab, {:entrega, id}) do
        [] ->
          {:error, :nao_encontrada}

        [{_, e}] ->
          cond do
            e.versao != versao_esperada ->
              {:error, {:conflito, e.versao}}

            not Enum.member?(Map.fetch!(@transicoes, e.status), novo) ->
              {:error, {:transicao_invalida, e.status, novo}}

            true ->
              atualizada = %{
                e
                | status: novo,
                  versao: e.versao + 1,
                  atualizada_em: DateTime.utc_now()
              }

              :ok = :dets.insert(tab, {{:entrega, id}, atualizada})
              registrar(tab, id, novo, atualizada.versao)
              sincronizar(tab)
              {:ok, atualizada}
          end
      end

    {:reply, resultado, st}
  end

  def handle_call({:consultar, id}, _from, st) do
    resposta =
      case :dets.lookup(st.tab, {:entrega, id}) do
        [] -> {:error, :nao_encontrada}
        [{_, e}] -> {:ok, e}
      end

    {:reply, resposta, st}
  end

  def handle_call({:historico, id}, _from, st) do
    entradas =
      st.tab
      |> :dets.match_object({{:historico, id, :_}, :_})
      |> Enum.sort_by(fn {{_, _, seq}, _} -> seq end)
      |> Enum.map(fn {_, entrada} -> entrada end)

    {:reply, entradas, st}
  end

  # Controle de concorrência

  def handle_call(:adquirir, {pid, _} = from, st) do
    if Map.has_key?(st.ativos, pid) do
      {:reply, {:error, :ja_possui_slot}, st}
    else
      # Monitora o chamador para tratar morte antes de liberar o slot.
      ref = Process.monitor(pid)
      st = %{st | fila: :queue.in({from, ref}, st.fila)}
      {:noreply, promover(st)}
    end
  end

  def handle_call(:liberar, {pid, _}, st) do
    case Map.pop(st.ativos, pid) do
      {nil, _} ->
        {:reply, {:error, :nao_detentor}, st}

      {ref, ativos} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, promover(%{st | ativos: ativos})}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _motivo}, st) do
    case Map.get(st.ativos, pid) do
      ^ref ->
        # Detentor de slot morreu: libera o slot e promove o próximo.
        {:noreply, promover(%{st | ativos: Map.delete(st.ativos, pid)})}

      _ ->
        # Pode ser um aguardando na fila que morreu: remove da fila.
        fila = :queue.filter(fn {_from, r} -> r != ref end, st.fila)
        {:noreply, %{st | fila: fila}}
    end
  end

  def handle_info(_msg, st), do: {:noreply, st}

  @impl true
  def terminate(_motivo, st) do
    :dets.close(st.tab)
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Concede slots a partir da fila enquanto houver capacidade disponível.
  defp promover(%{ativos: ativos, max: max} = st) when map_size(ativos) >= max, do: st

  defp promover(st) do
    case :queue.out(st.fila) do
      {:empty, _} ->
        st

      {{:value, {{pid, _} = from, ref}}, fila} ->
        GenServer.reply(from, :ok)
        promover(%{st | fila: fila, ativos: Map.put(st.ativos, pid, ref)})
    end
  end

  # Grava um evento no histórico com sequência global monotônica.
  defp registrar(tab, id, status, versao) do
    seq = :dets.update_counter(tab, {:meta, :hist_seq}, 1)

    evento = %{
      status: status,
      versao: versao,
      em: DateTime.utc_now()
    }

    :ok = :dets.insert(tab, {{:historico, id, seq}, evento})
  end

  defp sincronizar(tab), do: :dets.sync(tab)
end
