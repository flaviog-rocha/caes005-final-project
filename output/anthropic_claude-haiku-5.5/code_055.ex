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

defmodule Entregas do
  @moduledoc """
  Gerenciamento de entregas e rastreamento.

  Requisitos atendidos:
    1. Histórico: cada operação bem-sucedida gera um evento sequencial persistido.
    2. Limite de concorrência: `Entregas.Limitador` é um semáforo que restringe
       quantas operações rodam ao mesmo tempo; as demais aguardam em fila FIFO.
    3. Consistência: todas as escritas são serializadas pelo processo dono da
       tabela DETS (`Entregas.Store`), há validação de transições de status e
       controle otimista por versão (`:versao_esperada`).
    4. Banco de dados fictício: a persistência é feita em DETS, sem Ecto.

  Uso:
      {:ok, _} = Entregas.start_link(limite: 3, arquivo: "entregas.dets")
      {:ok, _} = Entregas.criar("PED-1", "Rua A, 100")
      {:ok, _} = Entregas.atualizar_status("PED-1", :em_transito, obs: "Saiu do depósito")
      {:ok, _} = Entregas.atualizar_status("PED-1", :entregue, versao_esperada: 2)
      {:ok, eventos} = Entregas.historico("PED-1")
  """

  alias Entregas.{Limitador, Store}

  @doc "Inicia o supervisor com o limitador de concorrência e o armazenamento DETS."
  def start_link(opts \\ []) do
    limite = Keyword.get(opts, :limite, 5)
    arquivo = Keyword.get(opts, :arquivo, "rastreamento.dets")

    filhos = [
      {Limitador, limite: limite},
      {Store, arquivo: arquivo}
    ]

    Supervisor.start_link(filhos, strategy: :one_for_one, name: Entregas.Supervisor)
  end

  @doc "Cria uma nova entrega no estado `:criada`."
  def criar(id, destino) do
    Limitador.executar(fn -> Store.criar(id, destino) end)
  end

  @doc """
  Altera o status de uma entrega respeitando as transições válidas.

  Opções:
    * `:obs` - observação registrada no histórico
    * `:versao_esperada` - se informado, a operação só ocorre se a versão atual coincidir
  """
  def atualizar_status(id, novo_status, opts \\ []) do
    Limitador.executar(fn -> Store.atualizar_status(id, novo_status, opts) end)
  end

  @doc "Consulta o estado atual de uma entrega."
  def consultar(id) do
    Limitador.executar(fn -> Store.consultar(id) end)
  end

  @doc "Retorna a lista ordenada de eventos de uma entrega."
  def historico(id) do
    Limitador.executar(fn -> Store.historico(id) end)
  end

  @doc "Retorna métricas do limitador de concorrência."
  def status_limitador do
    Limitador.status()
  end
end

defmodule Entregas.Limitador do
  @moduledoc """
  Semáforo de concorrência.

  Cada chamada a `executar/1` adquire uma vaga antes de executar a função
  e libera a vaga ao final, mesmo em caso de exceção. Se o processo chamador
  morrer, o monitor libera a vaga automaticamente. Chamadas excedentes
  ficam em uma fila FIFO até que uma vaga seja liberada.
  """

  use GenServer

  def start_link(opts) do
    limite = Keyword.fetch!(opts, :limite)
    GenServer.start_link(__MODULE__, limite, name: __MODULE__)
  end

  @doc "Executa `fun` respeitando o limite de operações simultâneas."
  def executar(fun) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :adquirir, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:liberar, self()})
    end
  end

  @doc "Retorna o número de vagas em uso, a fila de espera e o limite configurado."
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @impl true
  def init(limite) when is_integer(limite) and limite > 0 do
    {:ok, %{limite: limite, ativos: %{}, fila: :queue.new()}}
  end

  @impl true
  def handle_call(:adquirir, {pid, _} = from, estado) do
    mref = Process.monitor(pid)

    if map_size(estado.ativos) < estado.limite do
      {:reply, :ok, %{estado | ativos: Map.put(estado.ativos, mref, pid)}}
    else
      {:noreply, %{estado | fila: :queue.in({from, mref}, estado.fila)}}
    end
  end

  def handle_call(:status, _from, estado) do
    resposta = %{
      limite: estado.limite,
      em_uso: map_size(estado.ativos),
      enfileirados: :queue.len(estado.fila)
    }

    {:reply, resposta, estado}
  end

  @impl true
  def handle_cast({:liberar, pid}, estado) do
    case Enum.find(estado.ativos, fn {_mref, p} -> p == pid end) do
      {mref, _pid} ->
        Process.demonitor(mref, [:flush])
        ativos = Map.delete(estado.ativos, mref)
        {:noreply, promover(%{estado | ativos: ativos})}

      nil ->
        {:noreply, estado}
    end
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, _motivo}, estado) do
    # Remove o processo caso esteja ativo ou aguardando na fila
    fila = :queue.filter(fn {_from, m} -> m != mref end, estado.fila)
    ativos = Map.delete(estado.ativos, mref)
    {:noreply, promover(%{estado | ativos: ativos, fila: fila})}
  end

  # Move processos da fila para vagas livres, respeitando o limite
  defp promover(%{limite: limite, ativos: ativos, fila: fila} = estado) do
    with true <- map_size(ativos) < limite,
         {{:value, {{pid, _} = from, mref}}, resto} <- :queue.out(fila) do
      GenServer.reply(from, :ok)
      promover(%{estado | ativos: Map.put(ativos, mref, pid), fila: resto})
    else
      _ -> estado
    end
  end
end

defmodule Entregas.Store do
  @moduledoc """
  Dono da tabela DETS (banco de dados fictício).

  Todas as leituras e escritas passam por este processo, o que serializa
  o acesso e garante que cada operação veja o resultado das anteriores.

  Estrutura da tabela (um único arquivo DETS, tipo :set):
    * `{:entrega, id}`      -> mapa com o estado atual da entrega
    * `{:historico, seq}`   -> evento de histórico com número sequencial
    * `:contador`           -> último número de sequência utilizado
  """

  use GenServer

  @tabela :rastreamento_entregas

  # Transições de status permitidas
  @transicoes %{
    criada: [:em_transito, :cancelada],
    em_transito: [:entregue, :extraviada],
    entregue: [],
    cancelada: [],
    extraviada: []
  }

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def criar(id, destino), do: GenServer.call(__MODULE__, {:criar, id, destino})

  def atualizar_status(id, novo, opts \\ []),
    do: GenServer.call(__MODULE__, {:atualizar, id, novo, opts})

  def consultar(id), do: GenServer.call(__MODULE__, {:consultar, id})

  def historico(id), do: GenServer.call(__MODULE__, {:historico, id})

  @impl true
  def init(opts) do
    # Permite que terminate/2 feche o DETS em desligamentos ordenados
    Process.flag(:trap_exit, true)

    arquivo = opts |> Keyword.get(:arquivo, "rastreamento.dets") |> to_charlist()

    case :dets.open_file(@tabela, file: arquivo, type: :set) do
      {:ok, tab} -> {:ok, %{tab: tab}}
      {:error, motivo} -> {:stop, motivo}
    end
  end

  @impl true
  def handle_call({:criar, id, destino}, _from, %{tab: tab} = estado) do
    resposta =
      case :dets.lookup(tab, {:entrega, id}) do
        [_] ->
          {:error, :ja_existe}

        [] ->
          agora = DateTime.utc_now()

          entrega = %{
            id: id,
            destino: destino,
            status: :criada,
            versao: 1,
            criada_em: agora,
            atualizada_em: agora
          }

          :ok = registrar(tab, entrega, %{operacao: :criar, de: nil, para: :criada, obs: nil})
          {:ok, entrega}
      end

    {:reply, resposta, estado}
  end

  def handle_call({:atualizar, id, novo, opts}, _from, %{tab: tab} = estado) do
    resposta =
      with {:ok, entrega} <- buscar(tab, id),
           :ok <- verificar_versao(entrega, Keyword.get(opts, :versao_esperada)),
           :ok <- validar_transicao(entrega.status, novo) do
        nova = %{
          entrega
          | status: novo,
            versao: entrega.versao + 1,
            atualizada_em: DateTime.utc_now()
        }

        :ok =
          registrar(tab, nova, %{
            operacao: :atualizar_status,
            de: entrega.status,
            para: novo,
            obs: Keyword.get(opts, :obs)
          })

        {:ok, nova}
      end

    {:reply, resposta, estado}
  end

  def handle_call({:consultar, id}, _from, %{tab: tab} = estado) do
    {:reply, buscar(tab, id), estado}
  end

  def handle_call({:historico, id}, _from, %{tab: tab} = estado) do
    # Varredura completa da tabela; adequada para volumes moderados.
    eventos =
      :dets.foldl(
        fn
          {{:historico, _seq}, %{entrega_id: ^id} = evento}, acc -> [evento | acc]
          _outro, acc -> acc
        end,
        [],
        tab
      )

    {:reply, {:ok, Enum.sort_by(eventos, & &1.seq)}, estado}
  end

  @impl true
  def terminate(_motivo, %{tab: tab}) do
    :dets.close(tab)
  end

  # Funções auxiliares (executadas sempre dentro do processo dono do DETS)

  defp buscar(tab, id) do
    case :dets.lookup(tab, {:entrega, id}) do
      [{_, entrega}] -> {:ok, entrega}
      [] -> {:error, :nao_encontrada}
    end
  end

  defp verificar_versao(_entrega, nil), do: :ok
  defp verificar_versao(%{versao: v}, v), do: :ok
  defp verificar_versao(_entrega, _esperada), do: {:error, :conflito_de_versao}

  defp validar_transicao(atual, novo) do
    if novo in Map.get(@transicoes, atual, []) do
      :ok
    else
      {:error, {:transicao_invalida, atual, novo}}
    end
  end

  # Grava o estado da entrega e o evento de histórico em uma única chamada
  # de inserção ao DETS, seguida de sincronização com o disco.
  defp registrar(tab, entrega, detalhes) do
    seq = proximo_seq(tab)

    evento =
      Map.merge(
        %{seq: seq, entrega_id: entrega.id, em: entrega.atualizada_em},
        detalhes
      )

    :ok =
      :dets.insert(tab, [
        {{:entrega, entrega.id}, entrega},
        {{:historico, seq}, evento},
        {:contador, seq}
      ])

    :dets.sync(tab)
  end

  defp proximo_seq(tab) do
    case :dets.lookup(tab, :contador) do
      [{:contador, n}] -> n + 1
      [] -> 1
    end
  end
end
