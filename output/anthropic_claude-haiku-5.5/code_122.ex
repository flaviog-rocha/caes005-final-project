# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule EstoqueServer do
  @moduledoc """
  Gerenciador de estoque baseado em GenServer.

  - Histórico: toda operação (bem-sucedida ou não) é registrada em ordem cronológica.
  - Consistência: todas as alterações de estado passam pelo mesmo processo, que as
    executa uma por vez. Cada operação de leitura e escrita é atômica.
  - Limite de concorrência: `executar/2` controla quantas tarefas podem rodar ao mesmo
    tempo por meio de permissões. Chamadas excedentes ficam em fila de espera.
  """

  use GenServer

  @max_concorrencia_padrao 5

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o servidor.

  Opções:
    * `:name` - nome para registro do processo (opcional)
    * `:estoque_inicial` - mapa `%{sku => quantidade}` (padrão: `%{}`)
    * `:max_concorrencia` - número máximo de tarefas simultâneas em `executar/2`
  """
  def start_link(opts \\ []) do
    {nome, opts} = Keyword.pop(opts, :name)
    gen_opts = if nome, do: [name: nome], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Adiciona `qtd` unidades ao SKU informado."
  def adicionar(srv, sku, qtd) when is_binary(sku) and is_integer(qtd) and qtd > 0 do
    GenServer.call(srv, {:adicionar, sku, qtd})
  end

  @doc "Remove `qtd` unidades do SKU, falhando se não houver estoque suficiente."
  def remover(srv, sku, qtd) when is_binary(sku) and is_integer(qtd) and qtd > 0 do
    GenServer.call(srv, {:remover, sku, qtd})
  end

  @doc "Consulta a quantidade atual de um SKU."
  def consultar(srv, sku) when is_binary(sku) do
    GenServer.call(srv, {:consultar, sku})
  end

  @doc "Retorna o histórico de operações em ordem cronológica."
  def historico(srv) do
    GenServer.call(srv, :historico)
  end

  @doc "Retorna estatísticas de concorrência (tarefas ativas e aguardando)."
  def estatisticas(srv) do
    GenServer.call(srv, :estatisticas)
  end

  @doc """
  Executa `fun` (que recebe o `srv`) respeitando o limite de concorrência.

  Se o limite já foi atingido, o chamador aguarda na fila até obter uma permissão.
  A permissão é liberada mesmo se `fun` lançar exceção. Se o processo chamador
  morrer, o servidor libera a permissão automaticamente via monitor.
  """
  def executar(srv, fun) when is_function(fun, 1) do
    permissao = GenServer.call(srv, :adquirir, :infinity)

    try do
      fun.(srv)
    after
      :ok = GenServer.call(srv, {:liberar, permissao})
    end
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    estado = %{
      produtos: Keyword.get(opts, :estoque_inicial, %{}),
      historico: [],
      proximo_id: 1,
      max_concorrencia: Keyword.get(opts, :max_concorrencia, @max_concorrencia_padrao),
      # permissão (referência do monitor) => pid do dono
      ativas: %{},
      # fila de {from, pid} aguardando permissão
      fila: :queue.new()
    }

    {:ok, estado}
  end

  @impl true
  def handle_call({:adicionar, sku, qtd}, _from, s) do
    novo = Map.get(s.produtos, sku, 0) + qtd

    s =
      %{s | produtos: Map.put(s.produtos, sku, novo)}
      |> registrar(:adicionar, sku, qtd, :ok)

    {:reply, {:ok, novo}, s}
  end

  def handle_call({:remover, sku, qtd}, _from, s) do
    atual = Map.get(s.produtos, sku, 0)

    if atual >= qtd do
      novo = atual - qtd

      s =
        %{s | produtos: Map.put(s.produtos, sku, novo)}
        |> registrar(:remover, sku, qtd, :ok)

      {:reply, {:ok, novo}, s}
    else
      s = registrar(s, :remover, sku, qtd, {:erro, :estoque_insuficiente})
      {:reply, {:error, :estoque_insuficiente}, s}
    end
  end

  def handle_call({:consultar, sku}, _from, s) do
    {:reply, Map.get(s.produtos, sku, 0), s}
  end

  def handle_call(:historico, _from, s) do
    # O histórico é armazenado em ordem reversa por eficiência; invertemos na leitura.
    {:reply, Enum.reverse(s.historico), s}
  end

  def handle_call(:estatisticas, _from, s) do
    stats = %{
      ativas: map_size(s.ativas),
      aguardando: :queue.len(s.fila),
      max_concorrencia: s.max_concorrencia
    }

    {:reply, stats, s}
  end

  # Solicitação de permissão: concede imediatamente ou enfileira o chamador.
  def handle_call(:adquirir, {pid, _} = from, s) do
    if map_size(s.ativas) < s.max_concorrencia do
      {:noreply, conceder(s, from, pid)}
    else
      {:noreply, %{s | fila: :queue.in({from, pid}, s.fila)}}
    end
  end

  # Liberação de permissão: somente o dono da permissão pode liberá-la.
  def handle_call({:liberar, ref}, {pid, _}, s) do
    case Map.fetch(s.ativas, ref) do
      {:ok, ^pid} ->
        Process.demonitor(ref, [:flush])
        s = %{s | ativas: Map.delete(s.ativas, ref)}
        {:reply, :ok, promover(s)}

      _ ->
        {:reply, {:error, :permissao_invalida}, s}
    end
  end

  # Dono de permissão morreu sem liberá-la: libera automaticamente.
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _motivo}, s) do
    if Map.has_key?(s.ativas, ref) do
      s = %{s | ativas: Map.delete(s.ativas, ref)}
      {:noreply, promover(s)}
    else
      {:noreply, s}
    end
  end

  def handle_info(_msg, s), do: {:noreply, s}

  # ---------------------------------------------------------------------------
  # Funções auxiliares privadas
  # ---------------------------------------------------------------------------

  # Registra uma operação no histórico com id sequencial e timestamp.
  defp registrar(s, operacao, sku, qtd, resultado) do
    entrada = %{
      id: s.proximo_id,
      em: DateTime.utc_now(),
      operacao: operacao,
      sku: sku,
      quantidade: qtd,
      resultado: resultado
    }

    %{s | historico: [entrada | s.historico], proximo_id: s.proximo_id + 1}
  end

  # Concede uma permissão ao processo `pid`, monitorando-o.
  # A referência do monitor serve como identificador da permissão.
  defp conceder(s, from, pid) do
    ref = Process.monitor(pid)
    GenServer.reply(from, ref)
    %{s | ativas: Map.put(s.ativas, ref, pid)}
  end

  # Concede permissões a partir da fila enquanto houver vagas disponíveis.
  defp promover(s) do
    if map_size(s.ativas) < s.max_concorrencia do
      case :queue.out(s.fila) do
        {{:value, {from, pid}}, fila} ->
          s
          |> Map.put(:fila, fila)
          |> conceder(from, pid)
          |> promover()

        {:empty, _} ->
          s
      end
    else
      s
    end
  end
end

# Exemplo de uso:
#
# {:ok, srv} = EstoqueServer.start_link(
#   estoque_inicial: %{"SKU-1" => 10},
#   max_concorrencia: 3
# )
#
# EstoqueServer.adicionar(srv, "SKU-1", 5)
# EstoqueServer.remover(srv, "SKU-1", 20)   # => {:error, :estoque_insuficiente}
# EstoqueServer.consultar(srv, "SKU-1")     # => 15
#
# EstoqueServer.executar(srv, fn s ->
#   EstoqueServer.remover(s, "SKU-1", 2)
# end)
#
# EstoqueServer.historico(srv)
# EstoqueServer.estatisticas(srv)
