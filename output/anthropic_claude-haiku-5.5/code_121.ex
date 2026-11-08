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

# Gerenciamento de estoque com histórico, consistência concorrente e
# limite de operações simultâneas, usando apenas a biblioteca padrão (GenServer).
#
# Arquitetura:
#   * Estoque               - Supervisor raiz e API pública.
#   * Estoque.Limitador     - GenServer que funciona como semáforo, limitando
#                             quantas operações podem executar ao mesmo tempo.
#   * Estoque.Inventario    - GenServer que mantém o estoque e o histórico.
#                             Toda mutação é serializada por esse processo,
#                             o que garante consistência mesmo com acesso concorrente.

defmodule Estoque do
  @moduledoc """
  API pública do sistema de gerenciamento de estoque.

  Uso básico:

      {:ok, _} = Estoque.start_link(max_concurrent: 4)
      {:ok, 10} = Estoque.adicionar("SKU-1", 10)
      {:ok, 7} = Estoque.remover("SKU-1", 3)
      7 = Estoque.consultar("SKU-1")
      Estoque.historico(10)
  """

  use Supervisor

  @doc """
  Inicia o sistema. Opções:
    * `:max_concurrent` - número máximo de operações executando simultaneamente (padrão: 5)
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_concurrent, 5)

    children = [
      {Estoque.Limitador, max_concurrent: max},
      {Estoque.Inventario, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Adiciona `qtd` unidades ao estoque do `sku`. Retorna `{:ok, novo_saldo}`."
  def adicionar(sku, qtd) do
    executar({:adicionar, sku, qtd})
  end

  @doc """
  Remove `qtd` unidades do estoque do `sku`.
  Retorna `{:ok, novo_saldo}` ou `{:error, :estoque_insuficiente}`.
  """
  def remover(sku, qtd) do
    executar({:remover, sku, qtd})
  end

  @doc "Consulta o saldo atual do `sku`."
  def consultar(sku) do
    executar({:consultar, sku})
  end

  @doc "Retorna as últimas `limite` operações, em ordem cronológica."
  def historico(limite \\ 100) do
    executar({:historico, limite})
  end

  # Toda operação passa pelo limitador de concorrência antes de chegar ao inventário.
  defp executar(op) do
    Estoque.Limitador.executar(fn ->
      GenServer.call(Estoque.Inventario, op)
    end)
  end
end

defmodule Estoque.Limitador do
  @moduledoc """
  Semáforo implementado como GenServer.

  Mantém no máximo `max_concurrent` operações ativas. Chamadas excedentes
  ficam em uma fila FIFO e são liberadas assim que um slot é devolvido.

  Cada slot é identificado pela referência do monitor do processo que o
  adquiriu. Se o processo cliente morrer sem liberar o slot, o monitor
  dispara `:DOWN` e o slot é recuperado automaticamente.
  """

  use GenServer

  @doc "Inicia o limitador com `max_concurrent` slots."
  def start_link(opts) do
    max = Keyword.fetch!(opts, :max_concurrent)
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc """
  Executa `fun` ocupando um slot do semáforo. Se não houver slot livre,
  aguarda na fila. O slot é sempre liberado, mesmo se `fun` lançar exceção.
  """
  def executar(fun) when is_function(fun, 0) do
    {:ok, ref} = GenServer.call(__MODULE__, :adquirir, :infinity)

    try do
      fun.()
    after
      GenServer.call(__MODULE__, {:liberar, ref}, :infinity)
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, ativos: %{}, fila: :queue.new()}}
  end

  @impl true
  def handle_call(:adquirir, {pid, _tag} = from, state) do
    if map_size(state.ativos) < state.max do
      # Há slot livre: concede imediatamente e monitora o cliente.
      ref = Process.monitor(pid)
      {:reply, {:ok, ref}, %{state | ativos: Map.put(state.ativos, ref, pid)}}
    else
      # Sem slot: o cliente fica bloqueado até que um slot seja liberado.
      {:noreply, %{state | fila: :queue.in(from, state.fila)}}
    end
  end

  def handle_call({:liberar, ref}, _from, state) do
    case Map.pop(state.ativos, ref) do
      {nil, _} ->
        {:reply, :ok, state}

      {_pid, ativos} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, promover(%{state | ativos: ativos})}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    # Cliente terminou sem liberar o slot (crash ou kill): recupera o slot.
    case Map.pop(state.ativos, ref) do
      {nil, _} -> {:noreply, state}
      {_pid, ativos} -> {:noreply, promover(%{state | ativos: ativos})}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # Concede slots a clientes da fila enquanto houver vagas disponíveis.
  defp promover(state) do
    cond do
      map_size(state.ativos) >= state.max ->
        state

      true ->
        case :queue.out(state.fila) do
          {:empty, _} ->
            state

          {{:value, {pid, _tag} = from}, fila} ->
            # Monitora antes de responder: se o cliente já morreu, o DOWN
            # chegará imediatamente e o slot será recuperado.
            ref = Process.monitor(pid)
            GenServer.reply(from, {:ok, ref})

            promover(%{
              state
              | ativos: Map.put(state.ativos, ref, pid),
                fila: fila
            })
        end
    end
  end
end

defmodule Estoque.Inventario do
  @moduledoc """
  GenServer que mantém o saldo de cada SKU e o histórico de operações.

  Todas as operações são executadas sequencialmente dentro deste processo,
  portanto leitura, validação e escrita ocorrem de forma atômica, evitando
  condições de corrida (ex.: dois clientes removendo o último item ao mesmo tempo).

  Estado:
    * `estoque`   - mapa `%{sku => saldo}`
    * `historico` - lista de operações, da mais recente para a mais antiga
    * `seq`       - contador monotônico de operações
  """

  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{estoque: %{}, historico: [], seq: 0}}
  end

  @impl true
  def handle_call({:adicionar, sku, qtd}, _from, state) do
    {resposta, state} = aplicar(state, :adicionar, sku, qtd)
    {:reply, resposta, state}
  end

  def handle_call({:remover, sku, qtd}, _from, state) do
    {resposta, state} = aplicar(state, :remover, sku, qtd)
    {:reply, resposta, state}
  end

  def handle_call({:consultar, sku}, _from, state) do
    {:reply, Map.get(state.estoque, sku, 0), state}
  end

  def handle_call({:historico, limite}, _from, state) do
    entradas =
      state.historico
      |> Enum.take(limite)
      |> Enum.reverse()

    {:reply, entradas, state}
  end

  # Adição válida: incrementa o saldo e registra no histórico.
  defp aplicar(state, :adicionar, sku, qtd) when is_integer(qtd) and qtd > 0 do
    novo = Map.get(state.estoque, sku, 0) + qtd
    state = %{state | estoque: Map.put(state.estoque, sku, novo)}
    {{:ok, novo}, registrar(state, :adicionar, sku, qtd, :ok)}
  end

  # Remoção válida: só ocorre se houver saldo suficiente.
  defp aplicar(state, :remover, sku, qtd) when is_integer(qtd) and qtd > 0 do
    atual = Map.get(state.estoque, sku, 0)

    if atual >= qtd do
      novo = atual - qtd
      state = %{state | estoque: Map.put(state.estoque, sku, novo)}
      {{:ok, novo}, registrar(state, :remover, sku, qtd, :ok)}
    else
      erro = {:error, :estoque_insuficiente}
      {erro, registrar(state, :remover, sku, qtd, erro)}
    end
  end

  # Quantidade inválida (não inteira ou não positiva): rejeita e registra a tentativa.
  defp aplicar(state, op, sku, qtd) do
    erro = {:error, :quantidade_invalida}
    {erro, registrar(state, op, sku, qtd, erro)}
  end

  # Registra cada operação (inclusive as rejeitadas) no histórico.
  defp registrar(state, op, sku, qtd, resultado) do
    seq = state.seq + 1

    entrada = %{
      seq: seq,
      em: DateTime.utc_now(),
      operacao: op,
      sku: sku,
      quantidade: qtd,
      resultado: resultado
    }

    %{state | seq: seq, historico: [entrada | state.historico]}
  end
end
