# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de pedidos de um e-commerce, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Implementar expiração automática de registros.
# 3. Permitir comunicação entre diferentes processos.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# pedidos.exs
# Execução: elixir pedidos.exs

# ---------------------------------------------------------------------------
# Domínio: entidade Pedido e regras de transição de status (funções puras)
# ---------------------------------------------------------------------------
defmodule Pedidos.Pedido do
  @moduledoc "Entidade de domínio e regras de transição de status."

  @enforce_keys [:id, :cliente_id, :itens, :total_centavos, :status, :criado_em, :expira_em]
  defstruct [:id, :cliente_id, :itens, :total_centavos, :status, :criado_em, :expira_em]

  # Mapa de transições permitidas
  @transicoes %{
    aguardando_pagamento: [:pago, :cancelado, :expirado],
    pago: [],
    cancelado: [],
    expirado: []
  }

  @doc "Aplica uma transição de status, validando-a contra as regras do domínio."
  def transicionar(%__MODULE__{status: atual} = pedido, novo) do
    if novo in Map.fetch!(@transicoes, atual) do
      {:ok, %{pedido | status: novo}}
    else
      {:error, {:transicao_invalida, atual, novo}}
    end
  end

  @doc "Indica se o pedido deve expirar no instante `agora` (monotonic ms)."
  def expirado?(%__MODULE__{status: :aguardando_pagamento, expira_em: expira}, agora),
    do: agora >= expira

  def expirado?(_pedido, _agora), do: false
end

# ---------------------------------------------------------------------------
# Validação de dados de entrada (antes de qualquer operação)
# ---------------------------------------------------------------------------
defmodule Pedidos.Validador do
  @moduledoc "Valida e normaliza os dados recebidos."

  @doc "Retorna {:ok, dados_normalizados} ou {:error, lista_de_erros}."
  def validar(params) when is_map(params) do
    erros = erros_cliente(params) ++ erros_itens(params)

    if erros == [],
      do: {:ok, normalizar(params)},
      else: {:error, erros}
  end

  def validar(_), do: {:error, [{:params, "deve ser um mapa"}]}

  defp erros_cliente(params) do
    cliente = Map.get(params, :cliente_id)

    checar([], is_binary(cliente) and cliente != "", {:cliente_id, "deve ser string não vazia"})
  end

  defp erros_itens(params) do
    case Map.get(params, :itens) do
      [_ | _] = itens ->
        itens
        |> Enum.with_index()
        |> Enum.flat_map(fn {item, i} -> erros_item(item, i) end)

      _ ->
        [{:itens, "deve ser uma lista não vazia"}]
    end
  end

  defp erros_item(%{sku: sku, quantidade: qtd, preco_centavos: preco}, i) do
    []
    |> checar(is_binary(sku) and sku != "", {i, :sku, "deve ser string não vazia"})
    |> checar(is_integer(qtd) and qtd > 0, {i, :quantidade, "deve ser inteiro positivo"})
    |> checar(is_integer(preco) and preco >= 0, {i, :preco_centavos, "deve ser inteiro não negativo"})
  end

  defp erros_item(_item, i), do: [{i, "formato inválido"}]

  defp checar(erros, true, _erro), do: erros
  defp checar(erros, false, erro), do: erros ++ [erro]

  defp normalizar(params) do
    %{
      cliente_id: params.cliente_id,
      itens: Enum.map(params.itens, &Map.take(&1, [:sku, :quantidade, :preco_centavos]))
    }
  end
end

# ---------------------------------------------------------------------------
# Novas tentativas com backoff exponencial (apenas para falhas transitórias)
# ---------------------------------------------------------------------------
defmodule Pedidos.Retry do
  @moduledoc "Reexecuta operações em caso de falhas temporárias."

  @transitorios [:indisponivel, :timeout]

  @doc """
  Opções:
    * `:tentativas` - número máximo de tentativas (padrão 3)
    * `:atraso_base_ms` - atraso inicial do backoff exponencial (padrão 50)
  """
  def executar(fun, opts \\ []) when is_function(fun, 0) do
    tentativas = Keyword.get(opts, :tentativas, 3)
    atraso = Keyword.get(opts, :atraso_base_ms, 50)
    do_executar(fun, 1, tentativas, atraso)
  end

  defp do_executar(fun, n, max, atraso) do
    case fun.() do
      {:error, motivo} when motivo in @transitorios and n < max ->
        Process.sleep(atraso * Integer.pow(2, n - 1))
        do_executar(fun, n + 1, max, atraso)

      resultado ->
        resultado
    end
  end
end

# ---------------------------------------------------------------------------
# Comunicação entre processos: pub/sub baseado em Registry
# ---------------------------------------------------------------------------
defmodule Pedidos.Eventos do
  @moduledoc "Publicação e assinatura de eventos entre processos."

  @registro Pedidos.Registro

  def assinar(topico), do: Registry.register(@registro, topico, nil)

  def publicar(topico, evento) do
    Registry.dispatch(@registro, topico, fn assinantes ->
      for {pid, _valor} <- assinantes, do: send(pid, {:evento, topico, evento})
    end)

    :ok
  end
end

# ---------------------------------------------------------------------------
# Banco de dados fictício (GenServer com dados em memória)
# Simula falhas temporárias nas escritas para demonstrar as novas tentativas.
# ---------------------------------------------------------------------------
defmodule Pedidos.BancoFalso do
  use GenServer

  alias Pedidos.Pedido

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def inserir(pedido), do: GenServer.call(__MODULE__, {:inserir, pedido})
  def atualizar(id, fun), do: GenServer.call(__MODULE__, {:atualizar, id, fun})
  def buscar(id), do: GenServer.call(__MODULE__, {:buscar, id})
  def expirar_vencidos(agora), do: GenServer.call(__MODULE__, {:expirar_vencidos, agora})

  @impl true
  def init(opts) do
    {:ok, %{pedidos: %{}, falhas_restantes: Keyword.get(opts, :falhas_simuladas, 0)}}
  end

  @impl true
  # Falhas simuladas (temporárias) nas escritas
  def handle_call({:inserir, _pedido}, _from, %{falhas_restantes: n} = state) when n > 0 do
    {:reply, {:error, :indisponivel}, %{state | falhas_restantes: n - 1}}
  end

  def handle_call({:inserir, pedido}, _from, state) do
    {:reply, :ok, put_pedido(state, pedido)}
  end

  def handle_call({:atualizar, _id, _fun}, _from, %{falhas_restantes: n} = state) when n > 0 do
    {:reply, {:error, :indisponivel}, %{state | falhas_restantes: n - 1}}
  end

  def handle_call({:atualizar, id, fun}, _from, state) do
    case Map.fetch(state.pedidos, id) do
      :error ->
        {:reply, {:error, :nao_encontrado}, state}

      {:ok, pedido} ->
        case fun.(pedido) do
          {:ok, novo} -> {:reply, {:ok, novo}, put_pedido(state, novo)}
          {:error, _} = erro -> {:reply, erro, state}
        end
    end
  end

  def handle_call({:buscar, id}, _from, state) do
    {:reply, Map.fetch(state.pedidos, id) |> then(fn
      {:ok, p} -> {:ok, p}
      :error -> {:error, :nao_encontrado}
    end), state}
  end

  def handle_call({:expirar_vencidos, agora}, _from, state) do
    {ids, pedidos} =
      Enum.reduce(state.pedidos, {[], %{}}, fn {id, pedido}, {ids, acc} ->
        if Pedido.expirado?(pedido, agora) do
          {:ok, expirado} = Pedido.transicionar(pedido, :expirado)
          {[id | ids], Map.put(acc, id, expirado)}
        else
          {ids, Map.put(acc, id, pedido)}
        end
      end)

    {:reply, ids, %{state | pedidos: pedidos}}
  end

  defp put_pedido(state, pedido), do: %{state | pedidos: Map.put(state.pedidos, pedido.id, pedido)}
end

# ---------------------------------------------------------------------------
# Expiração automática: varredura periódica dos registros vencidos
# ---------------------------------------------------------------------------
defmodule Pedidos.Expirador do
  use GenServer

  alias Pedidos.{BancoFalso, Eventos}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :intervalo_ms, 1_000), name: __MODULE__)
  end

  @impl true
  def init(intervalo) do
    agendar(intervalo)
    {:ok, intervalo}
  end

  @impl true
  def handle_info(:varrer, intervalo) do
    agora = System.monotonic_time(:millisecond)

    for id <- BancoFalso.expirar_vencidos(agora) do
      Eventos.publicar(:pedido, {:mudou_status, id, :expirado})
    end

    agendar(intervalo)
    {:noreply, intervalo}
  end

  defp agendar(intervalo), do: Process.send_after(self(), :varrer, intervalo)
end

# ---------------------------------------------------------------------------
# Auditoria: processo assinante que recebe eventos de outros processos
# ---------------------------------------------------------------------------
defmodule Pedidos.Auditoria do
  use GenServer

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def historico, do: GenServer.call(__MODULE__, :historico)

  @impl true
  def init(:ok) do
    {:ok, _} = Pedidos.Eventos.assinar(:pedido)
    {:ok, []}
  end

  @impl true
  def handle_info({:evento, _topico, evento}, eventos), do: {:noreply, [evento | eventos]}

  @impl true
  def handle_call(:historico, _from, eventos), do: {:reply, Enum.reverse(eventos), eventos}
end

# ---------------------------------------------------------------------------
# API pública do domínio de pedidos
# ---------------------------------------------------------------------------
defmodule Pedidos do
  @moduledoc "Gerenciamento de pedidos de um e-commerce."

  alias Pedidos.{BancoFalso, Eventos, Pedido, Retry, Validador}

  @ttl_padrao_ms 30 * 60 * 1000
  @opcoes_retry [tentativas: 3, atraso_base_ms: 20]

  @doc "Valida e cria um pedido. Opções: `:ttl_ms`."
  def criar(params, opts \\ []) do
    ttl = Keyword.get(opts, :ttl_ms, @ttl_padrao_ms)

    with {:ok, dados} <- Validador.validar(params),
         pedido = montar(dados, ttl),
         :ok <- Retry.executar(fn -> BancoFalso.inserir(pedido) end, @opcoes_retry) do
      Eventos.publicar(:pedido, {:mudou_status, pedido.id, :aguardando_pagamento})
      {:ok, pedido}
    end
  end

  @doc "Marca o pedido como pago."
  def pagar(id), do: transicionar(id, :pago)

  @doc "Cancela o pedido."
  def cancelar(id), do: transicionar(id, :cancelado)

  @doc "Consulta um pedido pelo id."
  def consultar(id) when is_binary(id), do: BancoFalso.buscar(id)
  def consultar(_id), do: {:error, :id_invalido}

  defp transicionar(id, novo) when is_binary(id) do
    operacao = fn -> BancoFalso.atualizar(id, &Pedido.transicionar(&1, novo)) end

    with {:ok, pedido} <- Retry.executar(operacao, @opcoes_retry) do
      Eventos.publicar(:pedido, {:mudou_status, id, novo})
      {:ok, pedido}
    end
  end

  defp transicionar(_id, _novo), do: {:error, :id_invalido}

  defp montar(%{cliente_id: cliente, itens: itens}, ttl) do
    agora = System.monotonic_time(:millisecond)

    %Pedido{
      id: "PED-" <> Integer.to_string(System.unique_integer([:positive])),
      cliente_id: cliente,
      itens: itens,
      total_centavos: Enum.sum(for i <- itens, do: i.quantidade * i.preco_centavos),
      status: :aguardando_pagamento,
      criado_em: agora,
      expira_em: agora + ttl
    }
  end
end

# ---------------------------------------------------------------------------
# Supervisor raiz (estratégia :one_for_one)
# ---------------------------------------------------------------------------
defmodule Pedidos.Raiz do
  use Supervisor

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      # Registry deve iniciar antes dos processos que se comunicam por eventos
      {Registry, keys: :duplicate, name: Pedidos.Registro},
      {Pedidos.Auditoria, []},
      {Pedidos.BancoFalso, falhas_simuladas: Keyword.get(opts, :falhas_simuladas, 0)},
      {Pedidos.Expirador, intervalo_ms: Keyword.get(opts, :intervalo_expiracao_ms, 1_000)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ---------------------------------------------------------------------------
# Demonstração
# ---------------------------------------------------------------------------
{:ok, _sup} = Pedidos.Raiz.start_link(falhas_simuladas: 2, intervalo_expiracao_ms: 100)

# Validação: dados inválidos são rejeitados antes de qualquer operação
IO.inspect(Pedidos.criar(%{cliente_id: "", itens: []}), label: "validação")

# Criação com falhas temporárias simuladas (retry com backoff)
{:ok, p1} =
  Pedidos.criar(%{
    cliente_id: "cli-1",
    itens: [%{sku: "A1", quantidade: 2, preco_centavos: 1500}]
  })

# Pedido com TTL curto para demonstrar expiração automática
{:ok, p2} =
  Pedidos.criar(
    %{cliente_id: "cli-2", itens: [%{sku: "B7", quantidade: 1, preco_centavos: 9900}]},
    ttl_ms: 300
  )

{:ok, pago} = Pedidos.pagar(p1.id)
IO.inspect(pago.status, label: "p1 após pagamento")

Process.sleep(600)

IO.inspect(elem(Pedidos.consultar(p2.id), 1).status, label: "p2 após expiração")
IO.inspect(Pedidos.pagar(p2.id), label: "pagar pedido expirado")
IO.inspect(Pedidos.Auditoria.historico(), label: "histórico de eventos")
