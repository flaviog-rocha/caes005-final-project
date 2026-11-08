# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de estoque com processos independentes por SKU, tarefas periódicas,
# banco fictício em memória e separação entre erros de validação e falhas operacionais.
# Executar com: elixir estoque.exs

# ---------------------------------------------------------------------------
# Banco de dados fictício (Agent em memória).
# Não depende de Ecto. Se desejado, uma migração Ecto correspondente seria:
#
#   create table(:itens, primary_key: false) do
#     add :sku, :string, primary_key: true
#     add :quantidade, :integer, null: false
#     add :minimo, :integer, null: false
#     add :lote_reposicao, :integer, null: false
#   end
#   create constraint(:itens, :quantidade_nao_negativa, check: "quantidade >= 0")
#
# Aqui apenas simulamos latência e falhas de infraestrutura.
# ---------------------------------------------------------------------------
defmodule Estoque.FakeDB do
  @moduledoc false
  use Agent

  # Probabilidade de uma operação falhar por motivo de infraestrutura (simulação).
  @taxa_falha 0.15

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def buscar(sku) do
    with :ok <- simular_infra() do
      Agent.get(__MODULE__, fn db ->
        case Map.fetch(db, sku) do
          {:ok, item} -> {:ok, item}
          :error -> {:error, {:validacao, :sku_inexistente}}
        end
      end)
    end
  end

  def criar(item) do
    with :ok <- simular_infra() do
      Agent.get_and_update(__MODULE__, fn db ->
        if Map.has_key?(db, item.sku) do
          {{:error, {:validacao, :sku_duplicado}}, db}
        else
          {:ok, Map.put(db, item.sku, item)}
        end
      end)
    end
  end

  def salvar(item) do
    with :ok <- simular_infra() do
      Agent.update(__MODULE__, &Map.put(&1, item.sku, item))
    end
  end

  def listar_skus do
    Agent.get(__MODULE__, &Map.keys/1)
  end

  # Simula latência e falhas operacionais (timeout, conexão perdida etc.).
  defp simular_infra do
    Process.sleep(Enum.random(1..20))

    if :rand.uniform() < @taxa_falha do
      {:error, {:operacional, :timeout}}
    else
      :ok
    end
  end
end

# ---------------------------------------------------------------------------
# Domínio: entidade, regras de negócio e construção dos erros de validação.
# Convenção de erros:
#   {:error, {:validacao, motivo}}   -> entrada/regra de negócio inválida (não retentar)
#   {:error, {:operacional, motivo}} -> falha de infraestrutura (pode retentar)
# ---------------------------------------------------------------------------
defmodule Estoque.Dominio do
  @moduledoc false

  defmodule Item do
    @moduledoc false
    defstruct [:sku, :quantidade, :minimo, :lote_reposicao]
  end

  def novo_item(attrs) do
    attrs
    |> then(&struct(Item, &1))
    |> validar_item()
  end

  def validar_item(%Item{} = i) do
    cond do
      not is_binary(i.sku) or i.sku == "" -> erro_validacao(:sku_invalido)
      not is_integer(i.quantidade) or i.quantidade < 0 -> erro_validacao(:quantidade_invalida)
      not is_integer(i.minimo) or i.minimo < 0 -> erro_validacao(:minimo_invalido)
      not is_integer(i.lote_reposicao) or i.lote_reposicao <= 0 -> erro_validacao(:lote_invalido)
      true -> {:ok, i}
    end
  end

  # Regra: o estoque nunca pode ficar negativo.
  def aplicar_delta(%Item{} = item, delta) when is_integer(delta) do
    novo = item.quantidade + delta

    if novo < 0 do
      erro_validacao(:estoque_insuficiente)
    else
      {:ok, %{item | quantidade: novo}}
    end
  end

  def aplicar_delta(_item, _delta), do: erro_validacao(:delta_invalido)

  def precisa_reposicao?(%Item{quantidade: q, minimo: m}), do: q < m

  def erro_validacao(motivo), do: {:error, {:validacao, motivo}}
end

# ---------------------------------------------------------------------------
# Operações de negócio (funções puras de orquestração sobre o banco).
# Não garantem serialização por si só; a serialização por SKU é feita pelo ItemWorker.
# ---------------------------------------------------------------------------
defmodule Estoque.Operacoes do
  @moduledoc false
  alias Estoque.{Dominio, FakeDB}

  def cadastrar(attrs) do
    with {:ok, item} <- Dominio.novo_item(attrs),
         :ok <- FakeDB.criar(item),
         {:ok, _pid} <-
           DynamicSupervisor.start_child(Estoque.ItemSup, {Estoque.ItemWorker, item.sku}) do
      {:ok, item.sku}
    end
  end

  def consultar(sku), do: FakeDB.buscar(sku)

  def movimentar(sku, delta) do
    with {:ok, item} <- FakeDB.buscar(sku),
         {:ok, novo} <- Dominio.aplicar_delta(item, delta),
         :ok <- FakeDB.salvar(novo) do
      {:ok, novo}
    end
  end

  # Operação periódica: repõe o lote se o estoque estiver abaixo do mínimo.
  def reabastecer_se_preciso(sku) do
    with {:ok, item} <- FakeDB.buscar(sku) do
      if Dominio.precisa_reposicao?(item) do
        movimentar(sku, item.lote_reposicao)
      else
        {:ok, :sem_necessidade}
      end
    end
  end

  # Operação de auditoria: verifica invariantes de um item.
  def conferir(sku) do
    with {:ok, item} <- FakeDB.buscar(sku),
         {:ok, _} <- Dominio.validar_item(item) do
      {:ok, sku}
    end
  end
end

# ---------------------------------------------------------------------------
# Um processo por SKU: entidades são processadas de forma independente.
# Falhas de um SKU não afetam os demais. Tarefas periódicas por item.
# Falhas operacionais são retentadas com backoff exponencial;
# erros de validação são registrados e não são retentados.
# ---------------------------------------------------------------------------
defmodule Estoque.ItemWorker do
  @moduledoc false
  use GenServer
  require Logger

  @intervalo_ms 500
  @max_tentativas 3

  def start_link(sku) do
    GenServer.start_link(__MODULE__, sku, name: via(sku))
  end

  def via(sku), do: {:via, Registry, {Estoque.Registro, sku}}

  def movimentar(sku, delta) do
    GenServer.call(via(sku), {:movimentar, delta})
  end

  @impl true
  def init(sku) do
    agendar(@intervalo_ms)
    {:ok, %{sku: sku, falhas: 0}}
  end

  @impl true
  def handle_call({:movimentar, delta}, _from, state) do
    {:reply, Estoque.Operacoes.movimentar(state.sku, delta), state}
  end

  @impl true
  def handle_info(:tick, state) do
    resultado = Estoque.Operacoes.reabastecer_se_preciso(state.sku)
    {:noreply, tratar_resultado(resultado, state)}
  end

  defp tratar_resultado({:ok, _}, state) do
    agendar(@intervalo_ms)
    %{state | falhas: 0}
  end

  # Erro de validação: não é transitório, registra e segue o cronograma normal.
  defp tratar_resultado({:error, {:validacao, motivo}}, state) do
    Logger.warning("[#{state.sku}] validação falhou: #{inspect(motivo)}")
    agendar(@intervalo_ms)
    %{state | falhas: 0}
  end

  # Falha operacional: retenta com backoff exponencial até o limite.
  defp tratar_resultado({:error, {:operacional, motivo}}, state) do
    falhas = state.falhas + 1

    if falhas <= @max_tentativas do
      atraso = @intervalo_ms * Integer.pow(2, falhas)
      Logger.error("[#{state.sku}] falha operacional #{inspect(motivo)}; retentando em #{atraso}ms")
      agendar(atraso)
      %{state | falhas: falhas}
    else
      Logger.error("[#{state.sku}] limite de tentativas atingido; aguardando próximo ciclo")
      agendar(@intervalo_ms)
      %{state | falhas: 0}
    end
  end

  defp agendar(ms), do: Process.send_after(self(), :tick, ms)
end

# ---------------------------------------------------------------------------
# Tarefa periódica global: audita todos os SKUs concorrentemente.
# Cada verificação roda em uma Task isolada; falhas são contabilizadas, não propagadas.
# ---------------------------------------------------------------------------
defmodule Estoque.Auditoria do
  @moduledoc false
  use GenServer
  require Logger

  @intervalo_ms 2_000

  def start_link(_opts), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    agendar()
    {:ok, nil}
  end

  @impl true
  def handle_info(:varrer, state) do
    resumo =
      Estoque.FakeDB.listar_skus()
      |> then(fn skus ->
        Task.Supervisor.async_stream_nolink(
          Estoque.TarefasSup,
          skus,
          &Estoque.Operacoes.conferir/1,
          max_concurrency: 4,
          timeout: 1_000,
          on_timeout: :kill_task
        )
      end)
      |> Enum.reduce(%{ok: 0, validacao: 0, operacional: 0, falha_task: 0}, &classificar/2)

    Logger.info("Auditoria: #{inspect(resumo)}")
    agendar()
    {:noreply, state}
  end

  defp classificar({:ok, {:ok, _}}, acc), do: Map.update!(acc, :ok, &(&1 + 1))
  defp classificar({:ok, {:error, {:validacao, _}}}, acc), do: Map.update!(acc, :validacao, &(&1 + 1))
  defp classificar({:ok, {:error, {:operacional, _}}}, acc), do: Map.update!(acc, :operacional, &(&1 + 1))
  defp classificar({:exit, _}, acc), do: Map.update!(acc, :falha_task, &(&1 + 1))

  defp agendar, do: Process.send_after(self(), :varrer, @intervalo_ms)
end

# ---------------------------------------------------------------------------
# Árvore de supervisão e demonstração.
# ---------------------------------------------------------------------------
defmodule Estoque.Aplicacao do
  @moduledoc false

  def iniciar do
    children = [
      Estoque.FakeDB,
      {Registry, keys: :unique, name: Estoque.Registro},
      {DynamicSupervisor, strategy: :one_for_one, name: Estoque.ItemSup},
      {Task.Supervisor, name: Estoque.TarefasSup},
      Estoque.Auditoria
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Estoque.Raiz)
  end
end

defmodule Estoque.Demo do
  @moduledoc false

  def executar do
    {:ok, _} = Estoque.Aplicacao.iniciar()

    # Cadastros válidos (cada SKU ganha seu próprio processo)
    IO.inspect(Estoque.Operacoes.cadastrar(%{sku: "CAM-01", quantidade: 2, minimo: 5, lote_reposicao: 10}), label: "cadastro CAM-01")
    IO.inspect(Estoque.Operacoes.cadastrar(%{sku: "BOL-07", quantidade: 20, minimo: 5, lote_reposicao: 10}), label: "cadastro BOL-07")

    # Erros de validação (não retentados)
    IO.inspect(Estoque.Operacoes.cadastrar(%{sku: "CAM-01", quantidade: 1, minimo: 1, lote_reposicao: 1}), label: "duplicado")
    IO.inspect(Estoque.Operacoes.cadastrar(%{sku: "", quantidade: 1, minimo: 1, lote_reposicao: 1}), label: "sku inválido")
    IO.inspect(Estoque.ItemWorker.movimentar("BOL-07", -50), label: "estoque insuficiente")

    # Movimentação válida via worker do SKU
    IO.inspect(Estoque.ItemWorker.movimentar("BOL-07", -15), label: "saída BOL-07")

    # Deixa as tarefas periódicas (reposição e auditoria) rodarem
    Process.sleep(3_000)

    IO.inspect(Estoque.Operacoes.consultar("CAM-01"), label: "CAM-01 após reposição periódica")
    IO.inspect(Estoque.Operacoes.consultar("BOL-07"), label: "BOL-07 final")
  end
end

Estoque.Demo.executar()
