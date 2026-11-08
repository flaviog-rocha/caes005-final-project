# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo autocontido: gerenciamento de estoque com tarefas periódicas,
# novas tentativas com backoff exponencial e um banco de dados fictício.
#
# Execução: elixir estoque.exs

# ---------------------------------------------------------------------------
# Banco de dados fictício (em memória, via Agent).
# Simula falhas transitórias (timeout) antes de cada operação.
# Opcionalmente, este módulo poderia ser substituído por um Repo do Ecto,
# com uma migration equivalente à tabela de produtos:
#
#   create table(:produtos, primary_key: false) do
#     add :sku, :string, primary_key: true
#     add :nome, :string, null: false
#     add :quantidade, :integer, null: false, default: 0
#     add :minimo, :integer, null: false, default: 0
#   end
# ---------------------------------------------------------------------------
defmodule Estoque.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória. Cada operação pode falhar de forma
  transitória com a probabilidade configurada em `@taxa_falha`.
  A falha ocorre ANTES de a escrita ser aplicada, evitando aplicações duplicadas.
  """
  use Agent

  @taxa_falha 0.3

  def start_link(opts \\ []) do
    dados =
      Keyword.get(opts, :dados, %{
        "SKU-001" => %{nome: "Camiseta", quantidade: 50, minimo: 10},
        "SKU-002" => %{nome: "Calça", quantidade: 4, minimo: 5},
        "SKU-003" => %{nome: "Tênis", quantidade: 0, minimo: 3}
      })

    Agent.start_link(fn -> dados end, name: __MODULE__)
  end

  @doc "Retorna o snapshot do estoque ou um erro transitório."
  def listar do
    with :ok <- simular_rede() do
      {:ok, Agent.get(__MODULE__, & &1)}
    end
  end

  @doc "Ajusta a quantidade de um SKU por `delta` (positivo ou negativo)."
  def ajustar_quantidade(sku, delta) do
    with :ok <- simular_rede() do
      Agent.get_and_update(__MODULE__, fn dados ->
        case Map.fetch(dados, sku) do
          {:ok, item} ->
            novo = item.quantidade + delta

            if novo < 0 do
              {{:error, :estoque_insuficiente}, dados}
            else
              {{:ok, novo}, Map.put(dados, sku, %{item | quantidade: novo})}
            end

          :error ->
            {{:error, :sku_inexistente}, dados}
        end
      end)
    end
  end

  # Simula instabilidade de rede/banco.
  defp simular_rede do
    if :rand.uniform() < @taxa_falha, do: {:error, :timeout}, else: :ok
  end
end

# ---------------------------------------------------------------------------
# Mecanismo de novas tentativas com backoff exponencial e jitter.
# Apenas erros transitórios são reexecutados; erros de domínio retornam de imediato.
# ---------------------------------------------------------------------------
defmodule Estoque.Retry do
  @moduledoc """
  Reexecuta uma função em caso de erros transitórios.

  Opções:
    * `:max_tentativas` - número máximo de tentativas (padrão: 5)
    * `:backoff_base_ms` - base do backoff exponencial em ms (padrão: 100)
  """
  require Logger

  @erros_transitorios [:timeout, :conexao_perdida]

  def executar(fun, opts \\ []) when is_function(fun, 0) do
    max = Keyword.get(opts, :max_tentativas, 5)
    base = Keyword.get(opts, :backoff_base_ms, 100)
    tentar(fun, 1, max, base)
  end

  defp tentar(fun, tentativa, max, base) do
    case fun.() do
      {:error, motivo} when motivo in @erros_transitorios and tentativa < max ->
        # Backoff exponencial: base * 2^(n-1) + jitter aleatório
        atraso = base * Integer.pow(2, tentativa - 1) + :rand.uniform(base)

        Logger.warning(
          "Falha transitória (#{motivo}) na tentativa #{tentativa}/#{max}. " <>
            "Nova tentativa em #{atraso}ms"
        )

        Process.sleep(atraso)
        tentar(fun, tentativa + 1, max, base)

      resultado ->
        resultado
    end
  end
end

# ---------------------------------------------------------------------------
# Tarefa periódica: verifica o estoque e repõe itens abaixo do mínimo.
# ---------------------------------------------------------------------------
defmodule Estoque.Monitor do
  @moduledoc """
  GenServer que executa a conciliação de estoque periodicamente.
  Cada ciclo lê o estoque (com retry) e repõe itens abaixo do mínimo.
  """
  use GenServer
  require Logger

  @intervalo_padrao_ms 2_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    intervalo = Keyword.get(opts, :intervalo_ms, @intervalo_padrao_ms)
    agendar(intervalo)
    {:ok, %{intervalo: intervalo, ciclos: 0}}
  end

  @impl true
  def handle_info(:tick, state) do
    ciclo = state.ciclos + 1
    Logger.info("Ciclo de conciliação ##{ciclo} iniciado")

    case Estoque.Retry.executar(&Estoque.FakeDB.listar/0) do
      {:ok, estoque} ->
        repor_itens_criticos(estoque)

      {:error, motivo} ->
        Logger.error("Ciclo ##{ciclo} abortado após retries: #{inspect(motivo)}")
    end

    agendar(state.intervalo)
    {:noreply, %{state | ciclos: ciclo}}
  end

  defp agendar(intervalo) do
    Process.send_after(self(), :tick, intervalo)
  end

  defp repor_itens_criticos(estoque) do
    estoque
    |> Enum.filter(fn {_sku, item} -> item.quantidade < item.minimo end)
    |> Enum.each(fn {sku, item} ->
      alvo = item.minimo * 2
      delta = alvo - item.quantidade

      Logger.warning("Estoque baixo em #{sku} (#{item.nome}): #{item.quantidade}/#{item.minimo}")

      resultado =
        Estoque.Retry.executar(fn ->
          Estoque.FakeDB.ajustar_quantidade(sku, delta)
        end)

      case resultado do
        {:ok, novo} ->
          Logger.info("Reposição de #{sku} concluída. Nova quantidade: #{novo}")

        {:error, motivo} ->
          Logger.error("Reposição de #{sku} falhou: #{inspect(motivo)}")
      end
    end)
  end
end

# ---------------------------------------------------------------------------
# Supervisão da aplicação.
# ---------------------------------------------------------------------------
defmodule Estoque.Application do
  @moduledoc "Árvore de supervisão do sistema de estoque."
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Estoque.FakeDB, []},
      {Estoque.Monitor, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

# ---------------------------------------------------------------------------
# Ponto de entrada: sobe o sistema, deixa as tarefas periódicas rodarem
# e imprime o estado final do estoque.
# ---------------------------------------------------------------------------
{:ok, _pid} = Estoque.Application.start_link(intervalo_ms: 1_000)

Process.sleep(6_000)

IO.puts("\nEstado final do estoque:")

Estoque.FakeDB
|> Agent.get(& &1)
|> Enum.each(fn {sku, item} ->
  IO.puts("  #{sku} - #{item.nome}: #{item.quantidade} (mínimo #{item.minimo})")
end)
