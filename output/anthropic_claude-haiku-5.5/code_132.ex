# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de estoque: operações periódicas, novas tentativas com backoff
# e um banco de dados fictício em memória.
#
# Execução: elixir inventory.exs
#
# Requisitos:
#   - Python não é usado; apenas a biblioteca padrão (GenServer, Agent, Logger, :rand).
#   - Ecto.Migration é opcional: o módulo de migração só é definido se Ecto estiver disponível.

defmodule Inventory.Retry do
  @moduledoc """
  Executa uma função de 0 aridade com novas tentativas apenas para erros transitórios.
  Usa backoff exponencial limitado com jitter para evitar tempestades de requisições.
  """
  require Logger

  @defaults [max_attempts: 4, base_delay_ms: 100, max_delay_ms: 2_000]

  @spec run((-> term()), keyword()) :: term()
  def run(fun, opts \\ []) when is_function(fun, 0) do
    opts = Keyword.merge(@defaults, opts)
    do_run(fun, 1, opts)
  end

  defp do_run(fun, attempt, opts) do
    case safe_call(fun) do
      {:error, reason} = error ->
        if transient?(reason) and attempt < opts[:max_attempts] do
          delay = backoff(attempt, opts)

          Logger.warning(
            "Tentativa #{attempt} falhou (#{inspect(reason)}). Nova tentativa em #{delay}ms."
          )

          Process.sleep(delay)
          do_run(fun, attempt + 1, opts)
        else
          error
        end

      other ->
        other
    end
  end

  # Exceções não são tratadas como transitórias: são defeitos de código.
  defp safe_call(fun) do
    fun.()
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end

  defp transient?(:timeout), do: true
  defp transient?(:busy), do: true
  defp transient?({:transient, _}), do: true
  defp transient?(_), do: false

  defp backoff(attempt, opts) do
    base = opts[:base_delay_ms] * Integer.pow(2, attempt - 1)
    capped = min(base, opts[:max_delay_ms])
    # Jitter de até 25% do atraso calculado.
    capped + :rand.uniform(max(div(capped, 4), 1)) - 1
  end
end

defmodule Inventory.FakeDB do
  @moduledoc """
  Banco de dados fictício em memória (Agent). Simula falhas transitórias
  (`{:error, :timeout}`) de acordo com `failure_rate`.

  Cada item tem o formato: `%{name: String.t(), quantity: integer(), min_quantity: integer()}`.
  """
  use Agent

  def start_link(opts \\ []) do
    state = %{
      items: Keyword.get(opts, :items, %{}),
      failure_rate: Keyword.get(opts, :failure_rate, 0.0)
    }

    Agent.start_link(fn -> state end, name: __MODULE__)
  end

  @doc "Retorna todos os itens do estoque."
  def list_items, do: transact(fn state -> {{:ok, state.items}, state} end)

  @doc """
  Ajusta a quantidade de um SKU em `delta` (positivo ou negativo).
  Não permite estoque negativo.
  """
  def adjust_quantity(sku, delta) do
    transact(fn state ->
      case Map.fetch(state.items, sku) do
        :error ->
          {{:error, :not_found}, state}

        {:ok, item} ->
          new_qty = item.quantity + delta

          if new_qty < 0 do
            {{:error, :insufficient_stock}, state}
          else
            items = Map.put(state.items, sku, %{item | quantity: new_qty})
            {{:ok, new_qty}, %{state | items: items}}
          end
      end
    end)
  end

  # Simula indisponibilidade transitória antes de executar a operação.
  # Como a falha ocorre antes de qualquer alteração, a repetição é segura aqui.
  defp transact(fun) do
    Agent.get_and_update(__MODULE__, fn state ->
      if :rand.uniform() < state.failure_rate do
        {{:error, :timeout}, state}
      else
        fun.(state)
      end
    end)
  end
end

defmodule Inventory.Stock do
  @moduledoc """
  Regras de domínio do estoque. Cada operação de acesso ao banco passa por `Inventory.Retry`.
  """
  alias Inventory.{FakeDB, Retry}

  @doc "Retorna os itens cuja quantidade está abaixo do mínimo."
  def low_stock_report do
    with {:ok, items} <- Retry.run(&FakeDB.list_items/0) do
      low =
        for {sku, %{quantity: q, min_quantity: m} = item} <- items, q < m do
          {sku, item.name, q, m}
        end

      {:ok, low}
    end
  end

  @doc """
  Reabastece itens abaixo do mínimo até o dobro do mínimo.
  Retorna a quantidade de itens reabastecidos com sucesso.
  """
  def auto_replenish do
    with {:ok, low} <- low_stock_report() do
      restocked =
        Enum.count(low, fn {sku, _name, q, m} ->
          match?({:ok, _}, Retry.run(fn -> FakeDB.adjust_quantity(sku, m * 2 - q) end))
        end)

      {:ok, restocked}
    end
  end

  @doc "Registra a venda de `qty` unidades de um SKU."
  def sell(sku, qty) when is_integer(qty) and qty > 0 do
    Retry.run(fn -> FakeDB.adjust_quantity(sku, -qty) end)
  end
end

defmodule Inventory.Scheduler do
  @moduledoc """
  Executa periodicamente as operações de estoque usando `Process.send_after/3`.
  """
  use GenServer
  require Logger

  alias Inventory.Stock

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, 5_000)
    schedule(interval)
    {:ok, %{interval: interval, runs: 0}}
  end

  @impl true
  def handle_info(:tick, state) do
    run_jobs()
    schedule(state.interval)
    {:noreply, %{state | runs: state.runs + 1}}
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)

  defp run_jobs do
    jobs = [
      {"relatório de baixo estoque", &Stock.low_stock_report/0},
      {"reabastecimento automático", &Stock.auto_replenish/0}
    ]

    Enum.each(jobs, fn {name, job} ->
      case job.() do
        {:ok, result} -> Logger.info("[#{name}] concluído: #{inspect(result)}")
        {:error, reason} -> Logger.error("[#{name}] falhou: #{inspect(reason)}")
      end
    end)
  end
end

# Migração Ecto opcional: definida apenas se o Ecto estiver disponível no projeto.
# Não é usada pelo FakeDB; serve como referência para um banco real.
if Code.ensure_loaded?(Ecto.Migration) do
  defmodule Inventory.Migrations.CreateInventoryItems do
    use Ecto.Migration

    def change do
      create table(:inventory_items) do
        add :sku, :string, null: false
        add :name, :string, null: false
        add :quantity, :integer, null: false, default: 0
        add :min_quantity, :integer, null: false, default: 0

        timestamps()
      end

      create unique_index(:inventory_items, [:sku])
      create constraint(:inventory_items, :quantity_non_negative, check: "quantity >= 0")
    end
  end
end

defmodule Inventory.Demo do
  @moduledoc "Demonstração: popula o banco fictício, simula vendas e roda o agendador."

  alias Inventory.{FakeDB, Scheduler, Stock}

  def run do
    {:ok, _} = FakeDB.start_link(items: seed(), failure_rate: 0.3)
    {:ok, _} = Scheduler.start_link(interval_ms: 1_000)

    Enum.each(1..5, fn _ ->
      IO.inspect(Stock.sell("SKU-1", 1), label: "venda SKU-1")
      Process.sleep(300)
    end)

    Process.sleep(4_000)
  end

  defp seed do
    %{
      "SKU-1" => %{name: "Camiseta", quantity: 10, min_quantity: 5},
      "SKU-2" => %{name: "Calça", quantity: 3, min_quantity: 4},
      "SKU-3" => %{name: "Tênis", quantity: 50, min_quantity: 10}
    }
  end
end

Inventory.Demo.run()
