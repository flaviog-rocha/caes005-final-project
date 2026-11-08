# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# inventory.exs
# Execução: elixir inventory.exs
#
# Gerenciamento de estoque com:
#   * operações periódicas (ciclo por SKU e jobs agendados);
#   * "banco" fictício em memória, substituível por um adaptador Ecto;
#   * erros de validação separados de falhas operacionais;
#   * processamento independente por entidade (um processo por SKU).

defmodule Inventory.Error do
  @moduledoc """
  Classificação de erros:

    * `{:validation, motivos}` - a entrada é inválida ou viola uma regra de negócio.
      Não deve ser reexecutada: é descartada e registrada como rejeitada.
    * `{:operational, motivo}` - falha transitória de infraestrutura (ex.: banco
      indisponível). Pode ser reexecutada com backoff.
  """

  def validation(reasons), do: {:error, {:validation, List.wrap(reasons)}}
  def operational(reason), do: {:error, {:operational, reason}}

  def retryable?({:error, {:operational, _}}), do: true
  def retryable?(_), do: false
end

defmodule Inventory.Movement do
  @moduledoc "Movimentação de estoque com validação pura (sem efeitos colaterais)."

  @enforce_keys [:id, :sku, :type, :quantity]
  defstruct [:id, :sku, :type, :quantity]

  @types [:entry, :exit]

  def new(attrs) when is_map(attrs) do
    sku = Map.get(attrs, :sku)
    type = Map.get(attrs, :type)
    qty = Map.get(attrs, :quantity)

    errors =
      []
      |> check(is_binary(sku) and byte_size(sku) > 0, "sku deve ser uma string não vazia")
      |> check(type in @types, "type deve ser :entry ou :exit")
      |> check(is_integer(qty) and qty > 0, "quantity deve ser inteiro positivo")

    case errors do
      [] ->
        {:ok,
         %__MODULE__{
           id: System.unique_integer([:positive]),
           sku: sku,
           type: type,
           quantity: qty
         }}

      _ ->
        Inventory.Error.validation(Enum.reverse(errors))
    end
  end

  def new(_), do: Inventory.Error.validation("attrs deve ser um mapa")

  defp check(errors, true, _msg), do: errors
  defp check(errors, false, msg), do: [msg | errors]
end

defmodule Inventory.Store do
  @moduledoc """
  "Banco de dados" fictício em memória (Agent). Cada alteração é atômica via
  `Agent.get_and_update/2`. `fail_rate` simula indisponibilidade transitória.

  Em produção, este módulo poderia ser substituído por um adaptador baseado em
  Ecto (com migrations criando a tabela de produtos) sem alterar os workers,
  pois a interface pública (`apply_movement/1`, `snapshot/0`, `low_stock/1`)
  seria mantida.
  """
  use Agent

  def start_link(opts) do
    state = %{
      products: Keyword.get(opts, :products, %{}),
      fail_rate: Keyword.get(opts, :fail_rate, 0.0)
    }

    Agent.start_link(fn -> state end, name: __MODULE__)
  end

  def apply_movement(%Inventory.Movement{} = m) do
    Agent.get_and_update(__MODULE__, &do_apply(&1, m))
  end

  def snapshot, do: Agent.get(__MODULE__, & &1.products)

  def low_stock(threshold) do
    snapshot()
    |> Enum.filter(fn {_sku, p} -> p.quantity <= threshold end)
    |> Enum.map(fn {sku, p} -> {sku, p.quantity} end)
  end

  defp do_apply(state, m) do
    cond do
      # Falha operacional simulada (ex.: timeout do banco).
      :rand.uniform() < state.fail_rate ->
        {Inventory.Error.operational(:db_timeout), state}

      # Validação: produto inexistente.
      not Map.has_key?(state.products, m.sku) ->
        {Inventory.Error.validation("sku desconhecido: #{m.sku}"), state}

      true ->
        current = state.products[m.sku].quantity

        new_qty =
          if m.type == :entry, do: current + m.quantity, else: current - m.quantity

        # Validação: regra de negócio (estoque não pode ficar negativo).
        if new_qty < 0 do
          {Inventory.Error.validation(
             "estoque insuficiente para #{m.sku} (disponível: #{current})"
           ), state}
        else
          {{:ok, new_qty}, put_in(state, [:products, m.sku, :quantity], new_qty)}
        end
    end
  end
end

defmodule Inventory.SkuWorker do
  @moduledoc """
  Um processo por SKU (isolamento): cada entidade possui sua própria fila,
  seu próprio ciclo periódico e seu próprio backoff. Falhas ou lentidão em um
  SKU não afetam os demais.

  A validação ocorre de forma síncrona, em `enqueue/2`. Falhas operacionais
  ocorrem de forma assíncrona, durante o processamento periódico, e geram
  reexecução com backoff exponencial.
  """
  use GenServer

  @tick_ms 100
  @max_backoff_ms 5_000

  # API pública

  def start_link(sku), do: GenServer.start_link(__MODULE__, sku, name: via(sku))

  def enqueue(sku, attrs) do
    # Erros de validação retornam imediatamente ao chamador.
    with {:ok, movement} <- Inventory.Movement.new(Map.put(attrs, :sku, sku)) do
      {:ok, pid} = ensure_started(sku)
      GenServer.cast(pid, {:enqueue, movement})
      :ok
    end
  end

  def report(sku), do: GenServer.call(via(sku), :report)

  defp via(sku), do: {:via, Registry, {Inventory.Registry, sku}}

  defp ensure_started(sku) do
    case DynamicSupervisor.start_child(Inventory.WorkerSup, {__MODULE__, sku}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  # Callbacks do GenServer

  @impl true
  def init(sku) do
    schedule(@tick_ms)

    {:ok,
     %{
       sku: sku,
       queue: :queue.new(),
       rejected: [],
       backoff: @tick_ms,
       last_error: nil
     }}
  end

  @impl true
  def handle_cast({:enqueue, movement}, state) do
    {:noreply, %{state | queue: :queue.in(movement, state.queue)}}
  end

  @impl true
  def handle_call(:report, _from, state) do
    reply = %{
      pending: :queue.len(state.queue),
      rejected: Enum.reverse(state.rejected),
      last_error: state.last_error
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {state, status} = drain(state)
    delay = if status == :retry, do: state.backoff, else: @tick_ms
    schedule(delay)
    {:noreply, state}
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)

  # Processa a fila em ordem. Para ao encontrar falha operacional (preserva a
  # ordem das movimentações), mas descarta e registra erros de validação.
  defp drain(state) do
    case :queue.out(state.queue) do
      {:empty, _} ->
        {%{state | backoff: @tick_ms}, :ok}

      {{:value, movement}, rest} ->
        case Inventory.Store.apply_movement(movement) do
          {:ok, _qty} ->
            drain(%{state | queue: rest, last_error: nil})

          {:error, {:validation, reasons}} ->
            entry = %{movement: movement, reasons: reasons}
            drain(%{state | queue: rest, rejected: [entry | state.rejected]})

          {:error, {:operational, reason}} ->
            # Mantém o item na fila e aumenta o intervalo até a próxima tentativa.
            backoff = min(state.backoff * 2, @max_backoff_ms)
            {%{state | backoff: backoff, last_error: reason}, :retry}
        end
    end
  end
end

defmodule Inventory.Scheduler do
  @moduledoc """
  Executa jobs periódicos. Cada execução roda em um Task supervisionado e
  isolado: exceções são capturadas e registradas sem derrubar o scheduler.
  """
  use GenServer

  def start_link(jobs), do: GenServer.start_link(__MODULE__, jobs, name: __MODULE__)

  @impl true
  def init(jobs) do
    jobs = Map.new(jobs, fn {name, interval, fun} -> {name, {interval, fun}} end)

    for {name, {interval, _fun}} <- jobs do
      Process.send_after(self(), {:run, name}, interval)
    end

    {:ok, jobs}
  end

  @impl true
  def handle_info({:run, name}, jobs) do
    {interval, fun} = Map.fetch!(jobs, name)

    {:ok, _pid} = Task.Supervisor.start_child(Inventory.TaskSup, fn -> execute(name, fun) end)

    Process.send_after(self(), {:run, name}, interval)
    {:noreply, jobs}
  end

  defp execute(name, fun) do
    result =
      try do
        fun.()
      rescue
        e -> {:error, {:operational, Exception.message(e)}}
      end

    IO.puts("[job #{name}] #{inspect(result)}")
  end
end

defmodule Inventory.Root do
  @moduledoc "Árvore de supervisão da aplicação."
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Registry, keys: :unique, name: Inventory.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Inventory.WorkerSup},
      {Task.Supervisor, name: Inventory.TaskSup},
      {Inventory.Store, opts},
      {Inventory.Scheduler, Keyword.get(opts, :jobs, [])}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule Inventory.Demo do
  @moduledoc "Demonstração de uso."

  def run do
    products = %{
      "SKU-1" => %{name: "Teclado", quantity: 10},
      "SKU-2" => %{name: "Mouse", quantity: 2}
    }

    jobs = [
      {:low_stock, 1_000, fn -> {:ok, Inventory.Store.low_stock(3)} end}
    ]

    {:ok, _} = Inventory.Root.start_link(products: products, fail_rate: 0.3, jobs: jobs)

    IO.inspect(
      Inventory.SkuWorker.enqueue("SKU-1", %{type: :exit, quantity: 3}),
      label: "enqueue SKU-1 saída válida"
    )

    IO.inspect(
      Inventory.SkuWorker.enqueue("SKU-1", %{type: :exit, quantity: -1}),
      label: "enqueue SKU-1 quantidade inválida"
    )

    IO.inspect(
      Inventory.SkuWorker.enqueue("SKU-1", %{type: :exit, quantity: 50}),
      label: "enqueue SKU-1 saída grande"
    )

    IO.inspect(
      Inventory.SkuWorker.enqueue("SKU-2", %{type: :entry, quantity: 5}),
      label: "enqueue SKU-2 entrada"
    )

    IO.inspect(
      Inventory.SkuWorker.enqueue("SKU-9", %{type: :entry, quantity: 1}),
      label: "enqueue SKU-9 desconhecido"
    )

    # Aguarda o processamento periódico e as reexecuções com backoff.
    Process.sleep(3_000)

    IO.puts("\n--- Estoque final ---")
    IO.inspect(Inventory.Store.snapshot())

    IO.puts("\n--- Relatórios por SKU ---")

    for sku <- ["SKU-1", "SKU-2", "SKU-9"] do
      IO.inspect(Inventory.SkuWorker.report(sku), label: sku)
    end
  end
end

Inventory.Demo.run()
