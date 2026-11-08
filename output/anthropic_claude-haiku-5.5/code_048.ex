# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir a execução de operações simultâneas.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de estoque com OTP (arquivo único, somente biblioteca padrão)
#
# Configuração do projeto (mix.exs):
#   def application do
#     [mod: {Stock.Application, []}, extra_applications: [:logger]]
#   end
#
# Configuração opcional (config/config.exs):
#   config :stock, db_path: "priv/stock_data.dets"
#
# Exemplo de uso (iex -S mix):
#   Stock.Operations.operations()
#   #=> [:add, :remove]
#   Stock.Entity.execute("sku-1", "op-001", :add, %{amount: 10})
#   #=> {:ok, 10}
#   Stock.Entity.execute("sku-1", "op-001", :add, %{amount: 10})
#   #=> {:duplicate, {:ok, 10}}        # operação repetida não é reexecutada
#   Stock.Entity.execute("sku-1", "op-002", :remove, %{amount: 3})
#   #=> {:ok, 7}
#   Stock.Entity.quantity("sku-1")
#   #=> 7

defmodule Stock.Macros do
  @moduledoc """
  Macro `defop/2`: declara uma operação de estoque.

  Gera uma cláusula de `apply_op/3` e registra o nome da operação no
  atributo acumulado `@stock_ops`. O corpo tem acesso às variáveis `state`
  (estado da entidade) e `args` (mapa de argumentos) e deve retornar
  `{:ok, novo_estado, valor}` ou `{:error, motivo}`.
  """

  defmacro defop(name, do: body) do
    quote do
      # Registra o nome da operação em tempo de compilação
      Module.put_attribute(__MODULE__, :stock_ops, unquote(name))

      def apply_op(var!(state), unquote(name), var!(args)) do
        unquote(body)
      end
    end
  end
end

defmodule Stock.Operations do
  @moduledoc """
  Operações puras sobre o estado de uma entidade. Não possuem efeitos
  colaterais: a persistência e a idempotência são tratadas por `Stock.Entity`.
  """

  import Stock.Macros, only: [defop: 2]

  # Atributo acumulado usado pela macro `defop` para listar as operações
  Module.register_attribute(__MODULE__, :stock_ops, accumulate: true)

  defop :add do
    amount = Map.get(args, :amount)

    if is_integer(amount) and amount > 0 do
      novo = state.qty + amount
      {:ok, %{state | qty: novo}, novo}
    else
      {:error, :invalid_amount}
    end
  end

  defop :remove do
    amount = Map.get(args, :amount)

    cond do
      not (is_integer(amount) and amount > 0) ->
        {:error, :invalid_amount}

      amount > state.qty ->
        {:error, :insufficient_stock}

      true ->
        novo = state.qty - amount
        {:ok, %{state | qty: novo}, novo}
    end
  end

  # Operação desconhecida: cláusula de fallback (deve permanecer após as defop)
  def apply_op(_state, _name, _args), do: {:error, :unknown_operation}

  @doc "Lista as operações declaradas com `defop`."
  def operations, do: Enum.reverse(@stock_ops)
end

defmodule Stock.Storage do
  @moduledoc """
  Dono da tabela DETS. Mantém os dados em disco para recuperação após
  reinicializações. A tabela é compartilhada pelos processos de entidade,
  que são serializados internamente pelo próprio DETS.
  """

  use GenServer

  @table :stock_store

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Nome da tabela DETS."
  def table, do: @table

  @impl true
  def init(opts) do
    default_path = Application.get_env(:stock, :db_path, "stock_data.dets")
    path = Keyword.get(opts, :path, default_path)

    # DETS repara automaticamente arquivos inconsistentes ao abrir
    {:ok, @table} = :dets.open_file(@table, file: String.to_charlist(path), type: :set)
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end
end

defmodule Stock.Entity do
  @moduledoc """
  Processo de uma entidade (ex.: um SKU). Cada entidade tem seu próprio
  processo, registrado via `Registry` e supervisionado por um
  `DynamicSupervisor`.

  - Entidades diferentes são processadas em paralelo (processos distintos).
  - Operações sobre a mesma entidade são serializadas pelo GenServer.
  - Cada operação possui um `op_id`; se já foi aplicada, o resultado
    original é devolvido sem reexecução (idempotência).
  - O estado e os `op_id` aplicados são gravados no DETS a cada operação,
    na mesma escrita, o que garante atomicidade entre estado e registro.
  """

  use GenServer, restart: :transient

  alias Stock.{Operations, Storage}

  @supervisor Stock.EntitySupervisor

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: via(id))
  end

  @doc """
  Executa uma operação na entidade `id`. `op_id` identifica a operação
  de forma única dentro da entidade.
  """
  def execute(id, op_id, op_name, args) do
    ensure_started(id)
    GenServer.call(via(id), {:execute, op_id, op_name, args})
  end

  @doc "Retorna a quantidade atual da entidade."
  def quantity(id) do
    ensure_started(id)
    GenServer.call(via(id), :quantity)
  end

  defp via(id), do: {:via, Registry, {Stock.Registry, id}}

  # Inicia o processo da entidade sob o DynamicSupervisor, se necessário.
  # Se já existir, o Registry impede duplicação.
  defp ensure_started(id) do
    case DynamicSupervisor.start_child(@supervisor, {__MODULE__, id}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  @impl true
  def init(id) do
    {:ok, load(id)}
  end

  @impl true
  def handle_call({:execute, op_id, op_name, args}, _from, state) do
    case Map.fetch(state.applied, op_id) do
      {:ok, result} ->
        # Idempotência: operação já aplicada, nada é reexecutado
        {:reply, {:duplicate, result}, state}

      :error ->
        {result, new_state} = run(state, op_id, op_name, args)
        {:reply, result, new_state}
    end
  end

  def handle_call(:quantity, _from, state) do
    {:reply, state.qty, state}
  end

  defp run(state, op_id, op_name, args) do
    # Falhas também são registradas: a mesma tentativa sempre recebe
    # o mesmo resultado, mesmo após reinicialização.
    {result, base} =
      case Operations.apply_op(state, op_name, args) do
        {:ok, new_state, value} -> {{:ok, value}, new_state}
        {:error, reason} -> {{:error, reason}, state}
      end

    new_state = %{base | applied: Map.put(base.applied, op_id, result)}
    persist(new_state)
    {result, new_state}
  end

  defp persist(state) do
    table = Storage.table()
    :ok = :dets.insert(table, {state.id, state})
    :ok = :dets.sync(table)
  end

  defp load(id) do
    case :dets.lookup(Storage.table(), id) do
      [{^id, state}] -> state
      [] -> %{id: id, qty: 0, applied: %{}}
    end
  end
end

defmodule Stock.Application do
  @moduledoc """
  Árvore de supervisão. `rest_for_one` garante que, se o armazenamento
  reiniciar, os processos dependentes também sejam reiniciados e
  recarreguem o estado do DETS.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Stock.Storage,
      {Registry, keys: :unique, name: Stock.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Stock.EntitySupervisor}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: Stock.Supervisor)
  end
end
