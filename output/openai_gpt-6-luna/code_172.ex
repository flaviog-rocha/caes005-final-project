# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LoanManager.Store do
  use GenServer

  @table :loan_records_dets

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list, do: GenServer.call(__MODULE__, :list)
  def update(id, attrs), do: GenServer.call(__MODULE__, {:update, id, attrs})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})

  @impl true
  def init(opts) do
    file =
      opts
      |> Keyword.get(:file, "loans.dets")
      |> to_string()
      |> String.to_charlist()

    case :dets.open_file(@table, file: file, type: :set, repair: true) do
      {:ok, table} -> {:ok, table}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, table) when is_map(attrs) do
    id = new_id(table)
    record = attrs |> Map.drop([:id, "id"]) |> Map.put(:id, id)

    :ok = :dets.insert(table, {id, record})
    :ok = :dets.sync(table)

    {:reply, {:ok, record}, table}
  end

  def handle_call({:create, _attrs}, _from, table) do
    {:reply, {:error, :invalid_record}, table}
  end

  def handle_call({:get, id}, _from, table) do
    reply =
      case :dets.lookup(table, id) do
        [{^id, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, table}
  end

  def handle_call(:list, _from, table) do
    records = :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], table)
    {:reply, records, table}
  end

  def handle_call({:update, id, attrs}, _from, table) when is_map(attrs) do
    reply =
      case :dets.lookup(table, id) do
        [{^id, record}] ->
          updated = record |> Map.merge(Map.drop(attrs, [:id, "id"])) |> Map.put(:id, id)
          :ok = :dets.insert(table, {id, updated})
          :ok = :dets.sync(table)
          {:ok, updated}

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, table}
  end

  def handle_call({:update, _id, _attrs}, _from, table) do
    {:reply, {:error, :invalid_record}, table}
  end

  def handle_call({:delete, id}, _from, table) do
    reply =
      case :dets.lookup(table, id) do
        [{^id, record}] ->
          :ok = :dets.delete(table, id)
          :ok = :dets.sync(table)
          {:ok, record}

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, table}
  end

  @impl true
  def terminate(_reason, table) do
    :dets.close(table)
  end

  defp new_id(table) do
    id = System.unique_integer([:positive, :monotonic])

    case :dets.lookup(table, id) do
      [] -> id
      _ -> new_id(table)
    end
  end
end

defmodule LoanManager do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    store_opts = Keyword.take(opts, [:file])

    children = [
      %{
        id: LoanManager.Store,
        start: {LoanManager.Store, :start_link, [store_opts]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
