# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task.Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoans do
  @moduledoc """
  Gerenciamento persistente de empréstimos de livros.
  """

  use Supervisor

  @task_supervisor BookLoans.TaskSupervisor
  @store BookLoans.Store
  @async_operations [:create, :get, :list, :update, :delete]

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file = Keyword.get(opts, :file, Path.join(File.cwd!(), "book_loans.dets"))

    children = [
      {Task.Supervisor, name: @task_supervisor},
      {@store, file: file}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(attrs) when is_map(attrs), do: GenServer.call(@store, {:create, attrs})
  def create(_attrs), do: {:error, :invalid_record}

  def get(id), do: GenServer.call(@store, {:get, id})
  def list, do: GenServer.call(@store, :list)

  def update(id, attrs) when is_map(attrs),
    do: GenServer.call(@store, {:update, id, attrs})

  def update(_id, _attrs), do: {:error, :invalid_record}

  def delete(id), do: GenServer.call(@store, {:delete, id})

  def async(operation, args \\ [])
      when operation in @async_operations and is_list(args) do
    Task.Supervisor.async_nolink(@task_supervisor, fn ->
      apply(__MODULE__, operation, args)
    end)
  end

  def async(_operation, _args), do: {:error, :unsupported_operation}

  def await(task, timeout \\ 5_000), do: Task.await(task, timeout)
end

defmodule BookLoans.Store do
  @moduledoc false

  use GenServer

  @table :book_loans_dets
  @next_id_key :book_loans_next_id

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file = Keyword.fetch!(opts, :file)
    File.mkdir_p!(Path.dirname(file))

    case :dets.open_file(@table, file: String.to_charlist(file), type: :set, repair: true) do
      {:ok, @table} ->
        max_id =
          :dets.foldl(
            fn
              {id, _record}, current when is_integer(id) -> max(id, current)
              {_key, _value}, current -> current
            end,
            0,
            @table
          )

        next_id =
          case :dets.lookup(@table, @next_id_key) do
            [{@next_id_key, id}] when is_integer(id) -> max(id, max_id + 1)
            _ -> max_id + 1
          end

        {:ok, %{next_id: next_id}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = state.next_id
    record = attrs |> Map.delete(:id) |> Map.put(:id, id)

    :ok = :dets.insert(@table, [{id, record}, {@next_id_key, id + 1}])
    :ok = :dets.sync(@table)

    {:reply, {:ok, record}, %{state | next_id: id + 1}}
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(@table, id) do
        [{^id, record}] -> {:ok, record}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    records =
      @table
      |> :dets.foldl(
        fn
          {id, record}, acc when is_integer(id) -> [record | acc]
          {_key, _value}, acc -> acc
        end,
        []
      )
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, records}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    case :dets.lookup(@table, id) do
      [{^id, record}] ->
        updated = record |> Map.merge(Map.delete(attrs, :id)) |> Map.put(:id, id)
        :ok = :dets.insert(@table, {id, updated})
        :ok = :dets.sync(@table)
        {:reply, {:ok, updated}, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case :dets.lookup(@table, id) do
      [{^id, record}] ->
        :ok = :dets.delete(@table, id)
        :ok = :dets.sync(@table)
        {:reply, {:ok, record}, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end
end
