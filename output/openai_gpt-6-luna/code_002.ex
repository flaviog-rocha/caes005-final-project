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

  def start_link(opts \\ []) do
    Supervisor.start_link(
      __MODULE__,
      opts,
      name: Keyword.get(opts, :name, BookLoans.Supervisor)
    )
  end

  def create(attrs) when is_map(attrs), do: GenServer.call(BookLoans.Store, {:create, attrs})
  def get(id), do: GenServer.call(BookLoans.Store, {:get, id})
  def list, do: GenServer.call(BookLoans.Store, :list)
  def update(id, attrs) when is_map(attrs), do: GenServer.call(BookLoans.Store, {:update, id, attrs})
  def delete(id), do: GenServer.call(BookLoans.Store, {:delete, id})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "book_loans.dets")

    children = [
      {Task.Supervisor, name: BookLoans.TaskSupervisor},
      {BookLoans.Store, path: path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule BookLoans.Store do
  use GenServer

  @table :book_loans_dets

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = opts |> Keyword.fetch!(:path) |> Path.expand()

    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             repair: true
           ) do
      {:ok, %{path: path}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    id = Map.get(attrs, :id) || new_id()

    if :dets.lookup(@table, id) != [] do
      {:reply, {:error, :already_exists}, state}
    else
      loan = Map.put(attrs, :id, id)

      case persist(fn -> :dets.insert(@table, {id, loan}) end) do
        :ok -> {:reply, {:ok, loan}, state}
        error -> {:reply, error, state}
      end
    end
  end

  def handle_call({:get, id}, _from, state) do
    result =
      case :dets.lookup(@table, id) do
        [{^id, loan}] -> {:ok, loan}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call(:list, _from, state) do
    loans =
      :dets.foldl(fn {_id, loan}, acc -> [loan | acc] end, [], @table)
      |> Enum.sort_by(&Map.get(&1, :id))

    {:reply, {:ok, loans}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    case :dets.lookup(@table, id) do
      [{^id, loan}] ->
        updated = loan |> Map.merge(attrs) |> Map.put(:id, id)

        case persist(fn -> :dets.insert(@table, {id, updated}) end) do
          :ok -> {:reply, {:ok, updated}, state}
          error -> {:reply, error, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case :dets.lookup(@table, id) do
      [{^id, loan}] ->
        case persist(fn -> :dets.delete(@table, id) end) do
          :ok -> {:reply, {:ok, loan}, state}
          error -> {:reply, error, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp persist(operation) do
    task =
      Task.Supervisor.async_nolink(BookLoans.TaskSupervisor, fn ->
        case operation.() do
          :ok -> :dets.sync(@table)
          error -> error
        end
      end)

    Task.await(task, :infinity)
  end

  defp new_id do
    timestamp = System.system_time(:microsecond)
    unique = :erlang.unique_integer([:positive, :monotonic])
    "#{timestamp}-#{unique}"
  end
end
