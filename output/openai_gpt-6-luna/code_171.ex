# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Detectar encerramentos inesperados de processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoans do
  @moduledoc """
  Gerenciamento de empréstimos de livros com persistência em DETS.
  """

  def start_link(opts \\ []) do
    BookLoans.Supervisor.start_link(opts)
  end

  def create_loan(id, fields) when is_map(fields) do
    GenServer.call(BookLoans.Store, {:create, id, fields})
  end

  def create_loan(_id, _fields), do: {:error, :invalid_fields}

  def get_loan(id) do
    GenServer.call(BookLoans.Store, {:get, id})
  end

  def update_loan(id, fields) when is_map(fields) do
    GenServer.call(BookLoans.Store, {:update, id, fields})
  end

  def update_loan(_id, _fields), do: {:error, :invalid_fields}

  def delete_loan(id) do
    GenServer.call(BookLoans.Store, {:delete, id})
  end

  def list_loans do
    GenServer.call(BookLoans.Store, :list)
  end
end

defmodule BookLoans.Supervisor do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "book_loans.dets")

    children = [
      {BookLoans.Store, path: path},
      BookLoans.Watcher
    ]

    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: 10,
      max_seconds: 5
    )
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
    path = opts |> Keyword.fetch!(:path) |> to_string()
    directory = Path.dirname(path)

    with :ok <- File.mkdir_p(directory),
         {:ok, @table} <-
           :dets.open_file(@table,
             file: String.to_charlist(path),
             type: :set,
             repair: true
           ) do
      {:ok, %{}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, nil, _fields}, _from, state) do
    {:reply, {:error, :invalid_id}, state}
  end

  def handle_call({:create, id, fields}, _from, state) do
    case :dets.lookup(@table, id) do
      [] ->
        case persist({id, fields}) do
          :ok -> {:reply, {:ok, id}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      [_] ->
        {:reply, {:error, :already_exists}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case :dets.lookup(@table, id) do
        [{^id, fields}] -> {:ok, fields}
        [] -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, nil, _fields}, _from, state) do
    {:reply, {:error, :invalid_id}, state}
  end

  def handle_call({:update, id, fields}, _from, state) do
    case :dets.lookup(@table, id) do
      [{^id, current_fields}] ->
        case persist({id, Map.merge(current_fields, fields)}) do
          :ok -> {:reply, {:ok, id}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete, nil}, _from, state) do
    {:reply, {:error, :invalid_id}, state}
  end

  def handle_call({:delete, id}, _from, state) do
    case :dets.lookup(@table, id) do
      [_] ->
        case :dets.delete(@table, id) do
          :ok ->
            case :dets.sync(@table) do
              :ok -> {:reply, {:ok, id}, state}
              {:error, reason} -> {:reply, {:error, reason}, state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    loans =
      :dets.foldl(
        fn {id, fields}, acc -> [{id, fields} | acc] end,
        [],
        @table
      )

    {:reply, {:ok, loans}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp persist(record) do
    case :dets.insert(@table, record) do
      :ok -> :dets.sync(@table)
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule BookLoans.Watcher do
  use GenServer
  require Logger

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok)
  end

  @impl true
  def init(:ok) do
    {:ok, monitor_store(%{})}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state) do
    unless reason in [:normal, :shutdown] or match?({:shutdown, _}, reason) do
      Logger.warning("O processo de empréstimos foi encerrado inesperadamente: #{inspect(reason)}")
    end

    Process.send_after(self(), :find_store, 10)
    {:noreply, %{state | ref: nil, pid: nil}}
  end

  def handle_info(:find_store, state) do
    {:noreply, monitor_store(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp monitor_store(state) do
    case Process.whereis(BookLoans.Store) do
      nil ->
        Process.send_after(self(), :find_store, 10)
        state

      pid ->
        %{state | pid: pid, ref: Process.monitor(pid)}
    end
  end
end
