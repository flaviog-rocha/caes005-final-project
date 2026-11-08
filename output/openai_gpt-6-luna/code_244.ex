# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Manter um histórico das operações realizadas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule PersistentCache do
  use GenServer

  @table __MODULE__

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def get(key, server \\ __MODULE__) do
    GenServer.call(server, {:get, key})
  end

  def put(key, value, server \\ __MODULE__) do
    GenServer.call(server, {:put, key, value})
  end

  def delete(key, server \\ __MODULE__) do
    GenServer.call(server, {:delete, key})
  end

  def clear(server \\ __MODULE__) do
    GenServer.call(server, :clear)
  end

  def history(server \\ __MODULE__) do
    GenServer.call(server, :history)
  end

  def stop(server \\ __MODULE__) do
    GenServer.stop(server)
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:file, "cache.dets")
      |> Path.expand()

    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      {:ok, table} ->
        stored_history =
          :dets.lookup(table, {:meta, :history})
          |> case do
            [{{:meta, :history}, entries}] when is_list(entries) -> entries
            _ -> []
          end

        {:ok, %{table: table, history: stored_history}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    result =
      case :dets.lookup(state.table, {:cache, key}) do
        [{{:cache, ^key}, value}] -> {:ok, value}
        [] -> :not_found
      end

    state = record(state, :get, key, result)
    {:reply, result, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    :ok = :dets.insert(state.table, {{:cache, key}, value})
    state = record(state, :put, key, nil)
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    existed = :dets.lookup(state.table, {:cache, key}) != []
    :ok = :dets.delete(state.table, {:cache, key})
    state = record(state, :delete, key, existed)
    {:reply, existed, state}
  end

  def handle_call(:clear, _from, state) do
    keys =
      :dets.foldl(
        fn
          {{:cache, key}, _value}, acc -> [key | acc]
          _entry, acc -> acc
        end,
        [],
        state.table
      )

    Enum.each(keys, &:dets.delete(state.table, {:cache, &1}))
    state = record(state, :clear, nil, length(keys))
    {:reply, :ok, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
  end

  defp record(state, operation, key, result) do
    entry = %{
      operation: operation,
      key: key,
      result: result,
      timestamp: System.system_time(:millisecond)
    }

    history = [entry | state.history]
    :ok = :dets.insert(state.table, {{:meta, :history}, history})
    :ok = :dets.sync(state.table)
    %{state | history: history}
  end
end
