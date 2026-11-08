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

  @version 1
  @default_path "cache_store.term"

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  def put(key, value, server \\ __MODULE__) do
    GenServer.call(server, {:put, key, value})
  end

  def get(key, server \\ __MODULE__) do
    GenServer.call(server, {:get, key})
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

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)

    case load(path) do
      {:ok, data, history_rev} ->
        {:ok, %{path: path, data: data, history_rev: history_rev}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    entry = operation(:put, key)
    {state, reply} = commit(state, Map.put(state.data, key, value), entry, :ok)
    {:reply, reply, state}
  end

  def handle_call({:get, key}, _from, state) do
    result = Map.fetch(state.data, key)
    outcome = if match?({:ok, _}, result), do: :hit, else: :miss
    entry = operation(:get, key, outcome)

    reply =
      case result do
        {:ok, value} -> {:ok, value}
        :error -> :not_found
      end

    {state, reply} = commit(state, state.data, entry, reply)
    {:reply, reply, state}
  end

  def handle_call({:delete, key}, _from, state) do
    entry = operation(:delete, key)
    {state, reply} = commit(state, Map.delete(state.data, key), entry, :ok)
    {:reply, reply, state}
  end

  def handle_call(:clear, _from, state) do
    entry = %{operation: :clear, timestamp: now()}
    {state, reply} = commit(state, %{}, entry, :ok)
    {:reply, reply, state}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history_rev), state}
  end

  defp commit(state, data, entry, reply) do
    next = %{state | data: data, history_rev: [entry | state.history_rev]}

    case persist(next) do
      :ok -> {next, reply}
      {:error, reason} -> {state, {:error, {:persistence, reason}}}
    end
  end

  defp operation(name, key, outcome \\ nil) do
    entry = %{operation: name, key: key, timestamp: now()}
    if is_nil(outcome), do: entry, else: Map.put(entry, :outcome, outcome)
  end

  defp now, do: System.system_time(:millisecond)

  defp load(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          case :erlang.binary_to_term(binary) do
            %{version: @version, data: data, history_rev: history_rev}
            when is_map(data) and is_list(history_rev) ->
              {:ok, data, history_rev}

            _ ->
              {:error, :invalid_cache_file}
          end
        rescue
          _ -> {:error, :invalid_cache_file}
        end

      {:error, :enoent} ->
        {:ok, %{}, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist(state) do
    contents =
      :erlang.term_to_binary(%{
        version: @version,
        data: state.data,
        history_rev: state.history_rev
      })

    directory = Path.dirname(state.path)
    temporary_path = state.path <> ".tmp." <> Integer.to_string(:erlang.unique_integer([:positive]))

    with :ok <- File.mkdir_p(directory),
         :ok <- File.write(temporary_path, contents, [:binary]),
         :ok <- File.rename(temporary_path, state.path) do
      :ok
    else
      {:error, reason} ->
        File.rm(temporary_path)
        {:error, reason}
    end
  end
end
