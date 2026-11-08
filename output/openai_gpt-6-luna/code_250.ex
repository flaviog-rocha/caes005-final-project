# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule MessageStore do
  use GenServer

  @default_path "messages.db"
  @default_cleanup_interval 60_000

  # The server uses a single registered process by default. Pass name: nil to
  # start an unregistered instance.
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  def send_message(server, from, to, body, ttl_ms) do
    GenServer.call(server, {:send_message, from, to, body, ttl_ms})
  end

  def inbox(server, user) do
    GenServer.call(server, {:inbox, user})
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    interval = Keyword.get(opts, :cleanup_interval, @default_cleanup_interval)

    if is_binary(path) and is_integer(interval) and interval > 0 do
      case load_state(path) do
        {:ok, data} ->
          {data, changed?} = remove_expired(data)

          if changed? do
            case persist(path, data) do
              :ok -> schedule_cleanup(interval, path, data)
              {:error, reason} -> {:stop, {:persistence_failed, reason}}
            end
          else
            schedule_cleanup(interval, path, data)
          end

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, :invalid_options}
    end
  end

  @impl true
  def handle_call({:send_message, from, to, body, ttl_ms}, _from_pid, state) do
    if valid_message?(from, to, body, ttl_ms) do
      id = state.data.next_id
      now = System.system_time(:millisecond)

      message = %{
        id: id,
        from: from,
        to: to,
        body: body,
        created_at: now,
        expires_at: now + ttl_ms
      }

      data = %{
        state.data
        | next_id: id + 1,
          messages: Map.put(state.data.messages, id, message)
      }

      case persist(state.path, data) do
        :ok -> {:reply, {:ok, id}, %{state | data: data}}
        {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, state}
      end
    else
      {:reply, {:error, :invalid_message}, state}
    end
  end

  def handle_call({:inbox, user}, _from_pid, state) when is_binary(user) do
    now = System.system_time(:millisecond)

    messages =
      state.data.messages
      |> Map.values()
      |> Enum.filter(&(&1.to == user and &1.expires_at > now))
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, messages}, state}
  end

  def handle_call({:inbox, _user}, _from_pid, state) do
    {:reply, {:error, :invalid_user}, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    {data, changed?} = remove_expired(state.data)

    next_state =
      if changed? and persist(state.path, data) == :ok do
        %{state | data: data}
      else
        state
      end

    schedule_cleanup(state.interval, state.path, next_state.data)
  end

  defp schedule_cleanup(interval, path, data) do
    Process.send_after(self(), :cleanup, interval)
    {:ok, %{path: path, interval: interval, data: data}}
  end

  defp valid_message?(from, to, body, ttl_ms) do
    is_binary(from) and byte_size(from) > 0 and
      is_binary(to) and byte_size(to) > 0 and
      is_binary(body) and is_integer(ttl_ms) and ttl_ms > 0
  end

  defp remove_expired(data) do
    now = System.system_time(:millisecond)
    messages = Map.filter(data.messages, fn {_id, message} -> message.expires_at > now end)

    if map_size(messages) == map_size(data.messages) do
      {data, false}
    else
      {%{data | messages: messages}, true}
    end
  end

  defp load_state(path) do
    case File.read(path) do
      {:ok, contents} ->
        try do
          data = :erlang.binary_to_term(contents, [:safe])

          if valid_state?(data) do
            {:ok, data}
          else
            {:error, :invalid_persisted_data}
          end
        rescue
          _ -> {:error, :invalid_persisted_data}
        end

      {:error, :enoent} ->
        {:ok, %{next_id: 1, messages: %{}}}

      {:error, reason} ->
        {:error, {:read_failed, reason}}
    end
  end

  defp valid_state?(%{next_id: next_id, messages: messages})
       when is_integer(next_id) and next_id > 0 and is_map(messages) do
    Enum.all?(messages, fn
      {id, %{id: ^id, from: from, to: to, body: body, created_at: created_at, expires_at: expires_at}} ->
        is_integer(id) and id > 0 and is_binary(from) and is_binary(to) and
          is_binary(body) and is_integer(created_at) and is_integer(expires_at)

      _ ->
        false
    end)
  end

  defp valid_state?(_), do: false

  defp persist(path, data) do
    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, contents} <- encode(data),
         :ok <- File.write_file(path <> ".tmp", contents, [:binary, :sync]),
         :ok <- File.rename(path <> ".tmp", path) do
      :ok
    else
      {:error, reason} ->
        File.rm(path <> ".tmp")
        {:error, reason}
    end
  end

  defp encode(data) do
    try do
      {:ok, :erlang.term_to_binary(data)}
    rescue
      error -> {:error, error}
    end
  end
end
