# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Executar determinadas operações periodicamente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule PersistentMessaging do
  use GenServer

  @table __MODULE__
  @default_ttl_ms 24 * 60 * 60 * 1_000
  @default_cleanup_interval_ms 60 * 1_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def send_message(sender, recipient, body) do
    send_message(__MODULE__, sender, recipient, body)
  end

  def send_message(server, sender, recipient, body) do
    GenServer.call(server, {:send_message, sender, recipient, body})
  end

  def list_messages(user) do
    list_messages(__MODULE__, user)
  end

  def list_messages(server, user) do
    GenServer.call(server, {:list_messages, user})
  end

  @impl true
  def init(opts) do
    file =
      opts
      |> Keyword.get(:file, "messages.dets")
      |> to_string()
      |> String.to_charlist()

    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    cleanup_interval_ms =
      Keyword.get(opts, :cleanup_interval_ms, @default_cleanup_interval_ms)

    case :dets.open_file(@table, file: file, type: :set, repair: true) do
      {:ok, table} ->
        state = %{
          table: table,
          ttl_ms: ttl_ms,
          cleanup_interval_ms: cleanup_interval_ms
        }

        purge_expired(state)
        schedule_cleanup(cleanup_interval_ms)
        {:ok, state}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:send_message, sender, recipient, body}, _from, state) do
    now = System.system_time(:millisecond)

    message = %{
      id: make_ref(),
      sender: sender,
      recipient: recipient,
      body: body,
      inserted_at: now,
      expires_at: now + state.ttl_ms
    }

    case :dets.insert(state.table, {message.id, message}) do
      :ok ->
        case :dets.sync(state.table) do
          :ok -> {:reply, {:ok, message}, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:list_messages, user}, _from, state) do
    state = purge_expired(state)
    now = System.system_time(:millisecond)

    messages =
      :dets.foldl(
        fn {_id, message}, acc ->
          if message.expires_at > now and
               (message.sender == user or message.recipient == user) do
            [message | acc]
          else
            acc
          end
        end,
        [],
        state.table
      )
      |> Enum.sort_by(& &1.inserted_at)

    {:reply, messages, state}
  end

  @impl true
  def handle_info(:expire_messages, state) do
    purge_expired(state)
    schedule_cleanup(state.cleanup_interval_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
  end

  defp purge_expired(state) do
    now = System.system_time(:millisecond)

    expired_ids =
      :dets.foldl(
        fn {id, message}, acc ->
          if message.expires_at <= now, do: [id | acc], else: acc
        end,
        [],
        state.table
      )

    if expired_ids != [] do
      Enum.each(expired_ids, &:dets.delete(state.table, &1))
      :dets.sync(state.table)
    end

    state
  end

  defp schedule_cleanup(interval_ms) do
    Process.send_after(self(), :expire_messages, interval_ms)
  end
end
