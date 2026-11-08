# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting.API do
  defmacro client(name, arity) do
    args = for index <- 1..arity, do: Macro.var(:"arg#{index}", nil)

    quote do
      def unquote(name)(unquote_splicing(args)) do
        GenServer.call(
          __MODULE__,
          {unquote(name), unquote_splicing(args)}
        )
      end
    end
  end
end

defmodule OnlineVoting do
  use GenServer
  require OnlineVoting.API

  @table :online_voting_store

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def stop do
    GenServer.stop(__MODULE__)
  end

  OnlineVoting.API.client(:create_poll, 3)
  OnlineVoting.API.client(:get_poll, 1)
  OnlineVoting.API.client(:vote, 3)
  OnlineVoting.API.client(:results, 1)

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "online_voting.dets")
    cleanup_interval = Keyword.get(opts, :cleanup_interval, 60_000)

    with true <- is_integer(cleanup_interval) and cleanup_interval > 0,
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, @table} <-
           :dets.open_file(@table, file: String.to_charlist(path), type: :set) do
      Process.send_after(self(), :cleanup, cleanup_interval)
      {:ok, %{cleanup_interval: cleanup_interval}}
    else
      false -> {:stop, :invalid_cleanup_interval}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create_poll, question, options, ttl_ms}, _from, state) do
    with :ok <- validate_poll(question, options, ttl_ms) do
      id = generate_id()
      expires_at = now_ms() + ttl_ms

      poll = %{
        id: id,
        question: question,
        options: options,
        expires_at: expires_at
      }

      case persist({{:poll, id}, poll}) do
        :ok -> {:reply, {:ok, id}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_poll, id}, _from, state) do
    {:reply, fetch_active_poll(id), state}
  end

  def handle_call({:vote, poll_id, voter_id, option}, _from, state) do
    reply =
      with true <- is_binary(voter_id) and byte_size(voter_id) > 0,
           {:ok, poll} <- fetch_active_poll(poll_id),
           true <- option in poll.options,
           [] <- :dets.lookup(@table, {:vote, poll_id, voter_id}) do
        vote = %{option: option, expires_at: poll.expires_at}

        case persist({{:vote, poll_id, voter_id}, vote}) do
          :ok -> {:ok, :recorded}
          {:error, reason} -> {:error, reason}
        end
      else
        false -> {:error, :invalid_voter_or_option}
        {:error, reason} -> {:error, reason}
        [_existing] -> {:error, :already_voted}
      end

    {:reply, reply, state}
  end

  def handle_call({:results, poll_id}, _from, state) do
    reply =
      with {:ok, poll} <- fetch_active_poll(poll_id) do
        initial = Map.new(poll.options, &{&1, 0})

        counts =
          :dets.foldl(
            fn
              {{:vote, ^poll_id, _voter_id}, vote}, acc ->
                if vote.expires_at > now_ms() do
                  Map.update!(acc, vote.option, &(&1 + 1))
                else
                  acc
                end

              _, acc ->
                acc
            end,
            initial,
            @table
          )

        {:ok, counts}
      end

    {:reply, reply, state}
  end

  def handle_call(_request, _from, state) do
    {:reply, {:error, :unknown_request}, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired()
    Process.send_after(self(), :cleanup, state.cleanup_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  defp validate_poll(question, options, ttl_ms) do
    cond do
      not (is_binary(question) and byte_size(question) > 0) ->
        {:error, :invalid_question}

      not (is_list(options) and options != [] and
             Enum.all?(options, &(is_binary(&1) and byte_size(&1) > 0))) ->
        {:error, :invalid_options}

      length(Enum.uniq(options)) != length(options) ->
        {:error, :duplicate_options}

      not (is_integer(ttl_ms) and ttl_ms > 0) ->
        {:error, :invalid_ttl}

      true ->
        :ok
    end
  end

  defp fetch_active_poll(id) do
    case :dets.lookup(@table, {:poll, id}) do
      [{{:poll, ^id}, poll}] ->
        if poll.expires_at > now_ms() do
          {:ok, poll}
        else
          :dets.delete(@table, {:poll, id})
          :dets.sync(@table)
          {:error, :poll_expired}
        end

      [] ->
        {:error, :poll_not_found}
    end
  end

  defp cleanup_expired do
    now = now_ms()

    expired_keys =
      :dets.foldl(
        fn {key, record}, acc ->
          if record.expires_at <= now, do: [key | acc], else: acc
        end,
        [],
        @table
      )

    if expired_keys != [] do
      Enum.each(expired_keys, &:dets.delete(@table, &1))
      :dets.sync(@table)
    end

    :ok
  end

  defp persist(object) do
    case :dets.insert(@table, object) do
      :ok -> :dets.sync(@table)
      {:error, reason} -> {:error, reason}
    end
  end

  defp generate_id do
    :crypto.strong_rand_bytes(16)
    |> Base.url_encode64(padding: false)
  end

  defp now_ms, do: System.system_time(:millisecond)
end
