# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Implementar expiração automática de registros.
# 3. Fazer uso de macros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem.API do
  defmacro __using__(_opts) do
    quote do
      def create_poll(question, options, ttl_ms \\ 86_400_000) do
        GenServer.call(__MODULE__, {:create_poll, question, options, ttl_ms})
      end

      def vote(poll_id, voter_id, option) do
        GenServer.call(__MODULE__, {:vote, poll_id, voter_id, option})
      end

      def poll(poll_id) do
        GenServer.call(__MODULE__, {:poll, poll_id})
      end

      def results(poll_id) do
        GenServer.call(__MODULE__, {:results, poll_id})
      end
    end
  end
end

defmodule VotingSystem do
  use GenServer
  use VotingSystem.API

  @table :voting_system_dets
  @default_cleanup_interval 60_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path =
      opts
      |> Keyword.get(:path, "voting_system.dets")
      |> Path.expand()

    cleanup_interval =
      case Keyword.get(opts, :cleanup_interval, @default_cleanup_interval) do
        interval when is_integer(interval) and interval > 0 -> interval
        _ -> @default_cleanup_interval
      end

    with :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, @table} <-
           :dets.open_file(@table, file: String.to_charlist(path), type: :set, repair: true) do
      cleanup_expired_records()
      schedule_cleanup(cleanup_interval)

      {:ok, %{cleanup_interval: cleanup_interval}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create_poll, question, options, ttl_ms}, _from, state) do
    if valid_poll?(question, options, ttl_ms) do
      id = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

      poll = %{
        id: id,
        question: question,
        options: options,
        expires_at: now_ms() + ttl_ms
      }

      case persist({:poll, id}, poll) do
        :ok -> {:reply, {:ok, id}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :invalid_poll}, state}
    end
  end

  def handle_call({:vote, poll_id, voter_id, option}, _from, state) do
    now = now_ms()

    case get_active_poll(poll_id, now) do
      {:ok, poll} ->
        cond do
          not valid_identifier?(voter_id) ->
            {:reply, {:error, :invalid_voter_id}, state}

          option not in poll.options ->
            {:reply, {:error, :invalid_option}, state}

          true ->
            vote = %{option: option, expires_at: poll.expires_at}

            case persist({:vote, poll_id, voter_id}, vote) do
              :ok -> {:reply, :ok, state}
              {:error, reason} -> {:reply, {:error, reason}, state}
            end
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:poll, poll_id}, _from, state) do
    case get_active_poll(poll_id, now_ms()) do
      {:ok, poll} ->
        {:reply,
         {:ok,
          %{
            id: poll.id,
            question: poll.question,
            options: poll.options,
            expires_at: poll.expires_at
          }}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:results, poll_id}, _from, state) do
    case get_active_poll(poll_id, now_ms()) do
      {:ok, poll} ->
        counts = Map.new(poll.options, fn option -> {option, 0} end)
        now = now_ms()

        results =
          :dets.foldl(
            fn
              {{:vote, ^poll_id, _voter_id}, %{option: option, expires_at: expires_at}}, acc
              when expires_at > now ->
                if Map.has_key?(acc, option) do
                  Map.update!(acc, option, &(&1 + 1))
                else
                  acc
                end

              _entry, acc ->
                acc
            end,
            counts,
            @table
          )

        {:reply, {:ok, results}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired_records()
    schedule_cleanup(state.cleanup_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.sync(@table)
    :dets.close(@table)
    :ok
  end

  defp valid_poll?(question, options, ttl_ms) do
    is_binary(question) and byte_size(question) > 0 and
      is_list(options) and options != [] and
      Enum.all?(options, &(is_binary(&1) and byte_size(&1) > 0)) and
      length(Enum.uniq(options)) == length(options) and
      is_integer(ttl_ms) and ttl_ms > 0
  end

  defp valid_identifier?(value), do: is_binary(value) and byte_size(value) > 0

  defp get_active_poll(poll_id, now) do
    case :dets.lookup(@table, {:poll, poll_id}) do
      [{{:poll, ^poll_id}, %{expires_at: expires_at} = poll}] when expires_at > now ->
        {:ok, poll}

      [{{:poll, ^poll_id}, _poll}] ->
        delete_poll_records(poll_id)
        {:error, :expired}

      [] ->
        {:error, :not_found}
    end
  end

  defp persist(key, value) do
    with :ok <- :dets.insert(@table, {key, value}),
         :ok <- :dets.sync(@table) do
      :ok
    end
  end

  defp cleanup_expired_records do
    now = now_ms()
    entries = :dets.foldl(fn entry, acc -> [entry | acc] end, [], @table)

    expired_poll_ids =
      entries
      |> Enum.flat_map(fn
        {{:poll, id}, %{expires_at: expires_at}} when expires_at <= now -> [id]
        _entry -> []
      end)
      |> MapSet.new()

    expired_keys =
      Enum.flat_map(entries, fn
        {{:poll, id}, %{expires_at: expires_at}} = entry ->
          if expires_at <= now, do: [elem(entry, 0)], else: []

        {{:vote, poll_id, _voter_id}, %{expires_at: expires_at}} = entry ->
          if expires_at <= now or MapSet.member?(expired_poll_ids, poll_id),
            do: [elem(entry, 0)],
            else: []

        _entry ->
          []
      end)

    Enum.each(expired_keys, &:dets.delete(@table, &1))

    if expired_keys != [] do
      :dets.sync(@table)
    end

    :ok
  end

  defp delete_poll_records(poll_id) do
    keys =
      :dets.foldl(
        fn
          {{:poll, ^poll_id}, _value}, acc ->
            [{:poll, poll_id} | acc]

          {{:vote, ^poll_id, _voter_id} = key, _value}, acc ->
            [key | acc]

          _entry, acc ->
            acc
        end,
        [],
        @table
      )

    Enum.each(keys, &:dets.delete(@table, &1))
    :dets.sync(@table)
  end

  defp schedule_cleanup(interval) do
    Process.send_after(self(), :cleanup, interval)
  end

  defp now_ms do
    System.system_time(:millisecond)
  end
end
