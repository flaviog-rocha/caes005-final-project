# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  use GenServer

  @moduledoc """
  Sistema de votação com atualizações serializadas, notificações e votos em lote.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, Keyword.take(opts, [:name]))
  end

  def create_poll(server, poll_id, options, opts \\ []) do
    GenServer.call(server, {:create_poll, poll_id, options, Keyword.get(opts, :notify_at)})
  end

  def subscribe(server, pid \\ self()) when is_pid(pid) do
    GenServer.call(server, {:subscribe, pid})
  end

  def vote(server, poll_id, voter, option, opts \\ []) do
    retries = Keyword.get(opts, :retries, 3)
    vote_with_retry(server, poll_id, voter, option, retries)
  end

  def vote_many(server, votes, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    retries = Keyword.get(opts, :retries, 3)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      votes
      |> Task.async_stream(
        fn
          {poll_id, voter, option} ->
            vote(server, poll_id, voter, option, retries: retries)

          _invalid ->
            {:error, :invalid_vote_request}
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, reason} -> {:error, {:task_exit, reason}}
      end)
    else
      {:error, :invalid_max_concurrency}
    end
  end

  def results(server, poll_id) do
    GenServer.call(server, {:results, poll_id})
  end

  @impl true
  def init(_opts) do
    {:ok, %{polls: %{}, subscribers: MapSet.new()}}
  end

  @impl true
  def handle_call({:create_poll, poll_id, options, notify_at}, _from, state) do
    cond do
      Map.has_key?(state.polls, poll_id) ->
        {:reply, {:error, :poll_already_exists}, state}

      not valid_options?(options) ->
        {:reply, {:error, :invalid_options}, state}

      not is_nil(notify_at) and (not is_integer(notify_at) or notify_at < 1) ->
        {:reply, {:error, :invalid_notification_threshold}, state}

      true ->
        poll = %{
          options: MapSet.new(options),
          counts: Map.new(options, &{&1, 0}),
          voters: MapSet.new(),
          total: 0,
          notify_at: notify_at,
          notified: false
        }

        {:reply, :ok, put_in(state.polls[poll_id], poll)}
    end
  end

  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, update_in(state.subscribers, &MapSet.put(&1, pid))}
  end

  def handle_call({:vote, poll_id, voter, option}, _from, state) do
    case Map.fetch(state.polls, poll_id) do
      :error ->
        {:reply, {:error, :poll_not_found}, state}

      {:ok, poll} ->
        cond do
          MapSet.member?(poll.voters, voter) ->
            {:reply, {:error, :already_voted}, state}

          not MapSet.member?(poll.options, option) ->
            {:reply, {:error, :invalid_option}, state}

          true ->
            updated_poll = %{
              poll
              | voters: MapSet.put(poll.voters, voter),
                counts: Map.update!(poll.counts, option, &(&1 + 1)),
                total: poll.total + 1
            }

            updated_poll = maybe_notify(state.subscribers, poll_id, updated_poll)
            updated_state = put_in(state.polls[poll_id], updated_poll)

            {:reply, {:ok, %{poll_id: poll_id, option: option, total: updated_poll.total}},
             updated_state}
        end
    end
  end

  def handle_call({:results, poll_id}, _from, state) do
    case Map.fetch(state.polls, poll_id) do
      {:ok, poll} ->
        {:reply, {:ok, %{counts: poll.counts, total: poll.total}}, state}

      :error ->
        {:reply, {:error, :poll_not_found}, state}
    end
  end

  defp maybe_notify(subscribers, poll_id, %{notify_at: threshold, total: threshold, notified: false} = poll)
       when is_integer(threshold) do
    message = {:voting_notification, :threshold_reached, poll_id, threshold}
    Enum.each(subscribers, &send(&1, message))
    %{poll | notified: true}
  end

  defp maybe_notify(_subscribers, _poll_id, poll), do: poll

  defp vote_with_retry(server, poll_id, voter, option, retries) do
    result =
      try do
        GenServer.call(server, {:vote, poll_id, voter, option}, 5_000)
      catch
        :exit, reason -> {:error, {:temporarily_unavailable, reason}}
      end

    case result do
      {:error, {:temporarily_unavailable, _reason}} when retries > 0 ->
        Process.sleep(10)
        vote_with_retry(server, poll_id, voter, option, retries - 1)

      other ->
        other
    end
  end

  defp valid_options?(options) when is_list(options) do
    options != [] and Enum.uniq(options) == options
  end

  defp valid_options?(_options), do: false
end
