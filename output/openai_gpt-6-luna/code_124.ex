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
  Sistema de votação concorrente com notificações, limite de tarefas e novas tentativas.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def vote(server, poll_id, voter_id, choice, opts \\ []) do
    retries = non_negative_integer(Keyword.get(opts, :max_retries, 3), 3)
    delay = non_negative_integer(Keyword.get(opts, :retry_delay_ms, 50), 50)
    timeout = Keyword.get(opts, :call_timeout, 5_000)

    vote_with_retry(server, poll_id, voter_id, choice, retries, delay, timeout)
  end

  @doc """
  Processa votos em paralelo, respeitando o limite de concorrência configurado
  na inicialização do servidor. Cada item deve ser `{poll_id, voter_id, choice}`.
  """
  def vote_many(server, votes, opts \\ []) do
    configured_limit = GenServer.call(server, :max_concurrency)
    requested_limit = Keyword.get(opts, :max_concurrency, configured_limit)

    limit =
      requested_limit
      |> positive_integer(configured_limit)
      |> min(configured_limit)

    votes
    |> Task.async_stream(
      fn {poll_id, voter_id, choice} ->
        vote(server, poll_id, voter_id, choice, opts)
      end,
      max_concurrency: limit,
      timeout: :infinity,
      on_timeout: :kill_task,
      ordered: false
    )
    |> Enum.to_list()
  end

  def subscribe(server, subscriber \\ self()) when is_pid(subscriber) do
    GenServer.call(server, {:subscribe, subscriber})
  end

  def notifications(server) do
    GenServer.call(server, :notifications)
  end

  @doc "Configura falhas temporárias para permitir testar o mecanismo de novas tentativas."
  def fail_next(server, count) when is_integer(count) and count >= 0 do
    GenServer.call(server, {:fail_next, count})
  end

  @impl true
  def init(opts) do
    threshold = Keyword.get(opts, :threshold)

    {:ok,
     %{
       threshold: if(is_integer(threshold) and threshold > 0, do: threshold),
       notify_to: Keyword.get(opts, :notify_to),
       max_concurrency:
         positive_integer(
           Keyword.get(opts, :max_concurrency, System.schedulers_online()),
           System.schedulers_online()
         ),
       counts: %{},
       totals: %{},
       voter_choices: %{},
       notified_polls: MapSet.new(),
       subscribers: MapSet.new(),
       notifications: [],
       temporary_failures: 0
     }}
  end

  @impl true
  def handle_call({:vote, poll_id, voter_id, choice}, _from, %{temporary_failures: failures} = state)
      when failures > 0 do
    {:reply, {:error, :temporary}, %{state | temporary_failures: failures - 1}}
  end

  def handle_call({:vote, poll_id, voter_id, choice}, _from, state) do
    voter_key = {poll_id, voter_id}

    case Map.fetch(state.voter_choices, voter_key) do
      {:ok, ^choice} ->
        {:reply, {:ok, :already_voted}, state}

      {:ok, _different_choice} ->
        {:reply, {:error, :already_voted}, state}

      :error ->
        count_key = {poll_id, choice}
        count = Map.get(state.counts, count_key, 0) + 1
        total = Map.get(state.totals, poll_id, 0) + 1

        new_state = %{
          state
          | counts: Map.put(state.counts, count_key, count),
            totals: Map.put(state.totals, poll_id, total),
            voter_choices: Map.put(state.voter_choices, voter_key, choice)
        }

        new_state = maybe_notify(new_state, poll_id, total)

        result = %{
          poll_id: poll_id,
          voter_id: voter_id,
          choice: choice,
          count: count,
          total: total
        }

        {:reply, {:ok, result}, new_state}
    end
  end

  def handle_call(:max_concurrency, _from, state) do
    {:reply, state.max_concurrency, state}
  end

  def handle_call(:notifications, _from, state) do
    {:reply, Enum.reverse(state.notifications), state}
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, subscriber)}}
  end

  def handle_call({:fail_next, count}, _from, state) do
    {:reply, :ok, %{state | temporary_failures: count}}
  end

  defp maybe_notify(%{threshold: nil} = state, _poll_id, _total), do: state

  defp maybe_notify(state, poll_id, total) do
    if total >= state.threshold and not MapSet.member?(state.notified_polls, poll_id) do
      event = {:vote_threshold_reached, poll_id, total}

      recipients =
        [state.notify_to | MapSet.to_list(state.subscribers)]
        |> Enum.filter(&is_pid/1)
        |> Enum.uniq()

      Enum.each(recipients, &send(&1, event))

      %{
        state
        | notified_polls: MapSet.put(state.notified_polls, poll_id),
          notifications: [event | state.notifications]
      }
    else
      state
    end
  end

  defp vote_with_retry(server, poll_id, voter_id, choice, retries, delay, timeout) do
    case safe_vote_call(server, poll_id, voter_id, choice, timeout) do
      {:error, :temporary} when retries > 0 ->
        if delay > 0, do: Process.sleep(delay)
        vote_with_retry(server, poll_id, voter_id, choice, retries - 1, delay, timeout)

      result ->
        result
    end
  end

  defp safe_vote_call(server, poll_id, voter_id, choice, timeout) do
    try do
      GenServer.call(server, {:vote, poll_id, voter_id, choice}, timeout)
    catch
      :exit, _reason -> {:error, :temporary}
    end
  end

  defp positive_integer(value, _fallback) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, fallback), do: fallback

  defp non_negative_integer(value, _fallback) when is_integer(value) and value >= 0, do: value
  defp non_negative_integer(_value, fallback), do: fallback
end
