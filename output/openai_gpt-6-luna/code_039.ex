# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Fazer uso de macros.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Voting.API do
  defmacro __using__(_opts) do
    quote do
      def create_poll(id, choices), do: Voting.Polls.create_poll(id, choices)
      def vote(poll_id, voter_id, choice), do: Voting.Polls.vote(poll_id, voter_id, choice)
      def results(poll_id), do: Voting.Polls.results(poll_id)
    end
  end
end

defmodule Voting.Poll do
  use GenServer

  def start_link(opts) do
    id = Keyword.fetch!(opts, :id)

    GenServer.start_link(
      __MODULE__,
      opts,
      name: {:via, Registry, {Voting.Registry, id}}
    )
  end

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)

    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @impl true
  def init(opts) do
    choices = Keyword.fetch!(opts, :choices)

    {:ok,
     %{
       choices: choices,
       choice_set: MapSet.new(choices),
       votes: %{},
       counts: Map.new(choices, &{&1, 0})
     }}
  end

  @impl true
  def handle_call({:vote, voter_id, choice}, _from, state) do
    cond do
      not MapSet.member?(state.choice_set, choice) ->
        {:reply, {:error, :invalid_choice}, state}

      Map.has_key?(state.votes, voter_id) ->
        {:reply, {:error, :already_voted}, state}

      true ->
        votes = Map.put(state.votes, voter_id, choice)
        counts = Map.update!(state.counts, choice, &(&1 + 1))
        new_state = %{state | votes: votes, counts: counts}

        {:reply, {:ok, Map.fetch!(counts, choice)}, new_state}
    end
  end

  @impl true
  def handle_call(:results, _from, state) do
    {:reply,
     {:ok,
      %{
        choices: state.choices,
        counts: state.counts,
        total_votes: map_size(state.votes)
      }}, state}
  end
end

defmodule Voting.Polls do
  def create_poll(id, choices) when is_list(choices) do
    if choices != [] and length(choices) == MapSet.size(MapSet.new(choices)) do
      case DynamicSupervisor.start_child(
             Voting.PollSupervisor,
             {Voting.Poll, id: id, choices: choices}
           ) do
        {:ok, _pid} -> {:ok, id}
        {:error, {:already_started, _pid}} -> {:error, :poll_exists}
        {:error, {:already_registered, _pid}} -> {:error, :poll_exists}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :invalid_choices}
    end
  end

  def create_poll(_id, _choices), do: {:error, :invalid_choices}

  def vote(poll_id, voter_id, choice) do
    call_poll(poll_id, {:vote, voter_id, choice})
  end

  def results(poll_id) do
    call_poll(poll_id, :results)
  end

  defp call_poll(poll_id, message) do
    case Registry.lookup(Voting.Registry, poll_id) do
      [{pid, _value}] ->
        try do
          GenServer.call(pid, message)
        catch
          :exit, _reason -> {:error, :poll_unavailable}
        end

      [] ->
        {:error, :unknown_poll}
    end
  end
end

defmodule Voting do
  use Supervisor
  use Voting.API

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: Voting.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
