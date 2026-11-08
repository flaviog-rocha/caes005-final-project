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

defmodule OnlineVoting.Entity do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      def via(id) do
        {:via, Registry, {OnlineVoting.Registry, {__MODULE__, id}}}
      end

      def start_link(id, config) do
        GenServer.start_link(__MODULE__, {id, config}, name: via(id))
      end

      def entity_call(id, message, timeout \\ 5_000) do
        GenServer.call(via(id), message, timeout)
      end

      def child_spec({id, config}) do
        %{
          id: {__MODULE__, id},
          start: {__MODULE__, :start_link, [id, config]},
          restart: :temporary,
          shutdown: 5_000,
          type: :worker
        }
      end
    end
  end
end

defmodule OnlineVoting.Poll do
  @moduledoc false
  @behaviour GenServer

  use OnlineVoting.Entity

  @impl true
  def init({id, config}) do
    counts = Map.new(config.options, &{&1, 0})

    {:ok,
     %{
       id: id,
       question: config.question,
       options: config.options,
       counts: counts,
       voters: %{}
     }}
  end

  @impl true
  def handle_call({:vote, voter_id, option}, _from, state) do
    cond do
      Map.has_key?(state.voters, voter_id) ->
        {:reply, {:error, :already_voted}, state}

      not Map.has_key?(state.counts, option) ->
        {:reply, {:error, :invalid_option}, state}

      true ->
        counts = Map.update!(state.counts, option, &(&1 + 1))
        voters = Map.put(state.voters, voter_id, option)

        {:reply, {:ok, counts[option]}, %{state | counts: counts, voters: voters}}
    end
  end

  @impl true
  def handle_call(:results, _from, state) do
    results = %{
      id: state.id,
      question: state.question,
      options: state.options,
      counts: state.counts,
      total_votes: Enum.sum(Map.values(state.counts))
    }

    {:reply, {:ok, results}, state}
  end
end

defmodule OnlineVoting do
  @moduledoc """
  Sistema de votação em memória. Cada votação é processada por um processo
  independente; chamadas ao processo serializam as operações daquela votação.
  """

  use Supervisor

  @registry OnlineVoting.Registry
  @poll_supervisor OnlineVoting.PollSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @poll_supervisor}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def start_poll(id, question, options) when is_list(options) do
    if options != [] and length(Enum.uniq(options)) == length(options) do
      config = %{question: question, options: options}

      case DynamicSupervisor.start_child(@poll_supervisor, {OnlineVoting.Poll, {id, config}}) do
        {:ok, pid} ->
          {:ok, pid}

        {:error, {:already_started, _pid}} ->
          {:error, :already_exists}

        {:error, {:already_present, _id}} ->
          {:error, :already_exists}

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, :invalid_options}
    end
  end

  def start_poll(_id, _question, _options), do: {:error, :invalid_options}

  def cast_vote(poll_id, voter_id, option) do
    call_poll(poll_id, {:vote, voter_id, option})
  end

  def results(poll_id) do
    call_poll(poll_id, :results)
  end

  defp call_poll(poll_id, message) do
    case Registry.lookup(@registry, {OnlineVoting.Poll, poll_id}) do
      [{pid, _value}] -> GenServer.call(pid, message)
      [] -> {:error, :not_found}
    end
  end
end
