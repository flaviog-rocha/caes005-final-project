# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Fazer uso de macros.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Voting.Macros do
  defmacro valid_choice?(choice, allowed_choices) do
    quote do
      Enum.member?(unquote(allowed_choices), unquote(choice))
    end
  end
end

defmodule Voting.Store do
  use GenServer
  require Voting.Macros

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def cast_vote(operation_id, choice, server \\ __MODULE__) do
    GenServer.call(server, {:cast_vote, operation_id, choice}, :infinity)
  end

  def tally(server \\ __MODULE__) do
    GenServer.call(server, :tally, :infinity)
  end

  @impl true
  def init(opts) do
    table = Keyword.get(opts, :table, :voting_dets)
    file = opts |> Keyword.get(:path, "votes.dets") |> String.to_charlist()
    choices = Keyword.get(opts, :choices, [])

    case :dets.open_file(table, file: file, type: :set, repair: true) do
      {:ok, ^table} ->
        {:ok, %{table: table, choices: choices}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:cast_vote, operation_id, choice}, _from, state) do
    cond do
      not Voting.Macros.valid_choice?(choice, state.choices) ->
        {:reply, {:error, :invalid_choice}, state}

      :dets.lookup(state.table, {:vote, operation_id}) != [] ->
        {:reply, {:ok, :already_processed}, state}

      true ->
        case :dets.insert(state.table, {{:vote, operation_id}, choice}) do
          :ok ->
            case :dets.sync(state.table) do
              :ok -> {:reply, {:ok, :recorded}, state}
              {:error, reason} -> {:reply, {:error, reason}, state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:tally, _from, state) do
    initial = Map.new(state.choices, &{&1, 0})

    tally =
      :dets.foldl(
        fn
          {{:vote, _operation_id}, choice}, counts ->
            Map.update(counts, choice, 1, &(&1 + 1))

          _other, counts ->
            counts
        end,
        initial,
        state.table
      )

    {:reply, tally, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.table)
  end
end

defmodule Voting do
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def cast_vote(operation_id, choice, server \\ Voting.Store) do
    Voting.Store.cast_vote(operation_id, choice, server)
  end

  def tally(server \\ Voting.Store) do
    Voting.Store.tally(server)
  end

  def cast_many(votes, server \\ Voting.Store) do
    votes
    |> Stream.map(fn {operation_id, choice} ->
      Voting.Store.cast_vote(operation_id, choice, server)
    end)
    |> Enum.reduce(%{recorded: 0, already_processed: 0, errors: 0}, fn
      {:ok, :recorded}, counts -> %{counts | recorded: counts.recorded + 1}
      {:ok, :already_processed}, counts ->
        %{counts | already_processed: counts.already_processed + 1}

      {:error, _reason}, counts ->
        %{counts | errors: counts.errors + 1}
    end)
  end

  @impl true
  def init(opts) do
    store_opts =
      opts
      |> Keyword.put(:name, Keyword.get(opts, :store_name, Voting.Store))

    children = [
      %{
        id: Voting.Store,
        start: {Voting.Store, :start_link, [store_opts]},
        restart: :permanent,
        shutdown: 5_000,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
