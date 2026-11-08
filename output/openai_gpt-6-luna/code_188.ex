# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  use Supervisor

  @store OnlineVoting.Store
  @registry OnlineVoting.Registry
  @dynamic_supervisor OnlineVoting.ElectionSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, "online_voting.dets")

    children = [
      {OnlineVoting.Store, path: path},
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor},
      OnlineVoting.Recovery
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def create_election(title) when is_binary(title) do
    id = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    election = %{id: id, title: title, created_at: DateTime.utc_now(), vote_count: 0}

    case OnlineVoting.Store.create_election(election) do
      :ok ->
        case DynamicSupervisor.start_child(
               @dynamic_supervisor,
               OnlineVoting.Election.child_spec(id)
             ) do
          {:ok, _pid} -> {:ok, id}
          {:error, {:already_started, _pid}} -> {:ok, id}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def election(id), do: OnlineVoting.Store.get_election(id)

  def vote(election_id, voter_id, candidate)
      when is_binary(election_id) and not is_nil(voter_id) do
    case Registry.lookup(@registry, election_id) do
      [{pid, _}] -> GenServer.call(pid, {:vote, voter_id, candidate})
      [] -> {:error, :election_not_found}
    end
  end

  # Produces rows as {voter_id, candidate}, loading only one page at a time.
  def votes_stream(election_id, batch_size \\ 500)
      when is_binary(election_id) and is_integer(batch_size) and batch_size > 0 do
    Stream.resource(
      fn -> :first_page end,
      fn
        :done ->
          {:halt, :done}

        cursor ->
          case OnlineVoting.Store.next_votes_page(election_id, cursor, batch_size) do
            {:ok, rows, next_cursor} -> {:cont, rows, next_cursor}
            :done -> {:halt, :done}
          end
      end,
      fn _cursor -> :ok end
    )
  end
end

defmodule OnlineVoting.Store do
  use GenServer

  @table :online_voting_dets

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def create_election(election), do: GenServer.call(__MODULE__, {:create_election, election})
  def get_election(id), do: GenServer.call(__MODULE__, {:get_election, id})

  def cast_vote(election_id, voter_id, candidate) do
    GenServer.call(__MODULE__, {:cast_vote, election_id, voter_id, candidate})
  end

  def next_votes_page(election_id, cursor, limit) do
    GenServer.call(__MODULE__, {:next_votes_page, election_id, cursor, limit}, :infinity)
  end

  def election_ids_page(cursor, limit) do
    GenServer.call(__MODULE__, {:election_ids_page, cursor, limit}, :infinity)
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    File.mkdir_p!(Path.dirname(path))

    case :dets.open_file(@table,
           file: String.to_charlist(path),
           type: :set,
           repair: true
         ) do
      {:ok, @table} -> {:ok, %{}}
      {:error, reason} -> {:stop, {:dets_open_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create_election, election}, _from, state) do
    result =
      if :dets.insert_new(@table, {{:election, election.id}, election}) do
        :ok
      else
        {:error, :id_collision}
      end

    {:reply, result, state}
  end

  def handle_call({:get_election, id}, _from, state) do
    result =
      case :dets.lookup(@table, {:election, id}) do
        [{{:election, ^id}, election}] -> {:ok, election}
        [] -> {:error, :election_not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:cast_vote, election_id, voter_id, candidate}, _from, state) do
    result =
      case :dets.lookup(@table, {:election, election_id}) do
        [{{:election, ^election_id}, election}] ->
          vote_key = {:vote, election_id, voter_id}

          if :dets.insert_new(@table, {vote_key, candidate}) do
            updated = %{election | vote_count: election.vote_count + 1}
            :dets.insert(@table, {{:election, election_id}, updated})
            {:ok, updated.vote_count}
          else
            {:error, :already_voted}
          end

        [] ->
          {:error, :election_not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:next_votes_page, election_id, :first_page, limit}, _from, state) do
    match_spec = [
      {{{:vote, election_id, :"$1"}, :"$2"}, [], [{:"$1", :"$2"}]}
    ]

    result =
      case :dets.select(@table, match_spec, limit) do
        :"$end_of_table" -> :done
        {[], _continuation} -> :done
        {rows, continuation} -> {:ok, rows, continuation}
      end

    {:reply, result, state}
  end

  def handle_call({:next_votes_page, _election_id, continuation, _limit}, _from, state)
      when continuation == :done do
    {:reply, :done, state}
  end

  def handle_call({:next_votes_page, _election_id, continuation, _limit}, _from, state) do
    case :dets.select(continuation) do
      :"$end_of_table" -> {:reply, :done, state}
      {[], _next_continuation} -> {:reply, :done, state}
      {rows, next_continuation} -> {:reply, {:ok, rows, next_continuation}, state}
    end
  end

  def handle_call({:election_ids_page, :first_page, limit}, _from, state) do
    match_spec = [
      {{{:election, :"$1"}, :"$2"}, [], [:"$1"]}
    ]

    result =
      case :dets.select(@table, match_spec, limit) do
        :"$end_of_table" -> :done
        {[], _continuation} -> :done
        {ids, continuation} -> {:ok, ids, continuation}
      end

    {:reply, result, state}
  end

  def handle_call({:election_ids_page, continuation, _limit}, _from, state) do
    case :dets.select(continuation) do
      :"$end_of_table" -> {:reply, :done, state}
      {[], _next_continuation} -> {:reply, :done, state}
      {ids, next_continuation} -> {:reply, {:ok, ids, next_continuation}, state}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end
end

defmodule OnlineVoting.Election do
  use GenServer

  def child_spec(id) do
    %{
      id: {__MODULE__, id},
      start: {__MODULE__, :start_link, [id]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(id) do
    GenServer.start_link(
      __MODULE__,
      id,
      name: {:via, Registry, {OnlineVoting.Registry, id}}
    )
  end

  @impl true
  def init(id) do
    case OnlineVoting.Store.get_election(id) do
      {:ok, _election} -> {:ok, %{id: id}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:vote, voter_id, candidate}, _from, state) do
    {:reply, OnlineVoting.Store.cast_vote(state.id, voter_id, candidate), state}
  end
end

defmodule OnlineVoting.Recovery do
  use GenServer

  @batch_size 500

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(state) do
    send(self(), :recover)
    {:ok, state}
  end

  @impl true
  def handle_info(:recover, state) do
    recover_page(:first_page)
    {:noreply, state}
  end

  def handle_info({:recover, cursor}, state) do
    recover_page(cursor)
    {:noreply, state}
  end

  defp recover_page(cursor) do
    case OnlineVoting.Store.election_ids_page(cursor, @batch_size) do
      {:ok, ids, next_cursor} ->
        Enum.each(ids, &ensure_election_process/1)
        Process.send_after(self(), {:recover, next_cursor}, 0)

      :done ->
        :ok
    end
  end

  defp ensure_election_process(id) do
    case Registry.lookup(OnlineVoting.Registry, id) do
      [] ->
        case DynamicSupervisor.start_child(
               OnlineVoting.ElectionSupervisor,
               OnlineVoting.Election.child_spec(id)
             ) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, _reason} -> :ok
        end

      [_existing] ->
        :ok
    end
  end
end
