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

defmodule Voting do
  use Supervisor

  @registry Voting.Registry
  @catalog Voting.Catalog

  def start_link(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, Path.join(File.cwd!(), "voting_data"))
    Supervisor.start_link(__MODULE__, data_dir, name: __MODULE__)
  end

  @impl true
  def init(data_dir) do
    children = [
      {Voting.Persistence, data_dir},
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Voting.PollSupervisor},
      {Voting.Catalog, []}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def create_poll(id, options) when is_binary(id) and is_list(options) do
    with :ok <- validate_id(id),
         :ok <- validate_options(options) do
      GenServer.call(@catalog, {:create_poll, id, options}, :infinity)
    end
  end

  def create_poll(_, _), do: {:error, :invalid_poll}

  def cast_vote(id, voter_id, option)
      when is_binary(id) and is_binary(voter_id) and is_binary(option) do
    with :ok <- validate_id(id),
         :ok <- validate_id(voter_id) do
      GenServer.call(via_poll(id), {:cast_vote, voter_id, option}, :infinity)
    end
  end

  def cast_vote(_, _, _), do: {:error, :invalid_vote}

  def results(id) when is_binary(id) do
    GenServer.call(via_poll(id), :results, :infinity)
  end

  def results(_), do: {:error, :invalid_poll}

  def stream_votes(id, batch_size \\ 1_000)

  def stream_votes(id, batch_size)
      when is_binary(id) and is_integer(batch_size) and batch_size > 0 do
    case Registry.lookup(@registry, id) do
      [] ->
        {:error, :not_found}

      [_] ->
        stream =
          Stream.resource(
            fn -> :start end,
            fn
              :done ->
                {:halt, :done}

              cursor ->
                {rows, next_cursor} =
                  GenServer.call(Voting.Persistence, {:vote_batch, id, cursor, batch_size}, :infinity)

                {rows, next_cursor}
            end,
            fn _ -> :ok end
          )

        {:ok, stream}
    end
  end

  def stream_votes(_, _), do: {:error, :invalid_batch_size}

  defp via_poll(id), do: {:via, Registry, {@registry, id}}

  defp validate_id(id) do
    if byte_size(id) > 0, do: :ok, else: {:error, :invalid_id}
  end

  defp validate_options(options) do
    if options != [] and Enum.all?(options, &(is_binary(&1) and byte_size(&1) > 0)) and
         length(options) == length(Enum.uniq(options)) do
      :ok
    else
      {:error, :invalid_options}
    end
  end
end

defmodule Voting.Persistence do
  use GenServer

  @table :voting_data

  def start_link(data_dir) do
    GenServer.start_link(__MODULE__, data_dir, name: __MODULE__)
  end

  @impl true
  def init(data_dir) do
    File.mkdir_p!(data_dir)
    file = Path.join(data_dir, "voting.dets")

    case :dets.open_file(@table,
           file: String.to_charlist(file),
           type: :set,
           repair: true
         ) do
      {:ok, @table} -> {:ok, %{}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:vote_batch, id, cursor, limit}, _from, state) do
    first_key =
      case cursor do
        :start -> :dets.first(@table)
        key -> :dets.next(@table, key)
      end

    {rows, next_cursor} = collect_votes(id, first_key, limit, 0, [])
    {:reply, {rows, next_cursor}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  catch
    _, _ -> :ok
  end

  defp collect_votes(_id, :"$end_of_table", _limit, _count, acc) do
    {Enum.reverse(acc), :done}
  end

  defp collect_votes(id, key, limit, count, acc) do
    next_key = :dets.next(@table, key)

    case key do
      {:vote, ^id, voter_id} ->
        case :dets.lookup(@table, key) do
          [{^key, option}] ->
            next_acc = [{voter_id, option} | acc]
            next_count = count + 1

            if next_count >= limit do
              next_cursor =
                if next_key == :"$end_of_table", do: :done, else: next_key

              {Enum.reverse(next_acc), next_cursor}
            else
              collect_votes(id, next_key, limit, next_count, next_acc)
            end

          _ ->
            collect_votes(id, next_key, limit, count, acc)
        end

      _ ->
        collect_votes(id, next_key, limit, count, acc)
    end
  end
end

defmodule Voting.Poll do
  use GenServer

  @table :voting_data

  def start_link({id, options}) do
    GenServer.start_link(__MODULE__, {id, options}, name: {:via, Registry, {Voting.Registry, id}})
  end

  @impl true
  def init({id, options}) do
    {:ok, %{id: id, options: options}}
  end

  @impl true
  def handle_call({:cast_vote, voter_id, option}, _from, state) do
    cond do
      option not in state.options ->
        {:reply, {:error, :invalid_option}, state}

      true ->
        key = {:vote, state.id, voter_id}

        case :dets.insert_new(@table, {key, option}) do
          true ->
            case :dets.sync(@table) do
              :ok -> {:reply, :ok, state}
              {:error, reason} -> {:reply, {:error, reason}, state}
            end

          false ->
            {:reply, {:error, :already_voted}, state}
        end
    end
  end

  def handle_call(:results, _from, state) do
    counts = Map.new(state.options, &{&1, 0})

    results =
      :dets.foldl(
        fn
          {{:vote, id, _voter_id}, option}, acc when id == state.id ->
            Map.update(acc, option, 1, &(&1 + 1))

          _, acc ->
            acc
        end,
        counts,
        @table
      )

    {:reply, results, state}
  end
end

defmodule Voting.Catalog do
  use GenServer

  @table :voting_data

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    polls =
      :dets.foldl(
        fn
          {{:poll, id}, options}, acc -> [{id, options} | acc]
          _, acc -> acc
        end,
        [],
        @table
      )

    case Enum.reduce_while(polls, :ok, fn {id, options}, :ok ->
           case start_poll(id, options) do
             :ok -> {:cont, :ok}
             {:error, reason} -> {:halt, {:error, {id, reason}}}
           end
         end) do
      :ok -> {:ok, %{}}
      {:error, reason} -> {:stop, {:poll_recovery_failed, reason}}
    end
  end

  @impl true
  def handle_call({:create_poll, id, options}, _from, state) do
    key = {:poll, id}

    case :dets.lookup(@table, key) do
      [_] ->
        {:reply, {:error, :already_exists}, state}

      [] ->
        case :dets.insert(@table, {key, options}) do
          :ok ->
            case :dets.sync(@table) do
              :ok ->
                case start_poll(id, options) do
                  :ok ->
                    {:reply, :ok, state}

                  {:error, reason} ->
                    :dets.delete(@table, key)
                    :dets.sync(@table)
                    {:reply, {:error, reason}, state}
                end

              {:error, reason} ->
                {:reply, {:error, reason}, state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp start_poll(id, options) do
    case Registry.lookup(Voting.Registry, id) do
      [_] ->
        :ok

      [] ->
        case DynamicSupervisor.start_child(
               Voting.PollSupervisor,
               {Voting.Poll, {id, options}}
             ) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end
end
