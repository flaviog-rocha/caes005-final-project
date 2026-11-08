# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Implementar expiração automática de registros.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  use Supervisor

  @default_ttl 3_600

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    worker_opts = Keyword.take(opts, [:file, :sweep_interval])
    children = [{VotingSystem.Store, worker_opts}]
    Supervisor.init(children, strategy: :one_for_one)
  end

  def create(title, options, ttl_seconds \\ @default_ttl) do
    GenServer.call(VotingSystem.Store, {:create, title, options, ttl_seconds})
  end

  def read(id), do: GenServer.call(VotingSystem.Store, {:read, id})
  def update(id, attrs), do: GenServer.call(VotingSystem.Store, {:update, id, attrs})
  def delete(id), do: GenServer.call(VotingSystem.Store, {:delete, id})
  def list, do: GenServer.call(VotingSystem.Store, :list)
  def cast_vote(id, option), do: GenServer.call(VotingSystem.Store, {:cast_vote, id, option})
end

defmodule VotingSystem.Store do
  use GenServer

  @table :voting_system_records
  @default_file "voting_system.dets"
  @default_sweep_interval 1_000
  @default_ttl 3_600
  @allowed_updates [:title, :options, :ttl_seconds]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    file = opts |> Keyword.get(:file, @default_file) |> to_string()
    interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)

    with true <- is_integer(interval) and interval > 0,
         :ok <- File.mkdir_p(Path.dirname(file)),
         {:ok, table} <-
           :dets.open_file(@table,
             file: String.to_charlist(file),
             type: :set,
             repair: true
           ) do
      send(self(), :expire_records)
      {:ok, %{table: table, sweep_interval: interval}}
    else
      false -> {:stop, :invalid_sweep_interval}
      {:error, reason} -> {:stop, reason}
      other -> {:stop, other}
    end
  end

  @impl true
  def handle_call({:create, title, options, ttl_seconds}, _from, state) do
    with {:ok, title} <- validate_title(title),
         {:ok, options} <- validate_options(options),
         :ok <- validate_ttl(ttl_seconds) do
      id = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
      now = System.system_time(:second)

      record = %{
        id: id,
        title: title,
        options: options,
        votes: Map.new(options, &{&1, 0}),
        created_at: now,
        expires_at: now + ttl_seconds
      }

      case persist_record(record, nil) do
        :ok -> {:reply, {:ok, record}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:read, id}, _from, state) do
    case fetch_record(id) do
      {:ok, record} ->
        if expired?(record) do
          case delete_persisted(record) do
            :ok -> {:reply, {:error, :not_found}, state}
            {:error, reason} -> {:reply, {:error, reason}, state}
          end
        else
          {:reply, {:ok, record}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with {:ok, current} <- fetch_record(id),
         :ok <- ensure_not_expired(current),
         {:ok, updated} <- build_updated_record(current, attrs),
         :ok <- persist_record(updated, current) do
      {:reply, {:ok, updated}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case fetch_record(id) do
      {:ok, record} ->
        case delete_persisted(record) do
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:list, _from, state) do
    case purge_expired() do
      :ok ->
        records = :dets.foldl(fn {_id, record}, acc -> [record | acc] end, [], @table)
        {:reply, {:ok, Enum.reverse(records)}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cast_vote, id, option}, _from, state) do
    with {:ok, record} <- fetch_record(id),
         :ok <- ensure_not_expired(record),
         true <- option in record.options do
      updated = update_in(record, [:votes, option], &(&1 + 1))

      case persist_record(updated, record) do
        :ok -> {:reply, {:ok, updated}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      false -> {:reply, {:error, :invalid_option}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:expire_records, state) do
    _ = purge_expired()
    Process.send_after(self(), :expire_records, state.sweep_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  catch
    _, _ -> :ok
  end

  defp fetch_record(id) when is_binary(id) do
    case :dets.lookup(@table, id) do
      [{^id, record}] -> {:ok, record}
      [] -> {:error, :not_found}
    end
  end

  defp fetch_record(_), do: {:error, :invalid_id}

  defp build_updated_record(current, attrs) when is_map(attrs) do
    keys = Map.keys(attrs)

    cond do
      keys == [] ->
        {:error, :empty_update}

      Enum.any?(keys, &(&1 not in @allowed_updates)) ->
        {:error, :invalid_attributes}

      true ->
        with {:ok, title} <- validate_title(Map.get(attrs, :title, current.title)),
             {:ok, options} <- validate_options(Map.get(attrs, :options, current.options)),
             ttl_seconds = Map.get(attrs, :ttl_seconds),
             :ok <- if(is_nil(ttl_seconds), do: :ok, else: validate_ttl(ttl_seconds)) do
          now = System.system_time(:second)
          votes = Map.new(options, &{&1, Map.get(current.votes, &1, 0)})

          updated = %{
            current
            | title: title,
              options: options,
              votes: votes,
              expires_at:
                if(is_nil(ttl_seconds), do: current.expires_at, else: now + ttl_seconds)
          }

          {:ok, updated}
        end
    end
  end

  defp build_updated_record(_current, _attrs), do: {:error, :invalid_attributes}

  defp validate_title(title) when is_binary(title) do
    title = String.trim(title)
    if title == "", do: {:error, :invalid_title}, else: {:ok, title}
  end

  defp validate_title(_), do: {:error, :invalid_title}

  defp validate_options(options) when is_list(options) do
    normalized =
      Enum.map(options, fn
        option when is_binary(option) -> String.trim(option)
        _ -> nil
      end)

    cond do
      length(normalized) < 2 ->
        {:error, :at_least_two_options_required}

      Enum.any?(normalized, &(&1 in [nil, ""])) ->
        {:error, :invalid_options}

      length(Enum.uniq(normalized)) != length(normalized) ->
        {:error, :duplicate_options}

      true ->
        {:ok, normalized}
    end
  end

  defp validate_options(_), do: {:error, :invalid_options}

  defp validate_ttl(ttl) when is_integer(ttl) and ttl > 0, do: :ok
  defp validate_ttl(_), do: {:error, :invalid_ttl}

  defp ensure_not_expired(record) do
    if expired?(record), do: {:error, :not_found}, else: :ok
  end

  defp expired?(record), do: record.expires_at <= System.system_time(:second)

  defp persist_record(record, previous) do
    case :dets.insert(@table, {record.id, record}) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> :ok
          error ->
            restore_record(previous, record.id)
            {:error, {:storage_error, error}}
        end

      error ->
        {:error, {:storage_error, error}}
    end
  rescue
    error -> {:error, {:storage_error, error}}
  catch
    kind, reason -> {:error, {:storage_error, {kind, reason}}}
  end

  defp delete_persisted(record) do
    case :dets.delete(@table, record.id) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> :ok
          error ->
            restore_record(record, record.id)
            {:error, {:storage_error, error}}
        end

      error ->
        {:error, {:storage_error, error}}
    end
  rescue
    error -> {:error, {:storage_error, error}}
  catch
    kind, reason -> {:error, {:storage_error, {kind, reason}}}
  end

  defp restore_record(nil, id) do
    :dets.delete(@table, id)
    :dets.sync(@table)
  end

  defp restore_record(record, _id) do
    :dets.insert(@table, {record.id, record})
    :dets.sync(@table)
  end

  defp purge_expired do
    expired =
      :dets.foldl(
        fn {_id, record}, acc -> if expired?(record), do: [record | acc], else: acc end,
        [],
        @table
      )

    Enum.reduce_while(expired, :ok, fn record, :ok ->
      case delete_persisted(record) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end
