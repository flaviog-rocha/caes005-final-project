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
  use GenServer

  @table :voting_system_records
  @default_cleanup_interval 60_000
  @allowed_create_fields [:question, :options, :expires_at]
  @allowed_update_fields [:question, :options, :expires_at]

  # API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def create(attrs), do: GenServer.call(__MODULE__, {:create, attrs})
  def get(id), do: GenServer.call(__MODULE__, {:get, id})
  def list, do: GenServer.call(__MODULE__, :list)
  def update(id, attrs), do: GenServer.call(__MODULE__, {:update, id, attrs})
  def delete(id), do: GenServer.call(__MODULE__, {:delete, id})
  def vote(id, option), do: GenServer.call(__MODULE__, {:vote, id, option})

  # Supervisor

  def start_supervised(opts \\ []) do
    Supervisor.start_link(
      [{__MODULE__, opts}],
      strategy: :one_for_one,
      name: VotingSystem.Supervisor
    )
  end

  # GenServer callbacks

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :file, "voting_system.dets")
    cleanup_interval = Keyword.get(opts, :cleanup_interval, @default_cleanup_interval)

    case :dets.open_file(@table,
           file: to_charlist(path),
           type: :set,
           repair: true
         ) do
      {:ok, @table} ->
        cleanup_expired()
        schedule_cleanup(cleanup_interval)
        {:ok, %{cleanup_interval: cleanup_interval}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with :ok <- validate_fields(attrs, @allowed_create_fields),
         {:ok, question} <- required_string(attrs, :question),
         {:ok, options} <- validate_options(Map.get(attrs, :options)),
         {:ok, expires_at} <- validate_expiration(Map.get(attrs, :expires_at)) do
      id = new_id()

      record = %{
        id: id,
        question: question,
        options: options,
        votes: Map.new(options, &{&1, 0}),
        expires_at: expires_at,
        created_at: now()
      }

      case persist(record) do
        :ok -> {:reply, {:ok, record}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case lookup_active(id) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :not_found -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list, _from, state) do
    records =
      @table
      |> :dets.foldl(fn
        {_id, record}, acc when is_map(record) ->
          if expired?(record), do: acc, else: [record | acc]

        _, acc ->
          acc
      end, [])
      |> Enum.sort_by(& &1.created_at)

    cleanup_expired()
    {:reply, {:ok, records}, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    with :ok <- validate_fields(attrs, @allowed_update_fields),
         {:ok, current} <- active_record(id),
         {:ok, updated} <- build_updated_record(current, attrs) do
      case persist(updated) do
        :ok -> {:reply, {:ok, updated}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      :not_found -> {:reply, {:error, :not_found}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case :dets.delete(@table, id) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> {:reply, :ok, state}
          {:error, reason} -> {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:vote, id, option}, _from, state) do
    with {:ok, current} <- active_record(id),
         true <- option in current.options do
      updated = update_in(current.votes[option], &(&1 + 1))

      case persist(updated) do
        :ok -> {:reply, {:ok, updated}, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    else
      :not_found -> {:reply, {:error, :not_found}, state}
      false -> {:reply, {:error, :invalid_option}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(:expire, state) do
    cleanup_expired()
    schedule_cleanup(state.cleanup_interval)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
    :ok
  end

  # Validation and record operations

  defp build_updated_record(current, attrs) do
    question_result =
      if Map.has_key?(attrs, :question),
        do: required_string(attrs, :question),
        else: {:ok, current.question}

    options_result =
      if Map.has_key?(attrs, :options),
        do: validate_options(Map.get(attrs, :options)),
        else: {:ok, current.options}

    expiration_result =
      if Map.has_key?(attrs, :expires_at),
        do: validate_expiration(Map.get(attrs, :expires_at)),
        else: {:ok, current.expires_at}

    with {:ok, question} <- question_result,
         {:ok, options} <- options_result,
         {:ok, expires_at} <- expiration_result,
         :ok <- validate_option_change(current, options) do
      votes =
        if options == current.options do
          current.votes
        else
          Map.new(options, &{&1, 0})
        end

      {:ok,
       %{current | question: question, options: options, votes: votes, expires_at: expires_at}}
    end
  end

  defp validate_option_change(current, options) do
    if options != current.options and Enum.any?(Map.values(current.votes), &(&1 > 0)) do
      {:error, :cannot_change_options_after_voting}
    else
      :ok
    end
  end

  defp validate_fields(attrs, allowed) when is_map(attrs) do
    if Enum.all?(Map.keys(attrs), &(&1 in allowed)),
      do: :ok,
      else: {:error, :invalid_fields}
  end

  defp validate_fields(_, _), do: {:error, :invalid_attributes}

  defp required_string(attrs, field) do
    case Map.get(attrs, field) do
      value when is_binary(value) ->
        if String.trim(value) == "", do: {:error, {:invalid_field, field}}, else: {:ok, value}

      _ ->
        {:error, {:invalid_field, field}}
    end
  end

  defp validate_options(options)
       when is_list(options) and length(options) >= 2 do
    valid =
      Enum.all?(options, fn
        option when is_binary(option) -> String.trim(option) != ""
        _ -> false
      end)

    if valid and length(Enum.uniq(options)) == length(options),
      do: {:ok, options},
      else: {:error, {:invalid_field, :options}}
  end

  defp validate_options(_), do: {:error, {:invalid_field, :options}}

  defp validate_expiration(expires_at)
       when is_integer(expires_at) and expires_at > 0 do
    if expires_at > now(),
      do: {:ok, expires_at},
      else: {:error, :expiration_must_be_in_the_future}
  end

  defp validate_expiration(_), do: {:error, {:invalid_field, :expires_at}}

  defp active_record(id) do
    case lookup_active(id) do
      {:ok, record} -> {:ok, record}
      :not_found -> :not_found
    end
  end

  defp lookup_active(id) do
    case :dets.lookup(@table, id) do
      [{^id, record}] when is_map(record) ->
        if expired?(record) do
          :dets.delete(@table, id)
          :dets.sync(@table)
          :not_found
        else
          {:ok, record}
        end

      _ ->
        :not_found
    end
  end

  # A complete record is written as one DETS object, after all validation succeeds.
  # GenServer serialization prevents concurrent operations from observing partial updates.
  defp persist(record) do
    case :dets.insert(@table, {record.id, record}) do
      :ok ->
        case :dets.sync(@table) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cleanup_expired do
    expired_ids =
      :dets.foldl(
        fn
          {id, record}, acc when is_map(record) ->
            if expired?(record), do: [id | acc], else: acc

          _, acc ->
            acc
        end,
        [],
        @table
      )

    if expired_ids != [] do
      Enum.each(expired_ids, &:dets.delete(@table, &1))
      :dets.sync(@table)
    end

    :ok
  end

  defp expired?(%{expires_at: expires_at}) when is_integer(expires_at), do: expires_at <= now()
  defp expired?(_), do: true

  defp schedule_cleanup(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :expire, interval)
  end

  defp schedule_cleanup(_), do: Process.send_after(self(), :expire, @default_cleanup_interval)

  defp new_id do
    :crypto.strong_rand_bytes(16)
    |> Base.url_encode64(padding: false)
  end

  defp now, do: System.system_time(:millisecond)
end
