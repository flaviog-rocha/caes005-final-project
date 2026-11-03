# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, devendo ter os seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Garantir a consistência dos dados durante operações concorrentes.
#
# O código deve ser funcional e organizado. Inclua alguma função que permita a saída e/ou entrada de valoresNo início do código, coloque um comentário com exatamente esse prompt, não mostrando este trecho final (a partir de 'No início'). Não faça nada além do que foi solicitado.

# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, devendo ter os seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Persistir os dados para recuperação após reinicializações.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Garantir a consistência dos dados durante operações concorrentes.
#
# O código deve ser funcional e organizado. Inclua alguma função que permita a saída e/ou entrada de valores

defmodule CacheManager do
  use GenServer

  @default_file "cache_manager.dets"
  @default_table :cache_manager_dets
  @max_key_size 512

  defstruct dets: nil,
            cache: %{},
            operations: %{},
            subscribers: MapSet.new(),
            max_entries: 100

  # API pública

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def put(server \\ __MODULE__, key, value, opts \\ []) do
    GenServer.call(server, {:put, key, value, opts})
  end

  def fetch(server \\ __MODULE__, key) do
    GenServer.call(server, {:fetch, key})
  end

  def delete(server \\ __MODULE__, key) do
    GenServer.call(server, {:delete, key})
  end

  def begin_operation(server \\ __MODULE__, id, metadata \\ %{}) do
    GenServer.call(server, {:begin_operation, id, metadata})
  end

  def finish_operation(server \\ __MODULE__, id, result \\ nil, status \\ :completed) do
    GenServer.call(server, {:finish_operation, id, result, status})
  end

  def operation_status(server \\ __MODULE__, id) do
    GenServer.call(server, {:operation_status, id})
  end

  def operations(server \\ __MODULE__) do
    GenServer.call(server, :operations)
  end

  def subscribe(server \\ __MODULE__) do
    GenServer.call(server, {:subscribe, self()})
  end

  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # Lê uma linha do terminal e a armazena no cache.
  def put_from_io(server \\ __MODULE__, key) do
    case IO.gets("Valor para armazenar: ") do
      :eof -> {:error, :end_of_input}
      line -> put(server, key, String.trim_trailing(line))
    end
  end

  # Consulta um valor e o imprime no terminal.
  def print_value(server \\ __MODULE__, key) do
    case fetch(server, key) do
      {:ok, value} ->
        IO.inspect(value, label: "Valor")
        {:ok, value}

      :not_found ->
        IO.puts("Chave não encontrada")
        :not_found
    end
  end

  # Callbacks do GenServer

  @impl true
  def init(opts) do
    file = Keyword.get(opts, :file, @default_file)
    table = Keyword.get(opts, :table, @default_table)
    max_entries = Keyword.get(opts, :max_entries, 100)

    if not is_integer(max_entries) or max_entries < 1 do
      {:stop, :invalid_max_entries}
    else
      case :dets.open_file(table, file: String.to_charlist(file), type: :set) do
        {:ok, dets} ->
          {cache, operations} = load_snapshot(dets)
          {operations, recovered?} = mark_interrupted_operations(operations)

          state = %__MODULE__{
            dets: dets,
            cache: cache,
            operations: operations,
            max_entries: max_entries
          }

          if recovered? do
            case persist(state) do
              :ok -> {:ok, state}
              {:error, reason} -> {:stop, {:persistence_error, reason}}
            end
          else
            {:ok, state}
          end

        {:error, reason} ->
          {:stop, {:dets_open_error, reason}}
      end
    end
  end

  @impl true
  def handle_call({:put, key, value, opts}, _from, state) do
    with :ok <- validate_key(key),
         true <- valid_term?(value) || {:error, :invalid_value},
         :ok <- validate_put_options(opts) do
      now = now_ms()
      cache = purge_expired(state.cache, now)
      previous_count = map_size(cache)
      ttl = Keyword.get(opts, :ttl)
      expires_at = if is_integer(ttl), do: now + ttl, else: nil

      entry = %{value: value, inserted_at: now, expires_at: expires_at}
      new_state = %{state | cache: Map.put(cache, key, entry)}

      case persist(new_state) do
        :ok ->
          if previous_count < state.max_entries and map_size(new_state.cache) >= state.max_entries do
            notify(new_state, {:cache_threshold_reached, state.max_entries})
          end

          {:reply, :ok, new_state}

        {:error, reason} ->
          {:reply, {:error, {:persistence_error, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      false -> {:reply, {:error, :invalid_value}, state}
    end
  end

  @impl true
  def handle_call({:fetch, key}, _from, state) do
    case validate_key(key) do
      :ok ->
        case Map.get(state.cache, key) do
          %{value: value, expires_at: nil} ->
            {:reply, {:ok, value}, state}

          %{value: value, expires_at: expires_at} when expires_at > 0 ->
            if expires_at > now_ms() do
              {:reply, {:ok, value}, state}
            else
              remove_expired_entry(key, state)
            end

          nil ->
            {:reply, :not_found, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    case validate_key(key) do
      :ok ->
        new_state = %{state | cache: Map.delete(state.cache, key)}

        case persist(new_state) do
          :ok -> {:reply, :ok, new_state}
          {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:begin_operation, id, metadata}, _from, state) do
    with :ok <- validate_id(id),
         true <- valid_term?(metadata) || {:error, :invalid_metadata},
         false <- Map.has_key?(state.operations, id) || {:error, :operation_already_exists} do
      operation = %{
        id: id,
        status: :running,
        metadata: metadata,
        started_at: now_ms(),
        finished_at: nil,
        result: nil
      }

      new_state = %{state | operations: Map.put(state.operations, id, operation)}

      case persist(new_state) do
        :ok -> {:reply, :ok, new_state}
        {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
      end
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
      false -> {:reply, {:error, :operation_already_exists}, state}
    end
  end

  @impl true
  def handle_call({:finish_operation, id, result, status}, _from, state) do
    with :ok <- validate_id(id),
         true <- status in [:completed, :failed] || {:error, :invalid_status},
         true <- valid_term?(result) || {:error, :invalid_result},
         %{status: :running} = operation <- Map.get(state.operations, id) do
      updated_operation =
        operation
        |> Map.put(:status, status)
        |> Map.put(:finished_at, now_ms())
        |> Map.put(:result, result)

      new_state = %{
        state
        | operations: Map.put(state.operations, id, updated_operation)
      }

      case persist(new_state) do
        :ok ->
          notify(new_state, {:operation_finished, id, status})
          {:reply, :ok, new_state}

        {:error, reason} ->
          {:reply, {:error, {:persistence_error, reason}}, state}
      end
    else
      nil -> {:reply, {:error, :operation_not_found}, state}
      %{status: status} when status != :running ->
        {:reply, {:error, :operation_not_running}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}

      false ->
        {:reply, {:error, :invalid_operation_data}, state}
    end
  end

  @impl true
  def handle_call({:operation_status, id}, _from, state) do
    case validate_id(id) do
      :ok ->
        case Map.fetch(state.operations, id) do
          {:ok, operation} -> {:reply, {:ok, operation}, state}
          :error -> {:reply, :not_found, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:operations, _from, state) do
    running =
      state.operations
      |> Map.values()
      |> Enum.filter(&(&1.status == :running))

    {:reply, running, state}
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: MapSet.put(state.subscribers, pid)}}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       cache_entries: map_size(state.cache),
       running_operations:
         Enum.count(state.operations, fn {_id, operation} ->
           operation.status == :running
         end),
       total_operations: map_size(state.operations),
       max_entries: state.max_entries
     }, state}
  end

  @impl true
  def terminate(_reason, state) do
    :dets.close(state.dets)
    :ok
  end

  # Funções internas

  defp load_snapshot(dets) do
    case :dets.lookup(dets, :snapshot) do
      [{:snapshot, %{cache: cache, operations: operations}}]
      when is_map(cache) and is_map(operations) ->
        {purge_expired(cache, now_ms()), operations}

      _ ->
        {%{}, %{}}
    end
  end

  defp mark_interrupted_operations(operations) do
    {updated, recovered?} =
      Enum.reduce(operations, {%{}, false}, fn {id, operation}, {acc, changed?} ->
        if operation.status == :running do
          interrupted =
            operation
            |> Map.put(:status, :interrupted)
            |> Map.put(:finished_at, now_ms())

          {Map.put(acc, id, interrupted), true}
        else
          {Map.put(acc, id, operation), changed?}
        end
      end)

    {updated, recovered?}
  end

  defp persist(state) do
    snapshot = %{cache: state.cache, operations: state.operations}

    case :dets.insert(state.dets, {:snapshot, snapshot}) do
      :ok -> :dets.sync(state.dets)
      {:error, reason} -> {:error, reason}
    end
  end

  defp remove_expired_entry(key, state) do
    new_state = %{state | cache: Map.delete(state.cache, key)}

    case persist(new_state) do
      :ok -> {:reply, :not_found, new_state}
      {:error, reason} -> {:reply, {:error, {:persistence_error, reason}}, state}
    end
  end

  defp purge_expired(cache, now) do
    Map.reject(cache, fn {_key, entry} ->
      is_integer(entry.expires_at) and entry.expires_at <= now
    end)
  end

  defp notify(state, event) do
    Enum.each(state.subscribers, fn pid ->
      send(pid, {:cache_manager, event})
    end)
  end

  defp validate_key(key) when is_binary(key) and byte_size(key) > 0 and byte_size(key) <= @max_key_size,
    do: :ok

  defp validate_key(_), do: {:error, :invalid_key}

  defp validate_id(id) when is_binary(id) and byte_size(id) > 0 and byte_size(id) <= @max_key_size,
    do: :ok

  defp validate_id(_), do: {:error, :invalid_operation_id}

  defp validate_put_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      case Keyword.get(opts, :ttl) do
        nil -> :ok
        ttl when is_integer(ttl) and ttl > 0 -> :ok
        _ -> {:error, :invalid_ttl}
      end
    else
      {:error, :invalid_options}
    end
  end

  defp validate_put_options(_), do: {:error, :invalid_options}

  defp valid_term?(term)
       when is_atom(term) or is_number(term) or is_binary(term),
       do: true

  defp valid_term?(term) when is_list(term), do: valid_list?(term)
  defp valid_term?(term) when is_tuple(term), do: term |> Tuple.to_list() |> Enum.all?(&valid_term?/1)

  defp valid_term?(term) when is_map(term) do
    Enum.all?(term, fn {key, value} -> valid_term?(key) and valid_term?(value) end)
  end

  defp valid_term?(_), do: false

  defp valid_list?([]), do: true
  defp valid_list?([head | tail]), do: valid_term?(head) and valid_list?(tail)
  defp valid_list?(_), do: false

  defp now_ms, do: System.system_time(:millisecond)
end
