# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de transações financeiras, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Finance.Transactions do
  use GenServer

  @allowed_fields [:amount, :currency, :description]

  # amount é expresso em unidades monetárias mínimas, por exemplo, centavos.
  def start_link(opts \\ []) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  def create(server, attrs), do: GenServer.call(server, {:create, attrs})
  def get(server, id), do: GenServer.call(server, {:get, id})
  def list(server), do: GenServer.call(server, :list)
  def update(server, id, attrs), do: GenServer.call(server, {:update, id, attrs})
  def delete(server, id), do: GenServer.call(server, {:delete, id})
  def process(server, id), do: GenServer.call(server, {:process, id})
  def reconcile(server), do: GenServer.call(server, :reconcile)
  def reconciliation(server), do: GenServer.call(server, :get_reconciliation)

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)
    processing_delay = Keyword.get(opts, :processing_delay, 0)
    periodic_interval = Keyword.get(opts, :periodic_interval, 60_000)

    unless is_integer(max_concurrency) and max_concurrency > 0 do
      raise ArgumentError, ":max_concurrency deve ser um inteiro positivo"
    end

    unless is_integer(processing_delay) and processing_delay >= 0 do
      raise ArgumentError, ":processing_delay deve ser um inteiro não negativo"
    end

    unless is_nil(periodic_interval) or
             (is_integer(periodic_interval) and periodic_interval > 0) do
      raise ArgumentError, ":periodic_interval deve ser nil ou um inteiro positivo"
    end

    {:ok, supervisor} = Task.Supervisor.start_link()

    state = %{
      records: %{},
      next_id: 1,
      active: MapSet.new(),
      max_concurrency: max_concurrency,
      processing_delay: processing_delay,
      periodic_interval: periodic_interval,
      supervisor: supervisor,
      reconciliation: nil
    }

    schedule_reconciliation(periodic_interval)
    {:ok, state}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    with {:ok, fields} <- validate_fields(attrs, true) do
      now = DateTime.utc_now()

      record =
        Map.merge(fields, %{
          id: state.next_id,
          status: :pending,
          inserted_at: now,
          updated_at: now
        })

      next_state = %{
        state
        | records: Map.put(state.records, record.id, record),
          next_id: state.next_id + 1
      }

      {:reply, {:ok, record}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.records, id) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list, _from, state) do
    records =
      state.records
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    {:reply, records, state}
  end

  def handle_call({:update, id, attrs}, _from, state) do
    case Map.fetch(state.records, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, record} ->
        current_fields = Map.take(record, @allowed_fields)

        with {:ok, changes} <- validate_fields(attrs, false),
             {:ok, fields} <- validate_fields(Map.merge(current_fields, changes), true) do
          updated = Map.merge(record, fields) |> Map.put(:updated_at, DateTime.utc_now())
          next_state = %{state | records: Map.put(state.records, id, updated)}
          {:reply, {:ok, updated}, next_state}
        else
          {:error, reason} -> {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:delete, id}, _from, state) do
    case Map.fetch(state.records, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: :processing}} ->
        {:reply, {:error, :processing}, state}

      {:ok, record} ->
        {:reply, {:ok, record}, %{state | records: Map.delete(state.records, id)}}
    end
  end

  def handle_call({:process, id}, _from, state) do
    case Map.fetch(state.records, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, %{status: status}} when status != :pending ->
        {:reply, {:error, {:invalid_status, status}}, state}

      {:ok, _record} when map_size(state.active.map) >= state.max_concurrency ->
        {:reply, {:error, :capacity_reached}, state}

      {:ok, record} ->
        parent = self()
        delay = state.processing_delay

        case Task.Supervisor.start_child(state.supervisor, fn ->
               if delay > 0, do: Process.sleep(delay)
               send(parent, {:processed, id, :ok})
             end) do
          {:ok, _pid} ->
            updated = Map.put(record, :status, :processing)

            next_state = %{
              state
              | records: Map.put(state.records, id, updated),
                active: MapSet.put(state.active, id)
            }

            {:reply, :ok, next_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:reconcile, _from, state) do
    {summary, next_state} = build_reconciliation(state)
    {:reply, summary, next_state}
  end

  def handle_call(:get_reconciliation, _from, state) do
    {:reply, state.reconciliation, state}
  end

  @impl true
  def handle_info({:processed, id, :ok}, state) do
    next_state =
      case Map.fetch(state.records, id) do
        {:ok, %{status: :processing} = record} ->
          updated = Map.put(record, :status, :completed)
          %{state | records: Map.put(state.records, id, updated), active: MapSet.delete(state.active, id)}

        _ ->
          %{state | active: MapSet.delete(state.active, id)}
      end

    {:noreply, next_state}
  end

  def handle_info(:reconcile, state) do
    {_summary, next_state} = build_reconciliation(state)
    schedule_reconciliation(state.periodic_interval)
    {:noreply, next_state}
  end

  defp validate_fields(attrs, require_amount) when is_map(attrs) do
    unknown_fields = Map.keys(attrs) -- @allowed_fields

    cond do
      unknown_fields != [] ->
        {:error, {:unknown_fields, unknown_fields}}

      require_amount and not Map.has_key?(attrs, :amount) ->
        {:error, :amount_required}

      true ->
        amount = Map.get(attrs, :amount)
        currency = Map.get(attrs, :currency, "USD")
        description = Map.get(attrs, :description, "")

        cond do
          not is_integer(amount) or amount <= 0 ->
            {:error, :invalid_amount}

          not is_binary(currency) or not Regex.match?(~r/\A[A-Z]{3}\z/, currency) ->
            {:error, :invalid_currency}

          not is_binary(description) ->
            {:error, :invalid_description}

          true ->
            {:ok, %{amount: amount, currency: currency, description: description}}
        end
    end
  end

  defp validate_fields(_attrs, _require_amount), do: {:error, :invalid_attributes}

  defp build_reconciliation(state) do
    completed =
      state.records
      |> Map.values()
      |> Enum.filter(&(&1.status == :completed))

    totals =
      Enum.reduce(completed, %{}, fn record, acc ->
        Map.update(acc, record.currency, record.amount, &(&1 + record.amount))
      end)

    summary = %{
      at: DateTime.utc_now(),
      completed_count: length(completed),
      totals_by_currency: totals
    }

    {summary, %{state | reconciliation: summary}}
  end

  defp schedule_reconciliation(nil), do: :ok

  defp schedule_reconciliation(interval) do
    Process.send_after(self(), :reconcile, interval)
    :ok
  end
end
