# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  use GenServer

  defmodule Rule do
    @enforce_keys [:id, :name, :condition, :threshold, :message, :enabled]
    defstruct [:id, :name, :condition, :threshold, :message, :enabled]
  end

  defmodule Notification do
    @enforce_keys [:id, :rule_id, :message, :value, :triggered_at]
    defstruct [:id, :rule_id, :message, :value, :triggered_at]
  end

  defmodule HistoryEntry do
    @enforce_keys [:id, :operation, :entity, :entity_id, :details, :occurred_at]
    defstruct [:id, :operation, :entity, :entity_id, :details, :occurred_at]
  end

  @conditions [:gt, :gte, :lt, :lte, :eq]

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: name)
  end

  def stop(server \\ __MODULE__), do: GenServer.stop(server)

  def create_rule(attrs, server \\ __MODULE__),
    do: GenServer.call(server, {:create_rule, attrs})

  def get_rule(id, server \\ __MODULE__),
    do: GenServer.call(server, {:get_rule, id})

  def list_rules(server \\ __MODULE__),
    do: GenServer.call(server, :list_rules)

  def update_rule(id, attrs, server \\ __MODULE__),
    do: GenServer.call(server, {:update_rule, id, attrs})

  def delete_rule(id, server \\ __MODULE__),
    do: GenServer.call(server, {:delete_rule, id})

  def evaluate(id, value, server \\ __MODULE__),
    do: GenServer.call(server, {:evaluate, id, value})

  def list_notifications(server \\ __MODULE__),
    do: GenServer.call(server, :list_notifications)

  def get_notification(id, server \\ __MODULE__),
    do: GenServer.call(server, {:get_notification, id})

  def history(server \\ __MODULE__),
    do: GenServer.call(server, :history)

  @impl true
  def init(:ok) do
    {:ok,
     %{
       rules: %{},
       notifications: %{},
       history: [],
       next_rule_id: 1,
       next_notification_id: 1,
       next_history_id: 1
     }}
  end

  @impl true
  def handle_call({:create_rule, attrs}, _from, state) do
    case build_rule(attrs, state.next_rule_id) do
      {:ok, rule} ->
        state =
          state
          |> Map.update!(:rules, &Map.put(&1, rule.id, rule))
          |> Map.update!(:next_rule_id, &(&1 + 1))
          |> log(:create, :rule, rule.id, %{status: :ok})

        {:reply, {:ok, rule}, state}

      {:error, reason} ->
        state = log(state, :create, :rule, nil, %{status: :error, reason: reason})
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_rule, id}, _from, state) do
    case Map.fetch(state.rules, id) do
      {:ok, rule} ->
        state = log(state, :read, :rule, id, %{status: :ok})
        {:reply, {:ok, rule}, state}

      :error ->
        state = log(state, :read, :rule, id, %{status: :error, reason: :not_found})
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_rules, _from, state) do
    rules = state.rules |> Map.values() |> Enum.sort_by(& &1.id)
    state = log(state, :list, :rule, nil, %{status: :ok, count: length(rules)})
    {:reply, rules, state}
  end

  def handle_call({:update_rule, id, attrs}, _from, state) do
    case Map.fetch(state.rules, id) do
      {:ok, existing} ->
        case update_rule_record(existing, attrs) do
          {:ok, rule} ->
            state =
              state
              |> Map.update!(:rules, &Map.put(&1, id, rule))
              |> log(:update, :rule, id, %{status: :ok})

            {:reply, {:ok, rule}, state}

          {:error, reason} ->
            state = log(state, :update, :rule, id, %{status: :error, reason: reason})
            {:reply, {:error, reason}, state}
        end

      :error ->
        state = log(state, :update, :rule, id, %{status: :error, reason: :not_found})
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_rule, id}, _from, state) do
    case Map.pop(state.rules, id) do
      {nil, _rules} ->
        state = log(state, :delete, :rule, id, %{status: :error, reason: :not_found})
        {:reply, {:error, :not_found}, state}

      {rule, rules} ->
        state = %{state | rules: rules}
        state = log(state, :delete, :rule, id, %{status: :ok})
        {:reply, {:ok, rule}, state}
    end
  end

  def handle_call({:evaluate, id, value}, _from, state) do
    case Map.fetch(state.rules, id) do
      :error ->
        state = log(state, :evaluate, :rule, id, %{status: :error, reason: :not_found})
        {:reply, {:error, :not_found}, state}

      {:ok, %Rule{enabled: false}} ->
        state = log(state, :evaluate, :rule, id, %{status: :skipped, reason: :disabled})
        {:reply, {:ok, :disabled}, state}

      {:ok, rule} when not is_number(value) ->
        state = log(state, :evaluate, :rule, id, %{status: :error, reason: :invalid_value})
        {:reply, {:error, :invalid_value}, state}

      {:ok, rule} ->
        if condition_met?(rule.condition, value, rule.threshold) do
          notification = %Notification{
            id: state.next_notification_id,
            rule_id: rule.id,
            message: rule.message,
            value: value,
            triggered_at: DateTime.utc_now()
          }

          state =
            state
            |> Map.update!(:notifications, &Map.put(&1, notification.id, notification))
            |> Map.update!(:next_notification_id, &(&1 + 1))
            |> log(:trigger, :notification, notification.id, %{
              status: :ok,
              rule_id: rule.id
            })

          {:reply, {:ok, notification}, state}
        else
          state =
            log(state, :evaluate, :rule, id, %{
              status: :not_triggered,
              value: value
            })

          {:reply, {:ok, :not_triggered}, state}
        end
    end
  end

  def handle_call(:list_notifications, _from, state) do
    notifications =
      state.notifications
      |> Map.values()
      |> Enum.sort_by(& &1.id)

    state =
      log(state, :list, :notification, nil, %{
        status: :ok,
        count: length(notifications)
      })

    {:reply, notifications, state}
  end

  def handle_call({:get_notification, id}, _from, state) do
    case Map.fetch(state.notifications, id) do
      {:ok, notification} ->
        state = log(state, :read, :notification, id, %{status: :ok})
        {:reply, {:ok, notification}, state}

      :error ->
        state =
          log(state, :read, :notification, id, %{
            status: :error,
            reason: :not_found
          })

        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  defp build_rule(attrs, id) when is_map(attrs) do
    name = Map.get(attrs, :name)
    condition = Map.get(attrs, :condition)
    threshold = Map.get(attrs, :threshold)
    message = Map.get(attrs, :message, name)
    enabled = Map.get(attrs, :enabled, true)

    validate_rule(%Rule{
      id: id,
      name: name,
      condition: condition,
      threshold: threshold,
      message: message,
      enabled: enabled
    })
  end

  defp build_rule(_attrs, _id), do: {:error, :invalid_attrs}

  defp update_rule_record(%Rule{} = existing, attrs) when is_map(attrs) do
    allowed = [:name, :condition, :threshold, :message, :enabled]
    changes = Map.take(attrs, allowed)
    existing |> Map.from_struct() |> Map.merge(changes) |> then(&struct!(Rule, &1)) |> validate_rule()
  end

  defp update_rule_record(_existing, _attrs), do: {:error, :invalid_attrs}

  defp validate_rule(%Rule{} = rule) do
    cond do
      not (is_binary(rule.name) and String.trim(rule.name) != "") ->
        {:error, :invalid_name}

      rule.condition not in @conditions ->
        {:error, :invalid_condition}

      not is_number(rule.threshold) ->
        {:error, :invalid_threshold}

      not (is_binary(rule.message) and String.trim(rule.message) != "") ->
        {:error, :invalid_message}

      not is_boolean(rule.enabled) ->
        {:error, :invalid_enabled}

      true ->
        {:ok, rule}
    end
  end

  defp condition_met?(:gt, value, threshold), do: value > threshold
  defp condition_met?(:gte, value, threshold), do: value >= threshold
  defp condition_met?(:lt, value, threshold), do: value < threshold
  defp condition_met?(:lte, value, threshold), do: value <= threshold
  defp condition_met?(:eq, value, threshold), do: value == threshold

  defp log(state, operation, entity, entity_id, details) do
    entry = %HistoryEntry{
      id: state.next_history_id,
      operation: operation,
      entity: entity,
      entity_id: entity_id,
      details: details,
      occurred_at: DateTime.utc_now()
    }

    state
    |> Map.update!(:history, &[entry | &1])
    |> Map.update!(:next_history_id, &(&1 + 1))
  end
end
