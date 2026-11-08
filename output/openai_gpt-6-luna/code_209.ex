# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir cadastrar, consultar, atualizar e remover registros.
# 4. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationManager.Rule do
  @moduledoc false

  defstruct [
    :id,
    :title,
    :message,
    :condition,
    active: true,
    inserted_at: nil,
    updated_at: nil
  ]
end

defmodule NotificationManager.Notification do
  @moduledoc false

  defstruct [
    :id,
    :rule_id,
    :title,
    :message,
    :source,
    :payload,
    status: :unread,
    inserted_at: nil,
    updated_at: nil
  ]
end

defmodule NotificationManager do
  @moduledoc """
  Gerenciador de notificações em memória. Os dados são mantidos pelo processo
  GenServer e podem ser substituídos por um repositório ou banco de dados.
  """

  use GenServer

  alias NotificationManager.Rule
  alias NotificationManager.Notification

  @operators [:eq, :neq, :gt, :gte, :lt, :lte, :contains]
  @statuses [:unread, :read, :archived]

  # As regras usam uma condição no formato:
  # %{field: :temperature, operator: :gt, value: 30}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  def create_rule(attrs, server \\ __MODULE__), do: GenServer.call(server, {:create_rule, attrs})
  def get_rule(id, server \\ __MODULE__), do: GenServer.call(server, {:get_rule, id})
  def list_rules(server \\ __MODULE__), do: GenServer.call(server, :list_rules)
  def update_rule(id, attrs, server \\ __MODULE__), do: GenServer.call(server, {:update_rule, id, attrs})
  def delete_rule(id, server \\ __MODULE__), do: GenServer.call(server, {:delete_rule, id})

  def create_notification(attrs, server \\ __MODULE__),
    do: GenServer.call(server, {:create_notification, attrs})

  def get_notification(id, server \\ __MODULE__),
    do: GenServer.call(server, {:get_notification, id})

  def list_notifications(server \\ __MODULE__), do: GenServer.call(server, :list_notifications)

  def update_notification(id, attrs, server \\ __MODULE__),
    do: GenServer.call(server, {:update_notification, id, attrs})

  def delete_notification(id, server \\ __MODULE__),
    do: GenServer.call(server, {:delete_notification, id})

  def process_event(event, server \\ __MODULE__), do: GenServer.call(server, {:process_event, event})
  def history(server \\ __MODULE__), do: GenServer.call(server, :history)

  @impl true
  def init(:ok) do
    {:ok,
     %{
       rules: %{},
       notifications: %{},
       history: [],
       next_rule_id: 1,
       next_notification_id: 1
     }}
  end

  @impl true
  def handle_call({:create_rule, attrs}, _from, state) do
    attrs = if is_map(attrs), do: attrs, else: %{}
    rule = %Rule{
      id: state.next_rule_id,
      title: Map.get(attrs, :title),
      message: Map.get(attrs, :message),
      condition: Map.get(attrs, :condition),
      active: Map.get(attrs, :active, true),
      inserted_at: now(),
      updated_at: now()
    }

    if valid_rule?(rule) do
      state =
        state
        |> put_in([:rules, rule.id], rule)
        |> Map.put(:next_rule_id, rule.id + 1)
        |> record(:create, :rule, rule.id)

      {:reply, {:ok, rule}, state}
    else
      {:reply, {:error, :invalid_rule}, record(state, :create_failed, :rule, nil)}
    end
  end

  def handle_call({:get_rule, id}, _from, state) do
    {:reply, fetch_record(state.rules, id), state}
  end

  def handle_call(:list_rules, _from, state) do
    {:reply, sorted_records(state.rules), state}
  end

  def handle_call({:update_rule, id, attrs}, _from, state) do
    case Map.fetch(state.rules, id) do
      {:ok, rule} when is_map(attrs) ->
        updated =
          rule
          |> Map.from_struct()
          |> Map.merge(Map.take(attrs, [:title, :message, :condition, :active]))
          |> Map.put(:updated_at, now())
          |> then(&struct!(Rule, &1))

        if valid_rule?(updated) do
          state =
            state
            |> put_in([:rules, id], updated)
            |> record(:update, :rule, id)

          {:reply, {:ok, updated}, state}
        else
          {:reply, {:error, :invalid_rule}, record(state, :update_failed, :rule, id)}
        end

      {:ok, _rule} ->
        {:reply, {:error, :invalid_attributes}, record(state, :update_failed, :rule, id)}

      :error ->
        {:reply, {:error, :not_found}, record(state, :update_failed, :rule, id)}
    end
  end

  def handle_call({:delete_rule, id}, _from, state) do
    case Map.pop(state.rules, id) do
      {nil, _rules} ->
        {:reply, {:error, :not_found}, record(state, :delete_failed, :rule, id)}

      {rule, rules} ->
        state =
          state
          |> Map.put(:rules, rules)
          |> record(:delete, :rule, id)

        {:reply, {:ok, rule}, state}
    end
  end

  def handle_call({:create_notification, attrs}, _from, state) do
    attrs = if is_map(attrs), do: attrs, else: %{}

    notification = %Notification{
      id: state.next_notification_id,
      rule_id: nil,
      title: Map.get(attrs, :title),
      message: Map.get(attrs, :message),
      source: :manual,
      payload: Map.get(attrs, :payload),
      status: Map.get(attrs, :status, :unread),
      inserted_at: now(),
      updated_at: now()
    }

    if valid_notification?(notification) do
      state =
        state
        |> put_in([:notifications, notification.id], notification)
        |> Map.put(:next_notification_id, notification.id + 1)
        |> record(:create, :notification, notification.id)

      {:reply, {:ok, notification}, state}
    else
      {:reply, {:error, :invalid_notification},
       record(state, :create_failed, :notification, nil)}
    end
  end

  def handle_call({:get_notification, id}, _from, state) do
    {:reply, fetch_record(state.notifications, id), state}
  end

  def handle_call(:list_notifications, _from, state) do
    {:reply, sorted_records(state.notifications), state}
  end

  def handle_call({:update_notification, id, attrs}, _from, state) do
    case Map.fetch(state.notifications, id) do
      {:ok, notification} when is_map(attrs) ->
        updated =
          notification
          |> Map.from_struct()
          |> Map.merge(Map.take(attrs, [:title, :message, :status]))
          |> Map.put(:updated_at, now())
          |> then(&struct!(Notification, &1))

        if valid_notification?(updated) do
          state =
            state
            |> put_in([:notifications, id], updated)
            |> record(:update, :notification, id)

          {:reply, {:ok, updated}, state}
        else
          {:reply, {:error, :invalid_notification},
           record(state, :update_failed, :notification, id)}
        end

      {:ok, _notification} ->
        {:reply, {:error, :invalid_attributes},
         record(state, :update_failed, :notification, id)}

      :error ->
        {:reply, {:error, :not_found}, record(state, :update_failed, :notification, id)}
    end
  end

  def handle_call({:delete_notification, id}, _from, state) do
    case Map.pop(state.notifications, id) do
      {nil, _notifications} ->
        {:reply, {:error, :not_found}, record(state, :delete_failed, :notification, id)}

      {notification, notifications} ->
        state =
          state
          |> Map.put(:notifications, notifications)
          |> record(:delete, :notification, id)

        {:reply, {:ok, notification}, state}
    end
  end

  def handle_call({:process_event, event}, _from, state) when is_map(event) do
    matching_rules =
      state.rules
      |> Map.values()
      |> Enum.filter(&(&1.active and condition_met?(&1.condition, event)))
      |> Enum.sort_by(& &1.id)

    {created, state} =
      Enum.map_reduce(matching_rules, state, fn rule, acc ->
        notification = %Notification{
          id: acc.next_notification_id,
          rule_id: rule.id,
          title: rule.title,
          message: rule.message,
          source: :rule,
          payload: event,
          status: :unread,
          inserted_at: now(),
          updated_at: now()
        }

        acc =
          acc
          |> put_in([:notifications, notification.id], notification)
          |> Map.put(:next_notification_id, notification.id + 1)
          |> record(:generate, :notification, notification.id)

        {notification, acc}
      end)

    state = record(state, :process_event, :event, nil, %{generated_ids: Enum.map(created, & &1.id)})
    {:reply, {:ok, created}, state}
  end

  def handle_call({:process_event, _event}, _from, state) do
    {:reply, {:error, :invalid_event}, record(state, :process_event_failed, :event, nil)}
  end

  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  defp valid_rule?(%Rule{} = rule) do
    non_empty_string?(rule.title) and
      non_empty_string?(rule.message) and
      is_boolean(rule.active) and
      valid_condition?(rule.condition)
  end

  defp valid_notification?(%Notification{} = notification) do
    non_empty_string?(notification.title) and
      non_empty_string?(notification.message) and
      notification.status in @statuses
  end

  defp valid_condition?(condition) when is_map(condition) do
    field = Map.get(condition, :field)
    operator = Map.get(condition, :operator)

    not is_nil(field) and
      operator in @operators and
      Map.has_key?(condition, :value)
  end

  defp valid_condition?(_), do: false

  defp condition_met?(condition, event) do
    field = Map.get(condition, :field)
    operator = Map.get(condition, :operator)
    expected = Map.get(condition, :value)

    case event_value(event, field) do
      :missing -> false
      {:ok, actual} -> compare(actual, operator, expected)
    end
  end

  defp compare(actual, :eq, expected), do: actual == expected
  defp compare(actual, :neq, expected), do: actual != expected

  defp compare(actual, operator, expected)
       when operator in [:gt, :gte, :lt, :lte] and is_number(actual) and is_number(expected) do
    case operator do
      :gt -> actual > expected
      :gte -> actual >= expected
      :lt -> actual < expected
      :lte -> actual <= expected
    end
  end

  defp compare(actual, :contains, expected) when is_list(actual),
    do: Enum.member?(actual, expected)

  defp compare(actual, :contains, expected) when is_binary(actual) and is_binary(expected),
    do: String.contains?(actual, expected)

  defp compare(_actual, _operator, _expected), do: false

  defp event_value(event, field) do
    cond do
      Map.has_key?(event, field) ->
        {:ok, Map.get(event, field)}

      is_atom(field) and Map.has_key?(event, Atom.to_string(field)) ->
        {:ok, Map.get(event, Atom.to_string(field))}

      is_binary(field) ->
        atom_field =
          Enum.find(Map.keys(event), fn key ->
            is_atom(key) and Atom.to_string(key) == field
          end)

        if atom_field, do: {:ok, Map.get(event, atom_field)}, else: :missing

      true ->
        :missing
    end
  end

  defp fetch_record(records, id) do
    case Map.fetch(records, id) do
      {:ok, record} -> {:ok, record}
      :error -> {:error, :not_found}
    end
  end

  defp sorted_records(records), do: records |> Map.values() |> Enum.sort_by(& &1.id)
  defp non_empty_string?(value), do: is_binary(value) and String.trim(value) != ""
  defp now, do: DateTime.utc_now()

  defp record(state, action, entity, id, details \\ %{}) do
    entry = %{
      action: action,
      entity: entity,
      id: id,
      details: details,
      at: now()
    }

    %{state | history: [entry | state.history]}
  end
end
