# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Detectar encerramentos inesperados de processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  use GenServer

  @moduledoc """
  Sistema em memória para regras, notificações e monitoramento de processos.
  """

  # O armazenamento em memória representa um banco de dados fictício.
  # Uma falha de escrita pode ser simulada com `fail_next_write/1`.
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def add_rule(server, attrs) when is_map(attrs) do
    with {:ok, rule} <- validate_rule(attrs) do
      safe_call(server, {:add_rule, rule})
    end
  end

  def add_rule(_server, _attrs), do: {:error, {:validation, :rule_must_be_a_map}}

  def emit(server, event, value) do
    with :ok <- validate_event(event),
         :ok <- validate_value(value) do
      safe_call(server, {:emit, event, value})
    end
  end

  def list_rules(server), do: safe_call(server, :list_rules)
  def list_notifications(server), do: safe_call(server, :list_notifications)
  def list_incidents(server), do: safe_call(server, :list_incidents)

  def fail_next_write(server), do: safe_call(server, :fail_next_write)

  def monitor_process(server, pid, label \\ nil) when is_pid(pid) do
    safe_call(server, {:monitor_process, pid, label})
  end

  def monitor_process(_server, _pid, _label),
    do: {:error, {:validation, :pid_required}}

  def expect_shutdown(server, ref) when is_reference(ref) do
    safe_call(server, {:expect_shutdown, ref})
  end

  def expect_shutdown(_server, _ref),
    do: {:error, {:validation, :monitor_reference_required}}

  @impl true
  def init(opts) do
    {:ok,
     %{
       rules: %{},
       notifications: [],
       incidents: [],
       monitors: %{},
       next_rule_id: 1,
       next_notification_id: 1,
       fail_next_write: false,
       observer: Keyword.get(opts, :observer)
     }}
  end

  @impl true
  def handle_call({:add_rule, rule}, _from, state) do
    id = state.next_rule_id
    stored_rule = Map.put(rule, :id, id)

    {:reply, {:ok, stored_rule},
     %{state | rules: Map.put(state.rules, id, stored_rule), next_rule_id: id + 1}}
  end

  def handle_call({:emit, event, value}, _from, state) do
    matching_rules =
      state.rules
      |> Map.values()
      |> Enum.filter(&(&1.event == event and value >= &1.threshold))
      |> Enum.sort_by(& &1.id)

    if matching_rules == [] do
      {:reply, {:ok, []}, state}
    else
      if state.fail_next_write do
        {:reply, {:error, {:operational, :database_unavailable}},
         %{state | fail_next_write: false}}
      else
        {generated, next_id} =
          Enum.map_reduce(matching_rules, state.next_notification_id, fn rule, id ->
            notification = %{
              id: id,
              rule_id: rule.id,
              event: event,
              value: value,
              message: rule.message,
              created_at: DateTime.utc_now()
            }

            {notification, id + 1}
          end)

        updated_state = %{
          state
          | notifications: Enum.reverse(generated) ++ state.notifications,
            next_notification_id: next_id
        }

        {:reply, {:ok, generated}, updated_state}
      end
    end
  end

  def handle_call(:list_rules, _from, state) do
    rules = state.rules |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, {:ok, rules}, state}
  end

  def handle_call(:list_notifications, _from, state) do
    {:reply, {:ok, Enum.reverse(state.notifications)}, state}
  end

  def handle_call(:list_incidents, _from, state) do
    {:reply, {:ok, Enum.reverse(state.incidents)}, state}
  end

  def handle_call(:fail_next_write, _from, state) do
    {:reply, :ok, %{state | fail_next_write: true}}
  end

  def handle_call({:monitor_process, pid, label}, _from, state) do
    ref = Process.monitor(pid)

    monitor = %{
      pid: pid,
      label: label,
      expected_shutdown: false
    }

    {:reply, {:ok, ref}, %{state | monitors: Map.put(state.monitors, ref, monitor)}}
  end

  def handle_call({:expect_shutdown, ref}, _from, state) do
    case Map.fetch(state.monitors, ref) do
      {:ok, monitor} ->
        updated = Map.put(monitor, :expected_shutdown, true)
        {:reply, :ok, %{state | monitors: Map.put(state.monitors, ref, updated)}}

      :error ->
        {:reply, {:error, {:validation, :unknown_monitor_reference}}, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {monitor, monitors} ->
        unexpected? =
          not monitor.expected_shutdown and reason not in [:normal, :shutdown]

        if unexpected? do
          incident = %{
            pid: pid,
            label: monitor.label,
            reason: reason,
            detected_at: DateTime.utc_now()
          }

          if is_pid(state.observer) do
            send(state.observer, {:process_terminated_unexpectedly, incident})
          end

          {:noreply, %{state | monitors: monitors, incidents: [incident | state.incidents]}}
        else
          {:noreply, %{state | monitors: monitors}}
        end
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp validate_rule(attrs) do
    event = Map.get(attrs, :event)
    threshold = Map.get(attrs, :threshold)
    message = Map.get(attrs, :message)

    with :ok <- validate_event(event),
         :ok <- validate_value(threshold),
         :ok <- validate_message(message) do
      {:ok, %{event: event, threshold: threshold, message: message}}
    end
  end

  defp validate_event(event) when is_atom(event), do: :ok
  defp validate_event(event) when is_binary(event) and byte_size(event) > 0, do: :ok
  defp validate_event(_event), do: {:error, {:validation, :invalid_event}}

  defp validate_value(value) when is_integer(value) or is_float(value), do: :ok
  defp validate_value(_value), do: {:error, {:validation, :value_must_be_numeric}}

  defp validate_message(message) when is_binary(message) and byte_size(message) > 0, do: :ok
  defp validate_message(_message), do: {:error, {:validation, :message_required}}

  defp safe_call(server, request) do
    try do
      GenServer.call(server, request)
    catch
      :exit, reason -> {:error, {:operational, {:process_unavailable, reason}}}
    end
  end
end
