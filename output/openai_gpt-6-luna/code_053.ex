# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Validar os dados recebidos antes de realizar operações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule NotificationSystem do
  @moduledoc """
  Gerenciador de notificações com validação, histórico e recuperação do Agent.
  """

  @store __MODULE__.Store
  @supervisor __MODULE__.Supervisor
  @state_key {__MODULE__, :state}

  def start_link(_opts \\ []) do
    Supervisor.start_link(__MODULE__, :ok, name: @supervisor)
  end

  @impl true
  def init(:ok) do
    children = [
      %{
        id: @store,
        start: {Agent, :start_link, [fn -> load_state() end, [name: @store]]},
        restart: :permanent,
        type: :worker
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Processa um evento. Gera uma notificação quando `value >= threshold`.

  Espera um mapa com `:value` numérico, `:threshold` numérico e `:message`
  não vazio.
  """
  def process(data) do
    case validate(data) do
      :ok ->
        Agent.get_and_update(@store, fn state ->
          now = DateTime.utc_now()
          value = Map.fetch!(data, :value)
          threshold = Map.fetch!(data, :threshold)
          message = String.trim(Map.fetch!(data, :message))

          if value >= threshold do
            notification = %{
              message: message,
              value: value,
              threshold: threshold,
              created_at: now
            }

            entry = %{
              operation: :process,
              at: now,
              outcome: :notification_generated,
              notification: notification
            }

            new_state =
              state
              |> Map.update!(:notifications, &[notification | &1])
              |> Map.update!(:history, &[entry | &1])
              |> persist_state()

            {{:ok, notification}, new_state}
          else
            entry = %{
              operation: :process,
              at: now,
              outcome: :condition_not_met,
              value: value,
              threshold: threshold
            }

            new_state =
              state
              |> Map.update!(:history, &[entry | &1])
              |> persist_state()

            {{:ok, :no_notification}, new_state}
          end
        end)

      {:error, reasons} ->
        Agent.get_and_update(@store, fn state ->
          entry = %{
            operation: :process,
            at: DateTime.utc_now(),
            outcome: :invalid_data,
            reasons: reasons
          }

          new_state =
            state
            |> Map.update!(:history, &[entry | &1])
            |> persist_state()

          {{:error, {:invalid_data, reasons}}, new_state}
        end)
    end
  end

  def notifications do
    Agent.get(@store, fn state -> Enum.reverse(state.notifications) end)
  end

  def history do
    Agent.get(@store, fn state -> Enum.reverse(state.history) end)
  end

  defp validate(data) when is_map(data) do
    reasons =
      []
      |> add_error(not is_number(Map.get(data, :value)), :value_must_be_numeric)
      |> add_error(not is_number(Map.get(data, :threshold)), :threshold_must_be_numeric)
      |> add_error(
        not (is_binary(Map.get(data, :message)) and String.trim(Map.get(data, :message)) != ""),
        :message_must_be_non_empty
      )

    case reasons do
      [] -> :ok
      _ -> {:error, Enum.reverse(reasons)}
    end
  end

  defp validate(_data), do: {:error, [:data_must_be_a_map]}

  defp add_error(errors, true, reason), do: [reason | errors]
  defp add_error(errors, false, _reason), do: errors

  defp load_state do
    :persistent_term.get(@state_key, %{notifications: [], history: []})
  end

  defp persist_state(state) do
    :persistent_term.put(@state_key, state)
    state
  end
end
