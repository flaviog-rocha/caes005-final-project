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

# Sistema de gerenciamento de notificações com Agent, supervisão e recuperação de estado.
# Arquivo único e autocontido, usando apenas a biblioteca padrão do Elixir/OTP.

defmodule NotificationSystem do
  @moduledoc """
  Ponto de entrada do sistema.

  Supervisiona:
    * `NotificationSystem.Checkpoint` - Agent que guarda o último snapshot persistido do estado.
    * `NotificationSystem.Core` - Agent que mantém regras, notificações e histórico.

  Se o `Core` falhar, o supervisor o reinicia e ele restaura o estado a partir do `Checkpoint`.
  """

  use Supervisor

  # Inicia a árvore de supervisão.
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # Checkpoint é iniciado primeiro para que o Core possa restaurar o estado ao subir.
      NotificationSystem.Checkpoint,
      NotificationSystem.Core
    ]

    # one_for_one: somente o processo que falhou é reiniciado.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 30)
  end

  # API pública delegada ao núcleo.
  defdelegate define_rule(attrs), to: NotificationSystem.Core
  defdelegate record_metric(metric, value), to: NotificationSystem.Core
  defdelegate list_rules(), to: NotificationSystem.Core
  defdelegate list_notifications(), to: NotificationSystem.Core
  defdelegate history(), to: NotificationSystem.Core
  defdelegate simulate_failure(), to: NotificationSystem.Core
end

defmodule NotificationSystem.Validator do
  @moduledoc """
  Validação pura dos dados recebidos. Nenhuma operação de estado ocorre antes
  de os dados passarem por aqui.
  """

  @types [:info, :warning, :critical]
  @operators [:gt, :gte, :lt, :lte, :eq]

  @doc "Valida os atributos de uma regra de notificação."
  def validate_rule(attrs) when is_map(attrs) do
    with {:ok, metric} <- string_field(attrs, :metric),
         {:ok, operator} <- enum_field(attrs, :operator, @operators),
         {:ok, threshold} <- number_field(attrs, :threshold),
         {:ok, type} <- enum_field(attrs, :type, @types),
         {:ok, message} <- string_field(attrs, :message) do
      {:ok,
       %{
         metric: metric,
         operator: operator,
         threshold: threshold,
         type: type,
         message: message
       }}
    end
  end

  def validate_rule(_), do: {:error, {:invalid, :attrs}}

  @doc "Valida o nome de uma métrica e seu valor numérico."
  def validate_metric(metric, value) do
    with {:ok, metric} <- non_empty_string(metric, :metric),
         {:ok, value} <- number(value, :value) do
      {:ok, {metric, value}}
    end
  end

  defp string_field(attrs, key), do: attrs |> Map.get(key) |> non_empty_string(key)

  defp number_field(attrs, key), do: attrs |> Map.get(key) |> number(key)

  defp enum_field(attrs, key, allowed) do
    value = Map.get(attrs, key)
    if value in allowed, do: {:ok, value}, else: {:error, {:invalid, key}}
  end

  defp non_empty_string(value, key) when is_binary(value) do
    case String.trim(value) do
      "" -> {:error, {:invalid, key}}
      trimmed -> {:ok, trimmed}
    end
  end

  defp non_empty_string(_value, key), do: {:error, {:invalid, key}}

  defp number(value, _key) when is_number(value), do: {:ok, value}
  defp number(_value, key), do: {:error, {:invalid, key}}
end

defmodule NotificationSystem.Checkpoint do
  @moduledoc """
  Agent independente que armazena o último snapshot serializado do estado do `Core`.
  Por ser um processo separado, sobrevive às falhas do `Core`.
  """

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> nil end, name: __MODULE__)
  end

  @doc "Persiste um snapshot do estado (serializado em binário para isolamento)."
  def save(state) do
    Agent.update(__MODULE__, fn _ -> :erlang.term_to_binary(state) end)
  end

  @doc "Carrega o último snapshot, se existir."
  def load do
    Agent.get(__MODULE__, fn
      nil -> :empty
      binary -> {:ok, :erlang.binary_to_term(binary, [:safe])}
    end)
  end
end

defmodule NotificationSystem.Core do
  @moduledoc """
  Agent principal. Mantém:
    * `rules` - regras de condição por métrica;
    * `notifications` - notificações geradas (mais recentes primeiro);
    * `history` - registro de todas as operações (aceitas e rejeitadas).

  Cada operação é executada dentro do Agent de forma atômica: valida, altera o
  estado, registra no histórico e persiste o snapshot no `Checkpoint`.
  """

  use Agent

  alias NotificationSystem.{Checkpoint, Validator}

  @initial_state %{rules: %{}, notifications: [], history: [], seq: 0}

  def start_link(_opts \\ []) do
    # O estado inicial é restaurado do Checkpoint, permitindo recuperação após falhas.
    Agent.start_link(&restore_state/0, name: __MODULE__)
  end

  @doc """
  Define uma regra de notificação. Ex.:

      define_rule(%{metric: "cpu", operator: :gt, threshold: 90,
                    type: :critical, message: "CPU acima de 90%"})
  """
  def define_rule(attrs) do
    commit(:define_rule, fn state ->
      case Validator.validate_rule(attrs) do
        {:ok, rule} ->
          id = state.seq + 1
          rule = Map.put(rule, :id, id)
          new_state = %{state | rules: Map.put(state.rules, id, rule), seq: id}
          {{:ok, id}, new_state}

        {:error, _reason} = error ->
          {error, state}
      end
    end)
  end

  @doc """
  Registra o valor de uma métrica. Gera notificações para cada regra cuja
  condição seja atingida. Retorna `{:ok, notificacoes_geradas}`.
  """
  def record_metric(metric, value) do
    commit(:record_metric, fn state ->
      case Validator.validate_metric(metric, value) do
        {:ok, {metric, value}} ->
          now = DateTime.utc_now()

          triggered =
            state.rules
            |> Map.values()
            |> Enum.filter(fn rule ->
              rule.metric == metric and condition_met?(rule.operator, value, rule.threshold)
            end)
            |> Enum.map(fn rule ->
              %{
                id: System.unique_integer([:positive, :monotonic]),
                rule_id: rule.id,
                type: rule.type,
                message: rule.message,
                metric: metric,
                value: value,
                created_at: now
              }
            end)

          new_state = %{state | notifications: Enum.reverse(triggered) ++ state.notifications}
          {{:ok, triggered}, new_state}

        {:error, _reason} = error ->
          {error, state}
      end
    end)
  end

  @doc "Lista as regras cadastradas."
  def list_rules do
    Agent.get(__MODULE__, fn state -> state.rules |> Map.values() |> Enum.sort_by(& &1.id) end)
  end

  @doc "Lista as notificações geradas, em ordem cronológica."
  def list_notifications do
    Agent.get(__MODULE__, fn state -> Enum.reverse(state.notifications) end)
  end

  @doc "Retorna o histórico de operações, em ordem cronológica."
  def history do
    Agent.get(__MODULE__, fn state -> Enum.reverse(state.history) end)
  end

  @doc """
  Provoca uma falha intencional no Agent para testar a recuperação.
  O supervisor reinicia o processo e o estado é restaurado do Checkpoint.
  """
  def simulate_failure do
    try do
      Agent.update(__MODULE__, fn _state -> raise "falha simulada" end)
      :ok
    catch
      :exit, _reason -> :crashed_and_recovering
    end
  end

  # Executa uma operação atomicamente: calcula, registra histórico e persiste.
  # Exceções inesperadas são convertidas em erro, sem derrubar o processo.
  defp commit(op, fun) do
    Agent.get_and_update(__MODULE__, fn state ->
      {result, new_state} =
        try do
          fun.(state)
        rescue
          exception -> {{:error, {:exception, Exception.message(exception)}}, state}
        end

      new_state = add_history(new_state, op, result)
      Checkpoint.save(new_state)
      {result, new_state}
    end)
  end

  defp add_history(state, op, result) do
    entry = %{
      at: DateTime.utc_now(),
      operation: op,
      status: status_of(result),
      detail: result
    }

    %{state | history: [entry | state.history]}
  end

  defp status_of({:ok, _}), do: :ok
  defp status_of(_), do: :error

  defp condition_met?(:gt, value, threshold), do: value > threshold
  defp condition_met?(:gte, value, threshold), do: value >= threshold
  defp condition_met?(:lt, value, threshold), do: value < threshold
  defp condition_met?(:lte, value, threshold), do: value <= threshold
  defp condition_met?(:eq, value, threshold), do: value == threshold

  defp restore_state do
    case Checkpoint.load() do
      {:ok, state} -> state
      :empty -> @initial_state
    end
  end
end
