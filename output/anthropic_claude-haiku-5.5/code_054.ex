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
  Sistema de gerenciamento de notificações construído sobre `Agent` e `Supervisor`.

  Requisitos atendidos:
    * Recuperação: o estado principal é um Agent supervisionado. Ao falhar, o
      supervisor o reinicia e ele é reconstruído a partir do último checkpoint.
    * Validação: toda entrada passa por `NotificationSystem.Validator` antes de
      qualquer alteração de estado.
    * Notificações: regras (métrica, operador, limiar) geram notificações quando
      a condição é atingida.
    * Histórico: operações aceitas, rejeitadas e recuperações são registradas.
  """

  use Supervisor

  alias NotificationSystem.{Core, Validator}

  @core :notification_core
  @checkpoint :notification_checkpoint

  # ---------------------------------------------------------------------------
  # Supervisão e recuperação
  # ---------------------------------------------------------------------------

  @doc "Inicia a árvore de supervisão do sistema."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # O checkpoint é iniciado primeiro: ele guarda o último estado conhecido
      # e sobrevive às falhas do estado principal.
      %{
        id: @checkpoint,
        start: {Agent, :start_link, [fn -> nil end, [name: @checkpoint]]}
      },
      # Estado principal: se falhar, é reiniciado e recupera o checkpoint.
      %{
        id: @core,
        start: {Agent, :start_link, [fn -> Core.recover(@checkpoint) end, [name: @core]]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Cadastra uma regra de notificação após validação."
  def add_rule(rule) do
    execute(:add_rule, rule, &Validator.validate_rule/1, &Core.add_rule/2)
  end

  @doc "Registra um evento de métrica após validação e avalia as regras associadas."
  def record_event(event) do
    execute(:record_event, event, &Validator.validate_event/1, &Core.process_event/2)
  end

  @doc "Lista as notificações geradas, em ordem cronológica."
  def notifications do
    with_core(fn -> Agent.get(@core, &Core.notifications/1) end)
  end

  @doc "Lista o histórico de operações, em ordem cronológica."
  def history do
    with_core(fn -> Agent.get(@core, &Core.history/1) end)
  end

  @doc "Provoca uma falha no estado principal para testar a recuperação."
  def simulate_fault do
    with_core(fn -> Agent.update(@core, fn _ -> raise "falha simulada" end) end, 1)
  end

  # ---------------------------------------------------------------------------
  # Execução de operações
  # ---------------------------------------------------------------------------

  # Fluxo: validar -> aplicar no Agent principal -> salvar checkpoint.
  # Entradas inválidas são registradas no histórico e nunca alteram o estado.
  defp execute(op, input, validate, transform) do
    case validate.(input) do
      {:error, reasons} ->
        with_core(fn ->
          Agent.update(@core, &Core.add_history(&1, op, :rejected, %{reasons: reasons}))
        end)

        {:error, reasons}

      {:ok, data} ->
        case with_core(fn -> Agent.get_and_update(@core, &apply_transform(&1, transform, data)) end) do
          {:ok, {result, new_state}} ->
            # Falha ao salvar o checkpoint não invalida a operação; o snapshot
            # será regravado na próxima operação bem-sucedida.
            with_core(fn -> Agent.update(@checkpoint, fn _ -> new_state end) end, 1)
            result

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp apply_transform(state, transform, data) do
    {result, new_state} = transform.(state, data)
    {{result, new_state}, new_state}
  end

  # Executa uma chamada ao Agent principal, tentando novamente caso o processo
  # esteja reiniciando após uma falha.
  defp with_core(fun, attempts \\ 3) do
    {:ok, fun.()}
  catch
    :exit, _reason when attempts > 1 ->
      Process.sleep(50)
      with_core(fun, attempts - 1)

    :exit, reason ->
      {:error, {:core_indisponivel, reason}}
  end
end

defmodule NotificationSystem.Validator do
  @moduledoc """
  Validação pura dos dados de entrada. Não possui efeitos colaterais.
  """

  @operators [:gt, :gte, :lt, :lte, :eq]

  @doc "Valida uma regra de notificação."
  def validate_rule(rule) when is_map(rule) do
    id = Map.get(rule, :id)
    metric = Map.get(rule, :metric)
    operator = Map.get(rule, :operator)
    threshold = Map.get(rule, :threshold)
    message = Map.get(rule, :message)

    errors =
      []
      |> check(non_empty_string?(id), "id deve ser uma string não vazia")
      |> check(non_empty_string?(metric), "metric deve ser uma string não vazia")
      |> check(operator in @operators, "operator deve ser um de #{inspect(@operators)}")
      |> check(is_number(threshold), "threshold deve ser numérico")
      |> check(non_empty_string?(message), "message deve ser uma string não vazia")

    finish(errors, %{
      id: id,
      metric: metric,
      operator: operator,
      threshold: threshold,
      message: message
    })
  end

  def validate_rule(_), do: {:error, ["regra deve ser um mapa"]}

  @doc "Valida um evento de métrica."
  def validate_event(event) when is_map(event) do
    metric = Map.get(event, :metric)
    value = Map.get(event, :value)

    errors =
      []
      |> check(non_empty_string?(metric), "metric deve ser uma string não vazia")
      |> check(is_number(value), "value deve ser numérico")

    finish(errors, %{metric: metric, value: value})
  end

  def validate_event(_), do: {:error, ["evento deve ser um mapa"]}

  # Acumula erros: check/3 é chamado com a lista de erros como primeiro argumento.
  defp check(errors, true, _msg), do: errors
  defp check(errors, _false, msg), do: [msg | errors]

  defp finish([], data), do: {:ok, data}
  defp finish(errors, _data), do: {:error, Enum.reverse(errors)}

  defp non_empty_string?(value), do: is_binary(value) and byte_size(value) > 0
end

defmodule NotificationSystem.Core do
  @moduledoc """
  Lógica do estado principal. Todas as funções recebem e devolvem o estado
  (ou são executadas dentro do Agent), mantendo a lógica pura e testável.
  """

  @initial_state %{
    rules: %{},
    notifications: [],
    history: [],
    next_seq: 1,
    history_seq: 1
  }

  @doc """
  Recupera o estado a partir do checkpoint. Chamado na inicialização do Agent,
  inclusive após falhas. Se não houver checkpoint, inicia do zero.
  """
  def recover(checkpoint) do
    case Agent.get(checkpoint, & &1) do
      nil -> @initial_state
      snapshot -> add_history(snapshot, :recovered, :ok, %{})
    end
  catch
    # Se o próprio checkpoint estiver indisponível, a recuperação parte do zero.
    :exit, _ -> @initial_state
  end

  @doc "Cadastra (ou substitui) uma regra."
  def add_rule(state, rule) do
    state = %{state | rules: Map.put(state.rules, rule.id, rule)}
    state = add_history(state, :add_rule, :ok, %{rule_id: rule.id})
    {{:ok, rule.id}, state}
  end

  @doc """
  Processa um evento: avalia todas as regras da métrica e gera uma notificação
  para cada condição atingida.
  """
  def process_event(state, event) do
    now = DateTime.utc_now()

    triggered =
      state.rules
      |> Map.values()
      |> Enum.filter(fn rule ->
        rule.metric == event.metric and
          condition_met?(rule.operator, event.value, rule.threshold)
      end)

    {notifications, next_seq} =
      Enum.map_reduce(triggered, state.next_seq, fn rule, seq ->
        notification = %{
          id: seq,
          rule_id: rule.id,
          metric: event.metric,
          value: event.value,
          message: rule.message,
          created_at: now
        }

        {notification, seq + 1}
      end)

    # Notificações são armazenadas do mais recente para o mais antigo.
    state = %{
      state
      | notifications: Enum.reverse(notifications) ++ state.notifications,
        next_seq: next_seq
    }

    state =
      add_history(state, :record_event, :ok, %{
        metric: event.metric,
        value: event.value,
        notified: length(notifications)
      })

    {{:ok, length(notifications)}, state}
  end

  @doc "Lista as notificações em ordem cronológica."
  def notifications(state), do: Enum.reverse(state.notifications)

  @doc "Lista o histórico em ordem cronológica."
  def history(state), do: Enum.reverse(state.history)

  @doc "Adiciona uma entrada ao histórico de operações."
  def add_history(state, op, status, details) do
    entry = %{
      id: state.history_seq,
      at: DateTime.utc_now(),
      operation: op,
      status: status,
      details: details
    }

    %{state | history: [entry | state.history], history_seq: state.history_seq + 1}
  end

  defp condition_met?(:gt, value, threshold), do: value > threshold
  defp condition_met?(:gte, value, threshold), do: value >= threshold
  defp condition_met?(:lt, value, threshold), do: value < threshold
  defp condition_met?(:lte, value, threshold), do: value <= threshold
  defp condition_met?(:eq, value, threshold), do: value == threshold
end
