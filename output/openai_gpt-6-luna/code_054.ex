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

defmodule NotificationManager do
  use Supervisor

  @agent_id :notification_agent

  def start_link(opts \\ []) when is_list(opts) do
    threshold = Keyword.get(opts, :threshold, 100)

    if is_number(threshold) do
      supervisor_opts =
        case Keyword.fetch(opts, :name) do
          {:ok, name} -> [name: name]
          :error -> []
        end

      Supervisor.start_link(__MODULE__, %{threshold: threshold}, supervisor_opts)
    else
      {:error, :invalid_threshold}
    end
  end

  @impl true
  def init(%{threshold: threshold}) do
    table = :ets.new(:notification_manager_state, [:set, :public, read_concurrency: true])

    initial_state = %{
      threshold: threshold,
      history: [],
      notifications: [],
      sequence: 0
    }

    :ets.insert(table, {:state, initial_state})

    agent = %{
      id: @agent_id,
      start: {Agent, :start_link, [fn -> load_state(table, initial_state) end]},
      restart: :permanent,
      type: :worker
    }

    Supervisor.init([agent], strategy: :one_for_one)
  end

  def submit(supervisor, event) do
    case validate_event(event) do
      :ok ->
        safely(fn ->
          with {:ok, agent} <- find_agent(supervisor) do
            Agent.get_and_update(agent, fn state ->
              now = DateTime.utc_now()
              generated? = event.value >= state.threshold

              notification =
                if generated? do
                  %{
                    event: event,
                    threshold: state.threshold,
                    created_at: now
                  }
                else
                  nil
                end

              sequence = state.sequence + 1

              operation = %{
                sequence: sequence,
                event: event,
                notification_generated: generated?,
                performed_at: now
              }

              new_state = %{
                state
                | sequence: sequence,
                  history: [operation | state.history],
                  notifications:
                    if(generated?,
                      do: [notification | state.notifications],
                      else: state.notifications
                    )
              }

              persist_state(agent_table(agent), new_state)
              {{:ok, notification}, new_state}
            end)
          end
        end)

      error ->
        error
    end
  end

  def history(supervisor) do
    safely(fn ->
      with {:ok, agent} <- find_agent(supervisor) do
        Agent.get(agent, fn state -> {:ok, Enum.reverse(state.history)} end)
      end
    end)
  end

  def notifications(supervisor) do
    safely(fn ->
      with {:ok, agent} <- find_agent(supervisor) do
        Agent.get(agent, fn state -> {:ok, Enum.reverse(state.notifications)} end)
      end
    end)
  end

  defp validate_event(event) when is_map(event) do
    type = Map.get(event, :type)
    value = Map.get(event, :value)
    metadata = Map.get(event, :metadata, %{})

    if valid_type?(type) and is_number(value) and is_map(metadata) do
      :ok
    else
      {:error, :invalid_event}
    end
  end

  defp validate_event(_), do: {:error, :invalid_event}

  defp valid_type?(type) when is_atom(type), do: true
  defp valid_type?(type) when is_binary(type), do: String.trim(type) != ""
  defp valid_type?(_), do: false

  defp find_agent(supervisor) when is_pid(supervisor) do
    case Supervisor.which_children(supervisor) do
      children ->
        case List.keyfind(children, @agent_id, 0) do
          {@agent_id, agent, :worker, _modules} when is_pid(agent) -> {:ok, agent}
          _ -> {:error, :unavailable}
        end
    end
  end

  defp find_agent(_), do: {:error, :unavailable}

  defp safely(fun) do
    try do
      fun.()
    catch
      :exit, _ -> {:error, :unavailable}
    end
  end

  defp load_state(table, initial_state) do
    case :ets.lookup(table, :state) do
      [{:state, state}] -> state
      [] -> initial_state
    end
  end

  defp persist_state(agent, state) do
    table = Agent.get(agent, &Map.get(&1, :table))
    :ets.insert(table, {:state, state})
  end

  defp agent_table(agent) do
    Agent.get(agent, &Map.get(&1, :table))
  end
end
