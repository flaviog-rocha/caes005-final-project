# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir que diferentes entidades sejam processadas independentemente.
# 3. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 4. Manter um histórico das operações realizadas.
# 5. Impedir atualizações parcialmente concluídas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc false

  @registry SensorMonitor.Registry
  @dynamic_supervisor SensorMonitor.DynamicSupervisor

  def start_link do
    children = [
      {Registry, keys: :unique, name: @registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_supervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: SensorMonitor.Supervisor)
  end

  def ingest(entity_id, enumerable, timeout \\ :infinity) do
    with {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, {:ingest, enumerable}, timeout)
    end
  end

  def snapshot(entity_id) do
    with {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, :snapshot)
    end
  end

  def history(entity_id) do
    with {:ok, pid} <- ensure_entity(entity_id) do
      GenServer.call(pid, :history)
    end
  end

  defp ensure_entity(entity_id) do
    if Process.whereis(@registry) && Process.whereis(@dynamic_supervisor) do
      case Registry.lookup(@registry, entity_id) do
        [{pid, _}] ->
          {:ok, pid}

        [] ->
          start_entity(entity_id)
      end
    else
      {:error, :not_started}
    end
  end

  defp start_entity(entity_id) do
    child = %{
      id: make_ref(),
      start: {SensorMonitor.Entity, :start_link, [entity_id]},
      restart: :temporary
    }

    case DynamicSupervisor.start_child(@dynamic_supervisor, child) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, _reason} = error ->
        case Registry.lookup(@registry, entity_id) do
          [{pid, _}] -> {:ok, pid}
          [] -> error
        end
    end
  end
end

defmodule SensorMonitor.Entity do
  @moduledoc false
  use GenServer

  @registry SensorMonitor.Registry

  def start_link(entity_id) do
    GenServer.start_link(__MODULE__, entity_id,
      name: {:via, Registry, {@registry, entity_id}}
    )
  end

  @impl true
  def init(entity_id) do
    {:ok,
     %{
       entity_id: entity_id,
       aggregate: %{count: 0, sum: 0, min: nil, max: nil, latest: nil},
       history: []
     }}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, state.aggregate, state}
  end

  @impl true
  def handle_call(:history, _from, state) do
    {:reply, Enum.reverse(state.history), state}
  end

  @impl true
  def handle_call({:ingest, enumerable}, _from, state) do
    case calculate(enumerable, state.aggregate) do
      {:ok, candidate, readings_seen} ->
        operation = %{
          id: make_ref(),
          entity_id: state.entity_id,
          timestamp: System.system_time(:millisecond),
          status: :committed,
          readings_seen: readings_seen
        }

        new_state = %{
          state
          | aggregate: candidate,
            history: [operation | state.history]
        }

        {:reply, {:ok, candidate}, new_state}

      {:error, reason} ->
        operation = %{
          id: make_ref(),
          entity_id: state.entity_id,
          timestamp: System.system_time(:millisecond),
          status: :failed,
          error: reason
        }

        {:reply, {:error, reason}, %{state | history: [operation | state.history]}}
    end
  end

  defp calculate(enumerable, initial_aggregate) do
    try do
      Enum.reduce_while(enumerable, {:ok, initial_aggregate, 0}, fn reading,
                                                                    {:ok, aggregate, seen} ->
        case reading_value(reading) do
          {:ok, value} ->
            {:cont, {:ok, add_reading(aggregate, value), seen + 1}}

          :error ->
            {:halt, {:error, {:invalid_reading, seen + 1}}}
        end
      end)
    rescue
      error ->
        {:error, {:enumeration_failed, Exception.message(error)}}
    catch
      kind, reason ->
        {:error, {:enumeration_failed, {kind, inspect(reason, limit: 5)}}}
    else
      {:ok, aggregate, seen} -> {:ok, aggregate, seen}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reading_value(value) when is_number(value), do: {:ok, value}

  defp reading_value(%{value: value}) when is_number(value), do: {:ok, value}

  defp reading_value(_reading), do: :error

  defp add_reading(aggregate, value) do
    %{
      count: aggregate.count + 1,
      sum: aggregate.sum + value,
      min: if(is_nil(aggregate.min), do: value, else: min(aggregate.min, value)),
      max: if(is_nil(aggregate.max), do: value, else: max(aggregate.max, value)),
      latest: value
    }
  end
end
