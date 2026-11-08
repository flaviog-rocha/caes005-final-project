# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Diferenciar erros de validação de falhas operacionais.
# 2. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Permitir consultar o estado atual das operações em andamento.
# 5. Gerar notificações quando determinadas condições forem atingidas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  use GenServer

  @moduledoc """
  Fila em memória que processa uma tarefa por entidade de cada vez.
  Entidades diferentes podem ser processadas em paralelo.
  """

  @type task_id :: pos_integer()

  @doc """
  Inicia a fila. `:processor` deve ser uma função de aridade 2 que recebe
  `(entity_id, payload)` e retorna um resultado.
  """
  def start_link(opts \\ []) do
    processor =
      Keyword.get(opts, :processor, fn _entity_id, payload -> {:ok, payload} end)

    if is_function(processor, 2) do
      GenServer.start_link(__MODULE__, processor, Keyword.take(opts, [:name]))
    else
      {:error, {:validation, :processor_must_have_arity_2}}
    end
  end

  @doc "Adiciona uma tarefa à fila da entidade."
  def enqueue(server, entity_id, payload) do
    if is_nil(entity_id) do
      {:error, {:validation, :entity_id_required}}
    else
      safe_call(server, {:enqueue, entity_id, payload})
    end
  end

  @doc "Consulta o estado de uma tarefa."
  def status(server, task_id) do
    if valid_task_id?(task_id) do
      safe_call(server, {:status, task_id})
    else
      {:error, {:validation, :invalid_task_id}}
    end
  end

  @doc "Consulta as tarefas de uma entidade, incluindo as já concluídas."
  def entity_status(server, entity_id) do
    if is_nil(entity_id) do
      {:error, {:validation, :entity_id_required}}
    else
      safe_call(server, {:entity_status, entity_id})
    end
  end

  @doc """
  Inscreve o processo chamador para receber notificações de conclusão e falha.

  As mensagens têm o formato `{:task_queue, {:completed | :failed, task}}`.
  """
  def subscribe(server), do: safe_call(server, {:subscribe, self()})

  @doc "Remove a inscrição do processo chamador."
  def unsubscribe(server), do: safe_call(server, {:unsubscribe, self()})

  @impl true
  def init(processor) do
    {:ok,
     %{
       processor: processor,
       tasks: %{},
       queues: %{},
       running: %{},
       workers: %{},
       monitors: %{},
       subscribers: %{},
       subscriber_refs: %{}
     }}
  end

  @impl true
  def handle_call({:enqueue, entity_id, payload}, _from, state) do
    task_id = System.unique_integer([:positive, :monotonic])
    now = now_ms()

    task = %{
      id: task_id,
      entity_id: entity_id,
      payload: payload,
      status: :queued,
      enqueued_at: now,
      started_at: nil,
      finished_at: nil,
      result: nil,
      error: nil
    }

    queue =
      state.queues
      |> Map.get(entity_id, :queue.new())
      |> :queue.in(task_id)

    state =
      %{state | tasks: Map.put(state.tasks, task_id, task)}
      |> put_in([:queues, entity_id], queue)
      |> dispatch_all()

    {:reply, {:ok, task_id}, state}
  end

  def handle_call({:status, task_id}, _from, state) do
    case Map.fetch(state.tasks, task_id) do
      {:ok, task} -> {:reply, {:ok, task}, state}
      :error -> {:reply, {:error, {:operational, :task_not_found}}, state}
    end
  end

  def handle_call({:entity_status, entity_id}, _from, state) do
    tasks =
      state.tasks
      |> Map.values()
      |> Enum.filter(&(&1.entity_id == entity_id))
      |> Enum.sort_by(& &1.id)

    active_task_id = Map.get(state.running, entity_id)

    active =
      if active_task_id do
        Map.get(state.tasks, active_task_id)
      else
        nil
      end

    queued = Enum.filter(tasks, &(&1.status == :queued))

    {:reply,
     {:ok,
      %{
        entity_id: entity_id,
        active: active,
        queued: queued,
        tasks: tasks
      }}, state}
  end

  def handle_call({:subscribe, pid}, _from, state) do
    if Map.has_key?(state.subscribers, pid) do
      {:reply, :ok, state}
    else
      ref = Process.monitor(pid)

      state = %{
        state
        | subscribers: Map.put(state.subscribers, pid, ref),
          subscriber_refs: Map.put(state.subscriber_refs, ref, pid)
      }

      {:reply, :ok, state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, state) do
    case Map.pop(state.subscribers, pid) do
      {nil, _subscribers} ->
        {:reply, :ok, state}

      {ref, subscribers} ->
        Process.demonitor(ref, [:flush])

        state = %{
          state
          | subscribers: subscribers,
            subscriber_refs: Map.delete(state.subscriber_refs, ref)
        }

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:task_result, task_id, outcome}, state) do
    case Map.pop(state.workers, task_id) do
      {nil, _workers} ->
        {:noreply, state}

      {%{entity_id: entity_id, monitor: ref}, workers} ->
        Process.demonitor(ref, [:flush])

        state = %{
          state
          | workers: workers,
            monitors: Map.delete(state.monitors, ref)
        }

        state =
          state
          |> finish_task(task_id, entity_id, outcome)
          |> dispatch_all()

        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.subscriber_refs, ref) do
      {pid, subscriber_refs} when not is_nil(pid) ->
        state = %{
          state
          | subscriber_refs: subscriber_refs,
            subscribers: Map.delete(state.subscribers, pid)
        }

        {:noreply, state}

      {nil, _subscriber_refs} ->
        case Map.pop(state.monitors, ref) do
          {nil, _monitors} ->
            {:noreply, state}

          {task_id, monitors} ->
            case Map.pop(state.workers, task_id) do
              {nil, workers} ->
                {:noreply, %{state | monitors: monitors, workers: workers}}

              {%{entity_id: entity_id}, workers} ->
                state = %{state | monitors: monitors, workers: workers}

                state =
                  state
                  |> finish_task(task_id, entity_id, {:error, {:worker_exit, reason}})
                  |> dispatch_all()

                {:noreply, state}
            end
        end
    end
  end

  defp dispatch_all(state) do
    Enum.reduce(Map.keys(state.queues), state, &dispatch_entity(&2, &1))
  end

  defp dispatch_entity(state, entity_id) do
    if Map.has_key?(state.running, entity_id) do
      state
    else
      case Map.get(state.queues, entity_id, :queue.new()) |> :queue.out() do
        {:empty, _queue} ->
          %{state | queues: Map.delete(state.queues, entity_id)}

        {{:value, task_id}, queue} ->
          queues =
            if :queue.is_empty(queue) do
              Map.delete(state.queues, entity_id)
            else
              Map.put(state.queues, entity_id, queue)
            end

          task = Map.fetch!(state.tasks, task_id)
          parent = self()
          processor = state.processor

          {pid, ref} =
            spawn_monitor(fn ->
              outcome = execute(processor, entity_id, task.payload)
              send(parent, {:task_result, task_id, outcome})
            end)

          updated_task =
            task
            |> Map.put(:status, :running)
            |> Map.put(:started_at, now_ms())

          %{
            state
            | queues: queues,
              tasks: Map.put(state.tasks, task_id, updated_task),
              running: Map.put(state.running, entity_id, task_id),
              workers:
                Map.put(state.workers, task_id, %{
                  pid: pid,
                  entity_id: entity_id,
                  monitor: ref
                }),
              monitors: Map.put(state.monitors, ref, task_id)
          }
      end
    end
  end

  defp finish_task(state, task_id, entity_id, outcome) do
    case Map.fetch(state.tasks, task_id) do
      :error ->
        %{state | running: Map.delete(state.running, entity_id)}

      {:ok, task} ->
        {status, result, error} =
          case outcome do
            {:ok, value} -> {:completed, value, nil}
            {:error, reason} -> {:failed, nil, reason}
          end

        updated_task =
          task
          |> Map.put(:status, status)
          |> Map.put(:finished_at, now_ms())
          |> Map.put(:result, result)
          |> Map.put(:error, error)

        Enum.each(state.subscribers, fn {pid, _ref} ->
          send(pid, {:task_queue, {status, updated_task}})
        end)

        %{
          state
          | tasks: Map.put(state.tasks, task_id, updated_task),
            running: Map.delete(state.running, entity_id)
        }
    end
  end

  defp execute(processor, entity_id, payload) do
    try do
      case processor.(entity_id, payload) do
        {:ok, value} -> {:ok, value}
        {:error, reason} -> {:error, reason}
        value -> {:ok, value}
      end
    rescue
      error -> {:error, {:exception, error, __STACKTRACE__}}
    catch
      kind, reason -> {:error, {:caught, kind, reason}}
    end
  end

  defp safe_call(server, message) do
    try do
      GenServer.call(server, message)
    catch
      :exit, reason -> {:error, {:operational, reason}}
    end
  end

  defp valid_task_id?(task_id), do: is_integer(task_id) and task_id > 0

  defp now_ms, do: System.system_time(:millisecond)
end
