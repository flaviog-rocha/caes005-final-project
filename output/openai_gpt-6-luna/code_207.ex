# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule VotingSystem do
  use GenServer

  @moduledoc """
  Sistema de votação em memória com operações serializadas e submissão assíncrona
  limitada por `:max_concurrent`.
  """

  # API de processo

  def start_link(opts \\ []) do
    case Keyword.get(opts, :name) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  # CRUD síncrono de votações

  def create_record(server, attrs), do: GenServer.call(server, {:create_record, attrs})
  def get_record(server, id), do: GenServer.call(server, {:get_record, id})
  def list_records(server), do: GenServer.call(server, :list_records)
  def update_record(server, id, attrs), do: GenServer.call(server, {:update_record, id, attrs})
  def delete_record(server, id), do: GenServer.call(server, {:delete_record, id})

  # Votação

  def vote(server, poll_id, voter, option) do
    GenServer.call(server, {:vote, poll_id, voter, option})
  end

  def results(server, poll_id), do: GenServer.call(server, {:results, poll_id})

  # Operações assíncronas aceitas:
  # {:create, attrs}, {:update, id, attrs}, {:delete, id},
  # {:vote, poll_id, voter, option}

  def submit(server, operation), do: GenServer.call(server, {:submit, operation})
  def operation_status(server, id), do: GenServer.call(server, {:operation_status, id})
  def current_status(server), do: GenServer.call(server, :current_status)

  @impl true
  def init(opts) do
    max_concurrent =
      case Keyword.get(opts, :max_concurrent, 4) do
        value when is_integer(value) and value > 0 -> value
        _ -> 4
      end

    {:ok,
     %{
       records: %{},
       next_record_id: 1,
       max_concurrent: max_concurrent,
       running: 0,
       queue: :queue.new(),
       next_operation_id: 1,
       operations: %{}
     }}
  end

  @impl true
  def handle_call({:create_record, attrs}, _from, state) do
    case validate_new_record(attrs) do
      {:ok, question, options} ->
        id = state.next_record_id
        record = %{id: id, question: question, options: options, votes: %{}}

        {:reply, {:ok, record},
         %{state | records: Map.put(state.records, id, record), next_record_id: id + 1}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_record, id}, _from, state) do
    case Map.fetch(state.records, id) do
      {:ok, record} -> {:reply, {:ok, record}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_records, _from, state) do
    records = state.records |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, records, state}
  end

  def handle_call({:update_record, id, attrs}, _from, state) do
    case Map.fetch(state.records, id) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, record} ->
        case validate_record_update(attrs, record) do
          {:ok, updated_record} ->
            {:reply, {:ok, updated_record},
             %{state | records: Map.put(state.records, id, updated_record)}}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:delete_record, id}, _from, state) do
    if Map.has_key?(state.records, id) do
      {:reply, :ok, %{state | records: Map.delete(state.records, id)}}
    else
      {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:vote, poll_id, voter, option}, _from, state) do
    case cast_vote(state.records, poll_id, voter, option) do
      {:ok, updated_records, record} ->
        {:reply, {:ok, record}, %{state | records: updated_records}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:results, poll_id}, _from, state) do
    case Map.fetch(state.records, poll_id) do
      {:ok, record} ->
        counts = Map.new(record.options, &{&1, 0})

        counts =
          Enum.reduce(record.votes, counts, fn {_voter, option}, acc ->
            Map.update!(acc, option, &(&1 + 1))
          end)

        {:reply, {:ok, %{poll_id: poll_id, counts: counts, total_votes: map_size(record.votes)}},
         state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:submit, operation}, _from, state) do
    id = state.next_operation_id
    now = now()

    operation_state = %{
      id: id,
      operation: operation,
      status: :queued,
      submitted_at: now,
      started_at: nil,
      finished_at: nil,
      result: nil
    }

    state = %{
      state
      | next_operation_id: id + 1,
        queue: :queue.in(id, state.queue),
        operations: Map.put(state.operations, id, operation_state)
    }

    {:reply, {:ok, id}, dispatch(state)}
  end

  def handle_call({:operation_status, id}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} -> {:reply, {:ok, operation}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:current_status, _from, state) do
    counts =
      Enum.reduce(state.operations, %{queued: 0, running: 0, completed: 0}, fn
        {_id, %{status: status}}, acc -> Map.update!(acc, status, &(&1 + 1))
      end)

    {:reply,
     %{
       max_concurrent: state.max_concurrent,
       running: state.running,
       queued: :queue.len(state.queue),
       operations: counts
     }, state}
  end

  @impl true
  def handle_info({:operation_result, id, result}, state) do
    case Map.fetch(state.operations, id) do
      {:ok, operation} when operation.status == :running ->
        updated_operation = %{
          operation
          | status: :completed,
            result: result,
            finished_at: now()
        }

        state = %{
          state
          | running: max(state.running - 1, 0),
            operations: Map.put(state.operations, id, updated_operation)
        }

        {:noreply, dispatch(state)}

      _ ->
        {:noreply, state}
    end
  end

  defp dispatch(state) when state.running >= state.max_concurrent, do: state

  defp dispatch(state) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, id}, queue} ->
        operation = Map.fetch!(state.operations, id)
        updated_operation = %{operation | status: :running, started_at: now()}

        case Task.start(fn ->
               result = safely_execute(self_server = self(), operation.operation)
               send(self_server, {:operation_result, id, result})
             end) do
          {:ok, _pid} ->
            state = %{
              state
              | queue: queue,
                running: state.running + 1,
                operations: Map.put(state.operations, id, updated_operation)
            }

            dispatch(state)

          {:error, reason} ->
            failed_operation = %{
              updated_operation
              | status: :completed,
                result: {:error, {:task_start_failed, reason}},
                finished_at: now()
            }

            dispatch(%{
              state
              | queue: queue,
                operations: Map.put(state.operations, id, failed_operation)
            })
        end
    end
  end

  defp safely_execute(server, operation) do
    try do
      case operation do
        {:create, attrs} -> create_record(server, attrs)
        {:update, id, attrs} -> update_record(server, id, attrs)
        {:delete, id} -> delete_record(server, id)
        {:vote, poll_id, voter, option} -> vote(server, poll_id, voter, option)
        _ -> {:error, :invalid_operation}
      end
    rescue
      error -> {:error, {:exception, error}}
    catch
      kind, reason -> {:error, {kind, reason}}
    end
  end

  defp validate_new_record(attrs) when is_map(attrs) do
    question = Map.get(attrs, :question)
    options = Map.get(attrs, :options)

    cond do
      not valid_question?(question) -> {:error, :invalid_question}
      not valid_options?(options) -> {:error, :invalid_options}
      true -> {:ok, question, Enum.uniq(options)}
    end
  end

  defp validate_new_record(_), do: {:error, :invalid_record}

  defp validate_record_update(attrs, record) when is_map(attrs) do
    question = Map.get(attrs, :question, record.question)
    options = Map.get(attrs, :options, record.options)

    cond do
      not valid_question?(question) ->
        {:error, :invalid_question}

      not valid_options?(options) ->
        {:error, :invalid_options}

      not Enum.all?(Map.values(record.votes), &(&1 in options)) ->
        {:error, :cannot_remove_option_with_votes}

      true ->
        {:ok, %{record | question: question, options: Enum.uniq(options)}}
    end
  end

  defp validate_record_update(_, _), do: {:error, :invalid_record}

  defp valid_question?(question), do: is_binary(question) and String.trim(question) != ""

  defp valid_options?(options) do
    is_list(options) and options != [] and Enum.all?(options, &(not is_nil(&1)))
  end

  defp cast_vote(records, poll_id, voter, option) do
    case Map.fetch(records, poll_id) do
      :error ->
        {:error, :not_found}

      {:ok, record} ->
        cond do
          is_nil(voter) ->
            {:error, :invalid_voter}

          Map.has_key?(record.votes, voter) ->
            {:error, :already_voted}

          option not in record.options ->
            {:error, :invalid_option}

          true ->
            updated_record = %{record | votes: Map.put(record.votes, voter, option)}
            {:ok, Map.put(records, poll_id, updated_record), updated_record}
        end
    end
  end

  defp now, do: System.system_time(:millisecond)
end
