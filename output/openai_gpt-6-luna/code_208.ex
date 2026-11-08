# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de votação online, atendendo somente aos seguintes requisitos:
#
# 1. Garantir a consistência dos dados durante operações concorrentes.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Permitir cadastrar, consultar, atualizar e remover registros.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule OnlineVoting do
  use GenServer

  @type server :: GenServer.server()
  @type operation_id :: pos_integer()
  @type poll_id :: pos_integer()

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def create_poll(server, question, options) do
    submit(server, {:create_poll, question, options})
  end

  def update_poll(server, poll_id, attributes) when is_map(attributes) do
    submit(server, {:update_poll, poll_id, attributes})
  end

  def delete_poll(server, poll_id) do
    submit(server, {:delete_poll, poll_id})
  end

  def cast_vote(server, poll_id, voter_id, option) do
    submit(server, {:cast_vote, poll_id, voter_id, option})
  end

  def get_poll(server, poll_id), do: GenServer.call(server, {:get_poll, poll_id})
  def list_polls(server), do: GenServer.call(server, :list_polls)

  def operation_status(server, operation_id) do
    GenServer.call(server, {:operation_status, operation_id})
  end

  def operations(server), do: GenServer.call(server, :operations)

  @impl true
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, 4)

    if is_integer(max_concurrency) and max_concurrency > 0 do
      case Task.Supervisor.start_link() do
        {:ok, task_supervisor} ->
          {:ok,
           %{
             task_supervisor: task_supervisor,
             max_concurrency: max_concurrency,
             polls: %{},
             next_poll_id: 1,
             next_operation_id: 1,
             operations: %{},
             queue: :queue.new(),
             running: %{}
           }}

        {:error, reason} ->
          {:stop, reason}
      end
    else
      {:stop, {:invalid_max_concurrency, max_concurrency}}
    end
  end

  @impl true
  def handle_call({:submit, operation}, _from, state) do
    id = state.next_operation_id

    operation_state = %{
      id: id,
      status: :queued,
      result: nil,
      operation: operation
    }

    state =
      state
      |> put_in([:operations, id], operation_state)
      |> Map.put(:next_operation_id, id + 1)
      |> Map.update!(:queue, &:queue.in(id, &1))
      |> dispatch()

    {:reply, {:ok, id}, state}
  end

  def handle_call({:get_poll, poll_id}, _from, state) do
    reply =
      case Map.fetch(state.polls, poll_id) do
        {:ok, poll} -> {:ok, poll_view(poll)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_polls, _from, state) do
    polls =
      state.polls
      |> Map.values()
      |> Enum.map(&poll_view/1)
      |> Enum.sort_by(& &1.id)

    {:reply, polls, state}
  end

  def handle_call({:operation_status, id}, _from, state) do
    reply =
      case Map.fetch(state.operations, id) do
        {:ok, operation} -> {:ok, public_operation(operation)}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:operations, _from, state) do
    result =
      state.operations
      |> Map.values()
      |> Enum.map(&public_operation/1)
      |> Enum.sort_by(& &1.id)

    {:reply, result, state}
  end

  def handle_call({:execute, id}, _from, state) do
    case Map.fetch(state.operations, id) do
      {:ok, %{status: :running, operation: operation}} ->
        {result, state} = execute_operation(operation, state)
        {:reply, result, state}

      _ ->
        {:reply, {:error, :operation_not_running}, state}
    end
  end

  @impl true
  def handle_cast({:finished, id, result}, state) do
    state =
      case Map.fetch(state.operations, id) do
        {:ok, operation} ->
          status = if match?({:ok, _}, result), do: :completed, else: :failed
          operation = %{operation | status: status, result: result}

          state
          |> put_in([:operations, id], operation)
          |> update_in([:running], &Map.delete(&1, id))
          |> dispatch()

        :error ->
          state
      end

    {:noreply, state}
  end

  defp submit(server, operation), do: GenServer.call(server, {:submit, operation})

  defp dispatch(state) do
    if map_size(state.running) >= state.max_concurrency do
      state
    else
      case :queue.out(state.queue) do
        {:empty, _queue} ->
          state

        {{:value, id}, queue} ->
          state = %{state | queue: queue}
          operation = state.operations[id].operation
          server = self()

          case Task.Supervisor.start_child(state.task_supervisor, fn ->
                 result =
                   try do
                     GenServer.call(server, {:execute, id}, :infinity)
                   rescue
                     exception -> {:error, {:worker_error, exception.__struct__}}
                   catch
                     kind, reason -> {:error, {:worker_error, kind, reason}}
                   end

                 GenServer.cast(server, {:finished, id, result})
               end) do
            {:ok, pid} ->
              state =
                state
                |> update_in([:running], &Map.put(&1, id, pid))
                |> put_in([:operations, id, :status], :running)

              dispatch(state)

            {:error, reason} ->
              state =
                state
                |> put_in([:operations, id, :status], :failed)
                |> put_in([:operations, id, :result], {:error, {:worker_start_failed, reason}})

              dispatch(state)
          end
      end
    end
  end

  defp execute_operation({:create_poll, question, options}, state) do
    with :ok <- validate_question(question),
         :ok <- validate_options(options) do
      id = state.next_poll_id

      poll = %{
        id: id,
        question: question,
        options: options,
        votes: Map.new(options, &{&1, 0}),
        voter_ids: MapSet.new()
      }

      {{:ok, poll_view(poll)},
       state
       |> put_in([:polls, id], poll)
       |> Map.put(:next_poll_id, id + 1)}
    else
      error -> {error, state}
    end
  end

  defp execute_operation({:update_poll, id, attributes}, state) do
    case Map.fetch(state.polls, id) do
      :error ->
        {{:error, :not_found}, state}

      {:ok, poll} ->
        question = Map.get(attributes, :question, poll.question)
        options = Map.get(attributes, :options, poll.options)

        cond do
          not Enum.all?(Map.keys(attributes), &(&1 in [:question, :options])) ->
            {{:error, :invalid_attributes}, state}

          validate_question(question) != :ok ->
            {{:error, :invalid_question}, state}

          validate_options(options) != :ok ->
            {{:error, :invalid_options}, state}

          options != poll.options and MapSet.size(poll.voter_ids) > 0 ->
            {{:error, :cannot_change_options_after_votes}, state}

          true ->
            votes =
              if options == poll.options do
                poll.votes
              else
                Map.new(options, &{&1, 0})
              end

            updated = %{poll | question: question, options: options, votes: votes}
            {{:ok, poll_view(updated)}, put_in(state, [:polls, id], updated)}
        end
    end
  end

  defp execute_operation({:delete_poll, id}, state) do
    if Map.has_key?(state.polls, id) do
      {{:ok, :deleted}, update_in(state.polls, &Map.delete(&1, id))}
    else
      {{:error, :not_found}, state}
    end
  end

  defp execute_operation({:cast_vote, id, voter_id, option}, state) do
    case Map.fetch(state.polls, id) do
      :error ->
        {{:error, :not_found}, state}

      {:ok, poll} ->
        cond do
          not valid_identifier?(voter_id) ->
            {{:error, :invalid_voter_id}, state}

          not (is_binary(option) and option in poll.options) ->
            {{:error, :invalid_option}, state}

          MapSet.member?(poll.voter_ids, voter_id) ->
            {{:error, :already_voted}, state}

          true ->
            updated = %{
              poll
              | votes: Map.update!(poll.votes, option, &(&1 + 1)),
                voter_ids: MapSet.put(poll.voter_ids, voter_id)
            }

            {{:ok, poll_view(updated)}, put_in(state, [:polls, id], updated)}
        end
    end
  end

  defp execute_operation(_operation, state), do: {{:error, :invalid_operation}, state}

  defp validate_question(question) do
    if valid_identifier?(question), do: :ok, else: {:error, :invalid_question}
  end

  defp validate_options(options) when is_list(options) do
    valid =
      length(options) >= 2 and
        Enum.all?(options, &valid_identifier?/1) and
        length(Enum.uniq(options)) == length(options)

    if valid, do: :ok, else: {:error, :invalid_options}
  end

  defp validate_options(_), do: {:error, :invalid_options}

  defp valid_identifier?(value) do
    is_binary(value) and String.trim(value) != ""
  end

  defp poll_view(poll) do
    Map.drop(poll, [:voter_ids])
    |> Map.put(:voter_count, MapSet.size(poll.voter_ids))
  end

  defp public_operation(operation) do
    Map.take(operation, [:id, :status, :result])
  end
end
