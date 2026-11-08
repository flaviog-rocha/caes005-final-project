# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DurableTaskQueue do
  use Supervisor

  @default_path "task_queue.term"

  def start_link(opts \\ []) do
    path = Keyword.get(opts, :path, @default_path)
    server = Keyword.get(opts, :name, __MODULE__)
    supervisor_opts =
      case Keyword.get(opts, :supervisor_name) do
        nil -> []
        name -> [name: name]
      end

    Supervisor.start_link(__MODULE__, {path, server}, supervisor_opts)
  end

  @impl true
  def init({path, server}) do
    agent_opts = if server, do: [name: server], else: []

    child = %{
      id: {:queue_agent, server},
      start: {Agent, :start_link, [fn -> load_state(path) end, agent_opts]},
      restart: :permanent,
      shutdown: 5_000,
      type: :worker
    }

    Supervisor.init([child], strategy: :one_for_one)
  end

  def enqueue(task, server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      queue = state.queue ++ [task]

      case persist(queue, state.path) do
        :ok -> {:ok, %{state | queue: queue}}
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  def dequeue(server \\ __MODULE__) do
    Agent.get_and_update(server, fn
      %{queue: []} = state ->
        {:empty, state}

      %{queue: [task | rest]} = state ->
        case persist(rest, state.path) do
          :ok -> {{:ok, task}, %{state | queue: rest}}
          {:error, reason} -> {{:error, reason}, state}
        end
    end)
  end

  def peek(server \\ __MODULE__) do
    Agent.get(server, fn
      %{queue: [task | _]} -> {:ok, task}
      %{queue: []} -> :empty
    end)
  end

  def all(server \\ __MODULE__) do
    Agent.get(server, & &1.queue)
  end

  def size(server \\ __MODULE__) do
    Agent.get(server, fn state -> length(state.queue) end)
  end

  def clear(server \\ __MODULE__) do
    Agent.get_and_update(server, fn state ->
      case persist([], state.path) do
        :ok -> {:ok, %{state | queue: []}}
        {:error, reason} -> {{:error, reason}, state}
      end
    end)
  end

  defp load_state(path) do
    queue =
      case File.read(path) do
        {:ok, contents} ->
          case decode(contents) do
            {:ok, queue} -> queue
            {:error, reason} -> raise "Could not load task queue: #{inspect(reason)}"
          end

        {:error, :enoent} ->
          []

        {:error, reason} ->
          raise "Could not read task queue: #{inspect(reason)}"
      end

    %{queue: queue, path: path}
  end

  defp decode(contents) do
    try do
      case :erlang.binary_to_term(contents) do
        {:durable_task_queue, 1, queue} when is_list(queue) -> {:ok, queue}
        _ -> {:error, :invalid_format}
      end
    rescue
      ArgumentError -> {:error, :invalid_term}
    end
  end

  defp persist(queue, path) do
    with :ok <- File.mkdir_p(Path.dirname(path)) do
      temporary_path =
        path <> ".tmp." <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

      contents = :erlang.term_to_binary({:durable_task_queue, 1, queue})

      case File.write(temporary_path, contents, [:binary, :sync]) do
        :ok ->
          case File.rename(temporary_path, path) do
            :ok -> :ok
            {:error, reason} ->
              File.rm(temporary_path)
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end
end
