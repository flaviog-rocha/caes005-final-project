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

defmodule TaskQueue do
  use Supervisor

  @agent_name TaskQueue.Agent
  @default_path "task_queue.term"

  def start_link(opts \\ []) do
    path =
      opts
      |> Keyword.get(:path, @default_path)
      |> Path.expand()

    Supervisor.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(path) do
    children = [
      %{
        id: @agent_name,
        start: {Agent, :start_link, [fn -> load_state(path) end, [name: @agent_name]]},
        restart: :permanent
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def enqueue(task) do
    Agent.get_and_update(@agent_name, fn %{queue: queue, path: path} = state ->
      next_queue = :queue.in(task, queue)
      persist!(path, next_queue)
      {:ok, %{state | queue: next_queue}}
    end)
  end

  def dequeue do
    Agent.get_and_update(@agent_name, fn %{queue: queue, path: path} = state ->
      case :queue.out(queue) do
        {{:value, task}, next_queue} ->
          persist!(path, next_queue)
          {{:ok, task}, %{state | queue: next_queue}}

        {:empty, _queue} ->
          {:empty, state}
      end
    end)
  end

  def peek do
    Agent.get(@agent_name, fn %{queue: queue} ->
      case :queue.peek(queue) do
        {:value, task} -> {:ok, task}
        :empty -> :empty
      end
    end)
  end

  def size do
    Agent.get(@agent_name, fn %{queue: queue} -> :queue.len(queue) end)
  end

  def load_state(path) do
    queue =
      case File.read(path) do
        {:ok, binary} ->
          case :erlang.binary_to_term(binary) do
            {1, tasks} when is_list(tasks) -> :queue.from_list(tasks)
            _ -> raise "invalid task queue data in #{path}"
          end

        {:error, :enoent} ->
          :queue.new()

        {:error, reason} ->
          raise "could not read task queue data from #{path}: #{inspect(reason)}"
      end

    %{queue: queue, path: path}
  end

  defp persist!(path, queue) do
    directory = Path.dirname(path)
    temporary_path = path <> ".tmp"
    data = :erlang.term_to_binary({1, :queue.to_list(queue)}, [:compressed])

    File.mkdir_p!(directory)

    case File.open(temporary_path, [:write, :binary]) do
      {:ok, file} ->
        write_result =
          with :ok <- IO.binwrite(file, data),
               :ok <- :file.sync(file) do
            :ok
          end

        close_result = File.close(file)

        case {write_result, close_result} do
          {:ok, :ok} ->
            case File.rename(temporary_path, path) do
              :ok -> :ok
              {:error, reason} -> raise "could not replace task queue data: #{inspect(reason)}"
            end

          {{:error, reason}, _} ->
            raise "could not write task queue data: #{inspect(reason)}"

          {:ok, {:error, reason}} ->
            raise "could not close task queue data: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "could not open temporary task queue data file: #{inspect(reason)}"
    end
  end
end
