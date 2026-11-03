# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir e não adicione dependências externas. Inclua testes ExUnit que verifiquem os requisitos. Não escreva nada além do código (e comentários do código).

defmodule CsvProcessor do
  use GenServer

  @type operation_id :: reference()
  @type outcome ::
          {:ok, [[String.t()]]}
          | {:error, {:validation, term()} | {:operational, term()}}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.take(opts, [:name]))
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  def submit(server, path, opts \\ []) do
    with :ok <- validate_path(path),
         :ok <- validate_options(opts) do
      GenServer.call(server, {:submit, path, opts})
    end
  end

  def status(server), do: GenServer.call(server, :status)

  def status(server, operation_id) do
    GenServer.call(server, {:status, operation_id})
  end

  @impl true
  def init(:ok) do
    {:ok, supervisor} = Task.Supervisor.start_link()

    {:ok,
     %{
       supervisor: supervisor,
       operations: %{},
       task_refs: %{}
     }}
  end

  @impl true
  def handle_call({:submit, path, opts}, _from, state) do
    id = make_ref()
    notify_to = Keyword.get(opts, :notify_to)

    task =
      Task.Supervisor.async_nolink(state.supervisor, fn ->
        process_file(path)
      end)

    operation = %{
      id: id,
      status: :running,
      started_at: System.monotonic_time(),
      notify_to: notify_to
    }

    next_state = %{
      state
      | operations: Map.put(state.operations, id, operation),
        task_refs: Map.put(state.task_refs, task.ref, id)
    }

    {:reply, {:ok, id}, next_state}
  end

  def handle_call(:status, _from, state) do
    {:reply, state.operations, state}
  end

  def handle_call({:status, id}, _from, state) do
    reply =
      case Map.fetch(state.operations, id) do
        {:ok, operation} -> {:ok, operation}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_info({ref, outcome}, state) do
    case Map.pop(state.task_refs, ref) do
      {nil, _task_refs} ->
        {:noreply, state}

      {id, task_refs} ->
        Process.demonitor(ref, [:flush])
        next_state = %{state | task_refs: task_refs}
        {:noreply, finish_operation(next_state, id, outcome)}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.task_refs, ref) do
      {nil, _task_refs} ->
        {:noreply, state}

      {id, task_refs} ->
        outcome = {:error, {:operational, {:task_exit, reason}}}
        next_state = %{state | task_refs: task_refs}
        {:noreply, finish_operation(next_state, id, outcome)}
    end
  end

  defp finish_operation(state, id, outcome) do
    operation = Map.fetch!(state.operations, id)

    updated_operation =
      case outcome do
        {:ok, _rows} ->
          operation
          |> Map.put(:status, :completed)
          |> Map.put(:result, outcome)

        {:error, _reason} ->
          operation
          |> Map.put(:status, :failed)
          |> Map.put(:error, outcome)
      end

    if operation.notify_to do
      send(operation.notify_to, {__MODULE__, id, outcome})
    end

    %{state | operations: Map.put(state.operations, id, updated_operation)}
  end

  defp validate_path(path) when is_binary(path) and byte_size(path) > 0, do: :ok
  defp validate_path(_), do: {:error, {:validation, :invalid_path}}

  defp validate_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Enum.all?(opts, fn {key, _} -> key == :notify_to end) do
      case Keyword.get(opts, :notify_to) do
        nil -> :ok
        pid when is_pid(pid) -> :ok
        _ -> {:error, {:validation, :invalid_notify_to}}
      end
    else
      {:error, {:validation, :invalid_options}}
    end
  end

  defp validate_options(_), do: {:error, {:validation, :invalid_options}}

  defp process_file(path) do
    case File.read(path) do
      {:ok, contents} ->
        parse_csv(contents)

      {:error, reason} ->
        {:error, {:operational, {:file_read, reason}}}
    end
  end

  defp parse_csv(""), do: {:ok, []}

  defp parse_csv(contents) do
    lines = String.split(contents, "\n", trim: false)
    lines = if String.ends_with?(contents, "\n"), do: Enum.drop(lines, -1), else: lines

    lines
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {line, line_number}, {:ok, rows} ->
      line = String.trim_trailing(line, "\r")

      case parse_line(String.to_charlist(line)) do
        {:ok, row} ->
          {:cont, {:ok, [row | rows]}}

        {:error, reason} ->
          {:halt, {:error, {:validation, {:invalid_csv, line_number, reason}}}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp parse_line(chars), do: parse_line(chars, [], [], :plain)

  defp parse_line([], current, fields, :quoted), do: {:error, :unclosed_quote}

  defp parse_line([], current, fields, _state) do
    row = Enum.reverse([current |> Enum.reverse() |> List.to_string() | fields])
    {:ok, row}
  end

  defp parse_line([?", ?" | rest], current, fields, :quoted) do
    parse_line(rest, [?" | current], fields, :quoted)
  end

  defp parse_line([?" | rest], current, fields, :quoted) do
    parse_line(rest, current, fields, :closed)
  end

  defp parse_line([_char | rest], current, fields, :quoted) do
    [char | _] = [_char]
    parse_line(rest, [char | current], fields, :quoted)
  end

  defp parse_line([?, | rest], current, fields, state) when state in [:plain, :closed] do
    parse_line(rest, [], [current |> Enum.reverse() |> List.to_string() | fields], :plain)
  end

  defp parse_line([?" | rest], [], fields, :plain) do
    parse_line(rest, [], fields, :quoted)
  end

  defp parse_line([?" | _rest], _current, _fields, :plain) do
    {:error, :unexpected_quote}
  end

  defp parse_line([_char | _rest], _current, _fields, :closed) do
    {:error, :characters_after_quote}
  end

  defp parse_line([char | rest], current, fields, :plain) do
    parse_line(rest, [char | current], fields, :plain)
  end
end

ExUnit.start()

defmodule CsvProcessorTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, server} = CsvProcessor.start_link()
    on_exit(fn -> if Process.alive?(server), do: GenServer.stop(server) end)
    {:ok, server: server}
  end

  test "runs multiple operations and exposes their current status", %{server: server} do
    path_one = write_csv("name,age\nAda,37\n")
    path_two = write_csv("city,country\nParis,France\n")

    assert {:ok, first_id} = CsvProcessor.submit(server, path_one, notify_to: self())
    assert {:ok, second_id} = CsvProcessor.submit(server, path_two, notify_to: self())

    operations = CsvProcessor.status(server)
    assert Map.has_key?(operations, first_id)
    assert Map.has_key?(operations, second_id)

    assert {:ok, first_operation} = CsvProcessor.status(server, first_id)
    assert first_operation.status in [:running, :completed]

    assert_receive {CsvProcessor, ^first_id, {:ok, [["name", "age"], ["Ada", "37"]]}}
    assert_receive {CsvProcessor, ^second_id, {:ok, [["city", "country"], ["Paris", "France"]]}}

    assert {:ok, %{status: :completed}} = CsvProcessor.status(server, first_id)
    assert {:ok, %{status: :completed}} = CsvProcessor.status(server, second_id)
  end

  test "distinguishes validation errors from operational failures", %{server: server} do
    assert {:error, {:validation, :invalid_path}} = CsvProcessor.submit(server, "")
    assert {:error, {:validation, :invalid_notify_to}} =
             CsvProcessor.submit(server, "file.csv", notify_to: :not_a_pid)

    missing_path = Path.join(System.tmp_dir!(), "missing_csv_#{System.unique_integer([:positive])}.csv")
    assert {:ok, id} = CsvProcessor.submit(server, missing_path, notify_to: self())

    assert_receive {CsvProcessor, ^id, {:error, {:operational, {:file_read, :enoent}}}}
    assert {:ok, %{status: :failed, error: {:error, {:operational, {:file_read, :enoent}}}}} =
             CsvProcessor.status(server, id)
  end

  test "reports malformed CSV as a validation failure", %{server: server} do
    path = write_csv("a,b\n\"unfinished,value\n")
    assert {:ok, id} = CsvProcessor.submit(server, path, notify_to: self())

    assert_receive {CsvProcessor, ^id, {:error, {:validation, {:invalid_csv, 2, :unclosed_quote}}}}
    assert {:ok, %{status: :failed}} = CsvProcessor.status(server, id)
  end

  defp write_csv(contents) do
    path = Path.join(System.tmp_dir!(), "csv_processor_#{System.unique_integer([:positive])}.csv")
    File.write!(path, contents)
    path
  end
end
