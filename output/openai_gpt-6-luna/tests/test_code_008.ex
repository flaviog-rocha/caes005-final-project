# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Permitir a execução de operações simultâneas.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Permitir comunicação entre diferentes processos.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir e não adicione dependências externas. Inclua testes ExUnit que verifiquem os requisitos. Não escreva nada além do código (e comentários do código).

defmodule CSVProcessor do
  use GenServer

  @moduledoc """
  Processes CSV files concurrently and reports operation status to callers.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, opts)
  end

  def process_file(path, server \\ __MODULE__)

  def process_file(path, server) when is_binary(path) and byte_size(path) > 0 do
    GenServer.call(server, {:process_file, path})
  end

  def process_file(_path, _server), do: {:error, {:validation, :invalid_path}}

  def status(operation_id, server \\ __MODULE__) do
    GenServer.call(server, {:status, operation_id})
  end

  def subscribe(server \\ __MODULE__, subscriber \\ self())

  def subscribe(server, subscriber) when is_pid(subscriber) do
    GenServer.call(server, {:subscribe, subscriber})
  end

  def subscribe(_server, _subscriber), do: {:error, {:validation, :invalid_subscriber}}

  @impl true
  def init(:ok) do
    case Task.Supervisor.start_link() do
      {:ok, task_supervisor} ->
        {:ok, %{task_supervisor: task_supervisor, operations: %{}, subscribers: MapSet.new()}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:process_file, path}, _from, state) do
    operation_id = make_ref()
    server = self()

    task =
      Task.Supervisor.start_child(state.task_supervisor, fn ->
        result = process_csv_file(path)
        send(server, {:operation_finished, operation_id, result})
      end)

    case task do
      {:ok, _pid} ->
        operation = %{
          status: :running,
          path: path,
          started_at: System.monotonic_time(:millisecond)
        }

        notify(state.subscribers, {:started, operation_id, path})

        {:reply, {:ok, operation_id},
         put_in(state.operations[operation_id], operation)}

      {:error, reason} ->
        {:reply, {:error, {:operational, reason}}, state}
    end
  end

  def handle_call({:status, operation_id}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        {:reply, {:ok, operation}, state}

      :error ->
        {:reply, {:error, {:validation, :unknown_operation}}, state}
    end
  end

  def handle_call({:subscribe, subscriber}, _from, state) do
    {:reply, :ok, update_in(state.subscribers, &MapSet.put(&1, subscriber))}
  end

  @impl true
  def handle_info({:operation_finished, operation_id, result}, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        updated_operation =
          operation
          |> Map.put(:status, :completed)
          |> Map.put(:result, result)
          |> Map.put(:finished_at, System.monotonic_time(:millisecond))

        updated_state = put_in(state.operations[operation_id], updated_operation)
        notify(state.subscribers, {:finished, operation_id, result})
        {:noreply, updated_state}

      :error ->
        {:noreply, state}
    end
  end

  defp notify(subscribers, message) do
    Enum.each(subscribers, fn subscriber ->
      send(subscriber, {:csv_processor, message})
    end)
  end

  defp process_csv_file(path) do
    case File.read(path) do
      {:ok, contents} ->
        case parse_csv(contents) do
          {:ok, rows} ->
            {:ok, %{rows: rows, row_count: length(rows)}}

          {:error, reason} ->
            {:error, {:validation, reason}}
        end

      {:error, reason} ->
        {:error, {:operational, reason}}
    end
  rescue
    exception ->
      {:error, {:operational, {:exception, exception.__struct__, Exception.message(exception)}}}
  end

  defp parse_csv(contents) do
    scan_csv(contents, 0, false, [], [], [], false)
  end

  defp scan_csv(contents, index, quoted?, field, row, rows, touched?) do
    size = byte_size(contents)

    cond do
      index == size and quoted? ->
        {:error, :unterminated_quoted_field}

      index == size and touched? ->
        {:ok, Enum.reverse([finish_row(field, row) | rows])}

      index == size ->
        {:ok, Enum.reverse(rows)}

      true ->
        char = :binary.at(contents, index)
        next_char = if index + 1 < size, do: :binary.at(contents, index + 1), else: nil

        cond do
          quoted? and char == ?" and next_char == ?" ->
            scan_csv(contents, index + 2, true, [?" | field], row, rows, true)

          quoted? and char == ?" ->
            scan_csv(contents, index + 1, false, field, row, rows, true)

          quoted? ->
            scan_csv(contents, index + 1, true, [char | field], row, rows, true)

          char == ?" and field == [] ->
            scan_csv(contents, index + 1, true, field, row, rows, true)

          char == ?" ->
            {:error, :unexpected_quote}

          char == ?, ->
            scan_csv(contents, index + 1, false, [], [field_value(field) | row], rows, true)

          char == ?\n or char == ?\r ->
            next_index =
              if char == ?\r and next_char == ?\n do
                index + 2
              else
                index + 1
              end

            completed_rows = [finish_row(field, row) | rows]
            scan_csv(contents, next_index, false, [], [], completed_rows, false)

          true ->
            scan_csv(contents, index + 1, false, [char | field], row, rows, true)
        end
    end
  end

  defp finish_row(field, row), do: Enum.reverse([field_value(field) | row])

  defp field_value(field) do
    field
    |> Enum.reverse()
    |> :erlang.list_to_binary()
  end
end

ExUnit.start()

defmodule CSVProcessorTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, server} = CSVProcessor.start_link()
    %{server: server}
  end

  test "runs multiple operations concurrently and reports their results", %{server: server} do
    first_path = write_csv("name,age\nAda,36\n")
    second_path = write_csv("\"a,b\",c\nx,y\n")

    on_exit(fn ->
      File.rm(first_path)
      File.rm(second_path)
    end)

    assert :ok = CSVProcessor.subscribe(server)
    assert {:ok, first_id} = CSVProcessor.process_file(first_path, server)
    assert {:ok, second_id} = CSVProcessor.process_file(second_path, server)
    refute first_id == second_id

    first_result = await_finished(first_id)
    second_result = await_finished(second_id)

    assert {:ok, %{rows: [["name", "age"], ["Ada", "36"]], row_count: 2}} = first_result

    assert {:ok, %{rows: [["a,b", "c"], ["x", "y"]], row_count: 2}} =
             second_result

    assert {:ok, %{status: :completed, result: ^first_result}} =
             CSVProcessor.status(first_id, server)

    assert {:ok, %{status: :completed, result: ^second_result}} =
             CSVProcessor.status(second_id, server)
  end

  test "distinguishes validation errors from operational failures", %{server: server} do
    assert {:error, {:validation, :invalid_path}} =
             CSVProcessor.process_file("", server)

    assert {:error, {:validation, :invalid_subscriber}} =
             CSVProcessor.subscribe(server, :not_a_pid)

    assert {:error, {:validation, :unknown_operation}} =
             CSVProcessor.status(make_ref(), server)

    missing_path = Path.join(System.tmp_dir!(), "missing-#{System.unique_integer([:positive])}.csv")
    assert {:ok, operation_id} = CSVProcessor.process_file(missing_path, server)

    assert {:error, {:operational, :enoent}} = await_finished(operation_id)

    assert {:ok, %{status: :completed, result: {:error, {:operational, :enoent}}}} =
             CSVProcessor.status(operation_id, server)
  end

  test "reports malformed CSV as a validation error", %{server: server} do
    path = write_csv("\"unterminated")

    on_exit(fn -> File.rm(path) end)

    assert {:ok, operation_id} = CSVProcessor.process_file(path, server)
    assert {:error, {:validation, :unterminated_quoted_field}} = await_finished(operation_id)
  end

  defp await_finished(operation_id) do
    receive do
      {:csv_processor, {:finished, ^operation_id, result}} ->
        result

      {:csv_processor, _other_message} ->
        await_finished(operation_id)
    after
      5_000 ->
        flunk("timed out waiting for operation #{inspect(operation_id)}")
    end
  end

  defp write_csv(contents) do
    path = Path.join(System.tmp_dir!(), "csv-processor-#{System.unique_integer([:positive])}.csv")
    :ok = File.write(path, contents)
    path
  end
end
