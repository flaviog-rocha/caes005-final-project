# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de estoque de uma loja, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Permitir consultar o estado atual das operações em andamento.
# 3. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Inventory do
  use GenServer

  @moduledoc """
  Gerenciador de estoque com processamento de arquivos em fluxo e histórico
  persistido em um arquivo append-only.

  Cada linha de entrada deve ter o formato `sku,quantidade`, em que a
  quantidade é um inteiro diferente de zero. Valores negativos representam
  saídas de estoque.
  """

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def submit_file(server, path) do
    GenServer.call(server, {:submit_file, path})
  end

  def get_operation(server, operation_id) do
    GenServer.call(server, {:get_operation, operation_id})
  end

  def operations(server) do
    GenServer.call(server, :operations)
  end

  def stock(server) do
    GenServer.call(server, :stock)
  end

  def stock(server, sku) do
    GenServer.call(server, {:stock, sku})
  end

  def history(server) do
    path = GenServer.call(server, :history_path)

    path
    |> File.stream!([], :line)
    |> Stream.map(&decode_event!/1)
  end

  @impl true
  def init(opts) do
    journal_path = Keyword.get(opts, :journal, "inventory.log")

    with :ok <- File.mkdir_p(Path.dirname(journal_path)),
         {:ok, inventory} <- load_inventory(journal_path) do
      {:ok,
       %{
         journal_path: journal_path,
         inventory: inventory,
         operations: %{}
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:submit_file, path}, _from, state) do
    operation_id =
      System.unique_integer([:positive, :monotonic])
      |> Integer.to_string()

    started_at = timestamp()

    operation = %{
      id: operation_id,
      path: path,
      status: "running",
      processed: 0,
      started_at: started_at
    }

    event = %{
      type: "operation_started",
      operation_id: operation_id,
      path: path,
      timestamp: started_at
    }

    case append_event(state.journal_path, event) do
      :ok ->
        case Task.start(fn -> process_file(self_server(), operation_id, path) end) do
          {:ok, _pid} ->
            {:reply, {:ok, operation_id},
             put_in(state.operations[operation_id], operation)}

          {:error, reason} ->
            failed_at = timestamp()

            failure_event = %{
              type: "operation_failed",
              operation_id: operation_id,
              reason: inspect(reason),
              timestamp: failed_at
            }

            _ = append_event(state.journal_path, failure_event)

            failed_operation =
              operation
              |> Map.put(:status, "failed")
              |> Map.put(:error, inspect(reason))
              |> Map.put(:finished_at, failed_at)

            {:reply, {:error, reason},
             put_in(state.operations[operation_id], failed_operation)}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get_operation, operation_id}, _from, state) do
    {:reply, Map.fetch(state.operations, operation_id), state}
  end

  @impl true
  def handle_call(:operations, _from, state) do
    {:reply, state.operations, state}
  end

  @impl true
  def handle_call(:stock, _from, state) do
    {:reply, state.inventory, state}
  end

  @impl true
  def handle_call({:stock, sku}, _from, state) do
    {:reply, Map.get(state.inventory, sku, 0), state}
  end

  @impl true
  def handle_call(:history_path, _from, state) do
    {:reply, state.journal_path, state}
  end

  @impl true
  def handle_call({:apply_item, operation_id, sku, delta}, _from, state) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, %{status: "running"} = operation} ->
        current_stock = Map.get(state.inventory, sku, 0)
        new_stock = current_stock + delta

        cond do
          new_stock < 0 ->
            {:reply, {:error, "estoque insuficiente para #{sku}"}, state}

          true ->
            event = %{
              type: "item_applied",
              operation_id: operation_id,
              sku: sku,
              delta: delta,
              stock_after: new_stock,
              timestamp: timestamp()
            }

            case append_event(state.journal_path, event) do
              :ok ->
                updated_operation =
                  Map.update!(operation, :processed, &(&1 + 1))

                updated_state =
                  state
                  |> put_in([:inventory, sku], new_stock)
                  |> put_in([:operations, operation_id], updated_operation)

                {:reply, :ok, updated_state}

              {:error, reason} ->
                {:reply, {:error, "falha ao gravar histórico: #{inspect(reason)}"},
                 state}
            end
        end

      _ ->
        {:reply, {:error, "operação inexistente ou não está em andamento"}, state}
    end
  end

  @impl true
  def handle_call({:finish_operation, operation_id}, _from, state) do
    finish_operation(state, operation_id, "completed", nil)
  end

  @impl true
  def handle_call({:fail_operation, operation_id, reason}, _from, state) do
    finish_operation(state, operation_id, "failed", reason)
  end

  defp finish_operation(state, operation_id, status, reason) do
    case Map.fetch(state.operations, operation_id) do
      {:ok, operation} ->
        finished_at = timestamp()

        event =
          if status == "completed" do
            %{
              type: "operation_completed",
              operation_id: operation_id,
              timestamp: finished_at
            }
          else
            %{
              type: "operation_failed",
              operation_id: operation_id,
              reason: to_string(reason),
              timestamp: finished_at
            }
          end

        updated_operation =
          operation
          |> Map.put(:status, status)
          |> Map.put(:finished_at, finished_at)
          |> maybe_put_error(reason)

        updated_state =
          put_in(state.operations[operation_id], updated_operation)

        case append_event(state.journal_path, event) do
          :ok -> {:reply, :ok, updated_state}
          {:error, append_reason} ->
            {:reply, {:error, append_reason}, updated_state}
        end

      :error ->
        {:reply, {:error, "operação inexistente"}, state}
    end
  end

  defp maybe_put_error(operation, nil), do: operation
  defp maybe_put_error(operation, reason), do: Map.put(operation, :error, to_string(reason))

  defp process_file(server, operation_id, path) do
    result =
      try do
        path
        |> File.stream!([], :line)
        |> Stream.with_index(1)
        |> Enum.reduce_while(:ok, fn {line, line_number}, :ok ->
          if String.trim(line) == "" do
            {:cont, :ok}
          else
            case parse_line(line) do
              {:ok, sku, delta} ->
                case GenServer.call(server, {:apply_item, operation_id, sku, delta}) do
                  :ok -> {:cont, :ok}
                  {:error, reason} ->
                    {:halt, {:error, "linha #{line_number}: #{reason}"}}
                end

              {:error, reason} ->
                {:halt, {:error, "linha #{line_number}: #{reason}"}}
            end
          end
        end)
      rescue
        exception -> {:error, Exception.message(exception)}
      catch
        kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
      end

    case result do
      :ok ->
        GenServer.call(server, {:finish_operation, operation_id})

      {:error, reason} ->
        GenServer.call(server, {:fail_operation, operation_id, reason})
    end
  end

  defp parse_line(line) do
    case String.split(String.trim(line), ",", parts: 2) do
      [sku, raw_delta] ->
        sku = String.trim(sku)
        raw_delta = String.trim(raw_delta)

        case Integer.parse(raw_delta) do
          {delta, ""} when sku != "" and delta != 0 ->
            {:ok, sku, delta}

          _ ->
            {:error, "SKU ou quantidade inválidos"}
        end

      _ ->
        {:error, "formato inválido; esperado SKU,quantidade"}
    end
  end

  defp load_inventory(path) do
    if File.exists?(path) do
      try do
        inventory =
          path
          |> File.stream!([], :line)
          |> Enum.reduce(%{}, fn line, acc ->
            case decode_event(line) do
              %{type: "item_applied", sku: sku, delta: delta} ->
                Map.update(acc, sku, delta, &(&1 + delta))

              _ ->
                acc
            end
          end)

        {:ok, inventory}
      rescue
        exception -> {:error, Exception.message(exception)}
      end
    else
      {:ok, %{}}
    end
  end

  defp append_event(path, event) do
    encoded =
      event
      |> :erlang.term_to_binary()
      |> Base.encode64()

    File.write(path, encoded <> "\n", [:append])
  end

  defp decode_event!(line), do: decode_event(line)

  defp decode_event(line) do
    line
    |> String.trim()
    |> Base.decode64!()
    |> :erlang.binary_to_term([:safe])
  end

  defp timestamp do
    DateTime.utc_now()
    |> DateTime.to_iso8601()
  end

  # Captura o PID do servidor antes de iniciar a tarefa de processamento.
  defp self_server do
    Process.get(:inventory_server)
  end
end
