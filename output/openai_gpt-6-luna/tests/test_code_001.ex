# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, devendo ter os seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# O código deve ser funcional, organizado e acompanhado de testes automatizados que verifiquem os requisitos. No início do código, coloque um comentário com exatamente esse prompt, excluindo apenas este trecho final. Não faça nada além do que foi solicitado.

# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, devendo ter os seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
# 4. Validar os dados recebidos antes de realizar operações.
#
# O código deve ser funcional, organizado e acompanhado de testes automatizados que verifiquem os requisitos.

# mix.exs
defmodule CsvProcessor.MixProject do
  use Mix.Project

  def project do
    [
      app: :csv_processor,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:nimble_csv, "~> 1.2"}
    ]
  end
end

# lib/csv_processor/csv.ex
defmodule CsvProcessor.CSV do
  NimbleCSV.define(Parser, separator: ",", escape: "\"")

  @headers ["entity_id", "operation", "key", "value"]

  @doc """
  Processa o conteúdo de um CSV.

  `store` deve ser uma tupla `{modulo, referencia}`, cujo módulo implemente
  `apply_batch/3`. `max_concurrency` limita o número de entidades processadas
  simultaneamente.
  """
  def process_string(content, store, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    with :ok <- validate_concurrency(max_concurrency),
         {:ok, rows} <- parse_and_validate(content) do
      rows
      |> Enum.group_by(& &1.entity_id)
      |> process_entities(store, max_concurrency)
    end
  end

  @doc "Lê e processa um arquivo CSV."
  def process_file(path, store, opts \\ []) do
    case File.read(path) do
      {:ok, content} -> process_string(content, store, opts)
      {:error, reason} -> {:error, {:file_read_error, reason}}
    end
  end

  defp validate_concurrency(value) when is_integer(value) and value > 0, do: :ok
  defp validate_concurrency(_), do: {:error, :invalid_max_concurrency}

  defp parse_and_validate(content) do
    try do
      case Parser.parse_string(content) do
        [@headers | records] ->
          records
          |> Enum.with_index(2)
          |> Enum.reduce_while({:ok, []}, fn {record, line}, {:ok, acc} ->
            case validate_record(record, line) do
              {:ok, row} -> {:cont, {:ok, [row | acc]}}
              error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, rows} -> {:ok, Enum.reverse(rows)}
            error -> error
          end

        _ ->
          {:error, {:invalid_header, @headers}}
      end
    rescue
      error -> {:error, {:invalid_csv, Exception.message(error)}}
    end
  end

  defp validate_record(
         [entity_id, operation, key, value],
         line
       ) do
    cond do
      String.trim(entity_id) == "" ->
        invalid_row(line, :empty_entity_id)

      String.trim(key) == "" ->
        invalid_row(line, :empty_key)

      operation not in ["set", "delete"] ->
        invalid_row(line, :invalid_operation)

      operation == "delete" and value != "" ->
        invalid_row(line, :delete_must_have_empty_value)

      true ->
        {:ok,
         %{
           entity_id: entity_id,
           operation: operation,
           key: key,
           value: value
         }}
    end
  end

  defp validate_record(_record, line), do: invalid_row(line, :invalid_column_count)

  defp invalid_row(line, reason), do: {:error, {:invalid_row, line, reason}}

  defp process_entities(rows_by_entity, {store_module, store_ref}, max_concurrency) do
    results =
      rows_by_entity
      |> Task.async_stream(
        fn {entity_id, rows} ->
          operations = Enum.map(rows, &to_operation/1)

          result =
            try do
              store_module.apply_batch(store_ref, entity_id, operations)
            rescue
              error -> {:error, {:store_exception, Exception.message(error)}}
            catch
              kind, reason -> {:error, {:store_failure, kind, reason}}
            end

          {entity_id, result}
        end,
        max_concurrency: max_concurrency,
        ordered: false,
        timeout: :infinity
      )
      |> Enum.reduce(%{}, fn
        {:ok, {entity_id, result}}, acc ->
          Map.put(acc, entity_id, result)

        {:exit, reason}, acc ->
          Map.put(acc, :worker_error, {:error, reason})
      end)

    {:ok, results}
  end

  defp to_operation(%{operation: "set", key: key, value: value}),
    do: {:set, key, value}

  defp to_operation(%{operation: "delete", key: key}),
    do: {:delete, key}
end

# lib/csv_processor/store.ex
defmodule CsvProcessor.Store do
  @moduledoc """
  Armazenamento em memória com aplicação atômica de operações por entidade.
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, opts)
  end

  @doc "Aplica todas as operações de uma entidade ou nenhuma delas."
  def apply_batch(pid, entity_id, operations) do
    GenServer.call(pid, {:apply_batch, entity_id, operations})
  end

  def get(pid, entity_id, key) do
    GenServer.call(pid, {:get, entity_id, key})
  end

  def snapshot(pid) do
    GenServer.call(pid, :snapshot)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:apply_batch, entity_id, operations}, _from, state) do
    current_entity = Map.get(state, entity_id, %{})

    case apply_operations(current_entity, operations) do
      {:ok, updated_entity} ->
        {:reply, :ok, Map.put(state, entity_id, updated_entity)}

      {:error, reason} ->
        # O estado original permanece intacto se qualquer operação falhar.
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get, entity_id, key}, _from, state) do
    value = state |> Map.get(entity_id, %{}) |> Map.get(key)
    {:reply, value, state}
  end

  def handle_call(:snapshot, _from, state), do: {:reply, state, state}

  defp apply_operations(entity, operations) do
    Enum.reduce_while(operations, {:ok, entity}, fn
      {:set, key, value}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, key, value)}}

      {:delete, key}, {:ok, acc} ->
        if Map.has_key?(acc, key) do
          {:cont, {:ok, Map.delete(acc, key)}}
        else
          {:halt, {:error, {:key_not_found, key}}}
        end

      operation, {:ok, _acc} ->
        {:halt, {:error, {:invalid_operation, operation}}}
    end)
  end
end

# test/test_helper.exs
ExUnit.start()

# test/csv_processor_test.exs
defmodule CsvProcessor.TrackingStore do
  def apply_batch(agent, _entity_id, _operations) do
    Agent.update(agent, fn state ->
      active = state.active + 1
      %{state | active: active, max: max(active, state.max)}
    end)

    Process.sleep(50)

    Agent.update(agent, fn state -> %{state | active: state.active - 1} end)
    :ok
  end
end

defmodule CsvProcessorTest do
  use ExUnit.Case, async: true

  alias CsvProcessor.CSV
  alias CsvProcessor.Store

  setup do
    {:ok, store} = Store.start_link()
    %{store: store}
  end

  test "valida todos os dados antes de executar operações", %{store: store} do
    csv = """
    entity_id,operation,key,value
    cliente-1,set,nome,Ana
    cliente-2,invalid,chave,valor
    """

    assert {:error, {:invalid_row, 3, :invalid_operation}} =
             CSV.process_string(csv, {Store, store}, max_concurrency: 2)

    assert Store.snapshot(store) == %{}
  end

  test "reverte todas as operações de uma entidade quando uma delas falha", %{
    store: store
  } do
    assert :ok = Store.apply_batch(store, "cliente-1", [{:set, "nome", "Ana"}])

    csv = """
    entity_id,operation,key,value
    cliente-1,set,cidade,Recife
    cliente-1,delete,chave-inexistente,
    """

    assert {:ok, %{"cliente-1" => {:error, {:key_not_found, "chave-inexistente"}}}} =
             CSV.process_string(csv, {Store, store}, max_concurrency: 1)

    assert Store.snapshot(store) == %{"cliente-1" => %{"nome" => "Ana"}}
  end

  test "processa entidades independentemente", %{store: store} do
    csv = """
    entity_id,operation,key,value
    cliente-1,delete,inexistente,
    cliente-2,set,nome,Bia
    """

    assert {:ok, results} = CSV.process_string(csv, {Store, store}, max_concurrency: 2)

    assert results["cliente-1"] == {:error, {:key_not_found, "inexistente"}}
    assert results["cliente-2"] == :ok
    assert Store.get(store, "cliente-2", "nome") == "Bia"
    assert Store.get(store, "cliente-1", "nome") == nil
  end

  test "limita a quantidade de entidades processadas simultaneamente" do
    {:ok, tracker} = Agent.start_link(fn -> %{active: 0, max: 0} end)

    csv = """
    entity_id,operation,key,value
    cliente-1,set,chave,valor
    cliente-2,set,chave,valor
    cliente-3,set,chave,valor
    cliente-4,set,chave,valor
    """

    assert {:ok, results} =
             CSV.process_string(
               csv,
               {CsvProcessor.TrackingStore, tracker},
               max_concurrency: 2
             )

    assert Enum.all?(Map.values(results), &(&1 == :ok))
    assert Agent.get(tracker, & &1.max) == 2
  end

  test "rejeita limite de concorrência inválido", %{store: store} do
    csv = """
    entity_id,operation,key,value
    cliente-1,set,nome,Ana
    """

    assert {:error, :invalid_max_concurrency} =
             CSV.process_string(csv, {Store, store}, max_concurrency: 0)

    assert Store.snapshot(store) == %{}
  end
end
