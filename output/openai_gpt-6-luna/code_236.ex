# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule LoanManager do
  use GenServer

  @table __MODULE__
  @default_chunk_size 500

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.take(opts, [:name]))
  end

  def stop(server \\ __MODULE__) do
    GenServer.stop(server)
  end

  @impl true
  def init(:ok) do
    :ets.new(@table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end

  def create(record, opts \\ []) when is_map(record) do
    with {:ok, id} <- Map.fetch(record, :id) do
      retry(
        fn ->
          with_key_lock(id, fn ->
            if :ets.insert_new(@table, {id, record}) do
              {:ok, record}
            else
              {:error, :already_exists}
            end
          end)
        end,
        opts
      )
    else
      :error -> {:error, :missing_id}
    end
  end

  def create(_record, _opts), do: {:error, :invalid_record}

  def get(id, opts \\ []) do
    retry(
      fn ->
        case :ets.lookup(@table, id) do
          [{^id, record}] -> {:ok, record}
          [] -> {:error, :not_found}
        end
      end,
      opts
    )
  end

  def update(id, attrs, opts \\ []) when is_map(attrs) do
    retry(
      fn ->
        with_key_lock(id, fn ->
          case :ets.lookup(@table, id) do
            [{^id, record}] ->
              updated = record |> Map.merge(Map.delete(attrs, :id)) |> Map.put(:id, id)
              true = :ets.insert(@table, {id, updated})
              {:ok, updated}

            [] ->
              {:error, :not_found}
          end
        end)
      end,
      opts
    )
  end

  def update(_id, _attrs, _opts), do: {:error, :invalid_attributes}

  def delete(id, opts \\ []) do
    retry(
      fn ->
        with_key_lock(id, fn ->
          case :ets.lookup(@table, id) do
            [{^id, record}] ->
              true = :ets.delete(@table, id)
              {:ok, record}

            [] ->
              {:error, :not_found}
          end
        end)
      end,
      opts
    )
  end

  def records_stream(opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)

    Stream.resource(
      fn -> select_batch(chunk_size) end,
      fn
        :done ->
          {:halt, :done}

        {:batch, entries, continuation} ->
          records = Enum.map(entries, fn {_id, record} -> record end)
          {records, {:continuation, continuation}}

        {:continuation, continuation} ->
          case select_next(continuation, chunk_size) do
            :done -> {:halt, :done}
            batch -> {[], batch}
          end
      end,
      fn _state -> :ok end
    )
  end

  def import_stream(enumerable, opts \\ []) do
    retry_opts = Keyword.take(opts, [:retries, :backoff])

    enumerable
    |> Task.async_stream(
      fn record -> create(record, retry_opts) end,
      max_concurrency: Keyword.get(opts, :max_concurrency, System.schedulers_online()),
      ordered: false,
      timeout: Keyword.get(opts, :timeout, :infinity)
    )
    |> Stream.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:task_exit, reason}}
    end)
  end

  def retry(operation, opts \\ []) when is_function(operation, 0) do
    retries = Keyword.get(opts, :retries, 3)
    backoff = Keyword.get(opts, :backoff, 25)
    do_retry(operation, retries, backoff, 0)
  end

  defp do_retry(operation, retries, backoff, attempt) do
    result = operation.()

    if temporary_error?(result) and attempt < retries do
      Process.sleep(backoff * Integer.pow(2, attempt))
      do_retry(operation, retries, backoff, attempt + 1)
    else
      result
    end
  end

  defp temporary_error?({:error, :temporary}), do: true
  defp temporary_error?({:error, {:temporary, _reason}}), do: true
  defp temporary_error?(_result), do: false

  defp with_key_lock(id, operation) do
    :global.trans({{@table, id}, self()}, operation)
  end

  defp select_batch(chunk_size) do
    match_spec = [{:"$1", [], [:"$1"]}]

    case :ets.select(@table, match_spec, chunk_size) do
      :"$end_of_table" -> :done
      {entries, continuation} -> {:batch, entries, continuation}
    end
  end

  defp select_next(continuation, chunk_size) do
    case :ets.select(continuation, chunk_size) do
      :"$end_of_table" -> :done
      {entries, next_continuation} -> {:batch, entries, next_continuation}
    end
  end
end
