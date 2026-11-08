# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule BookLoans do
  use GenServer

  @table __MODULE__
  @match_spec [{{:"$1", :"$2"}, [], [:"$2"]}]

  @doc """
  Inicia o armazenamento DETS. Apenas uma instância pode ser iniciada por nó.
  """
  def start_link(path, opts \\ []) do
    GenServer.start_link(__MODULE__, {path, opts}, name: __MODULE__)
  end

  @doc "Encerra o armazenamento."
  def stop(reason \\ :normal) do
    GenServer.stop(__MODULE__, reason)
  end

  @doc "Cadastra um empréstimo com o identificador informado."
  def create(id, attrs, opts \\ []) when is_map(attrs) do
    GenServer.call(__MODULE__, {:create, id, attrs, opts})
  end

  def create(_id, _attrs, _opts), do: {:error, :invalid_record}

  @doc "Consulta um empréstimo pelo identificador."
  def get(id, opts \\ []) do
    case retry(fn -> :dets.lookup(@table, id) end, opts) do
      [{^id, loan}] -> {:ok, loan}
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Atualiza os campos de um empréstimo existente."
  def update(id, attrs, opts \\ []) when is_map(attrs) do
    GenServer.call(__MODULE__, {:update, id, attrs, opts})
  end

  def update(_id, _attrs, _opts), do: {:error, :invalid_record}

  @doc "Remove um empréstimo pelo identificador."
  def delete(id, opts \\ []) do
    GenServer.call(__MODULE__, {:delete, id, opts})
  end

  @doc """
  Retorna um stream de empréstimos, lido em lotes sem carregar toda a tabela
  na memória. Aceita `:chunk_size`, `:retries` e `:retry_delay`.
  """
  def stream(opts \\ []) do
    chunk_size = Keyword.get(opts, :chunk_size, 500)

    if not (is_integer(chunk_size) and chunk_size > 0) do
      raise ArgumentError, ":chunk_size deve ser um inteiro positivo"
    end

    Stream.resource(
      fn -> :start end,
      fn state -> stream_next(state, chunk_size, opts) end,
      fn _state -> :ok end
    )
  end

  @impl true
  def init({path, opts}) do
    file =
      if is_binary(path) do
        String.to_charlist(path)
      else
        path
      end

    case :dets.open_file(@table, [
           {:file, file},
           {:type, :set},
           {:keypos, 1},
           {:repair, true}
         ]) do
      {:ok, @table} ->
        {:ok,
         %{
           retries: Keyword.get(opts, :retries, 3),
           retry_delay: Keyword.get(opts, :retry_delay, 50)
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call({:create, id, attrs, opts}, _from, state) do
    loan = Map.put(attrs, :id, id)

    result =
      retry(
        fn ->
          case :dets.lookup(@table, id) do
            [] ->
              case :dets.insert(@table, {id, loan}) do
                :ok -> {:ok, loan}
                {:error, reason} -> {:error, reason}
              end

            [{^id, _existing}] ->
              {:error, :already_exists}

            {:error, reason} ->
              {:error, reason}
          end
        end,
        retry_options(state, opts)
      )

    {:reply, result, state}
  end

  def handle_call({:update, id, attrs, opts}, _from, state) do
    result =
      retry(
        fn ->
          case :dets.lookup(@table, id) do
            [{^id, existing}] ->
              loan =
                existing
                |> Map.merge(Map.delete(attrs, :id))
                |> Map.put(:id, id)

              case :dets.insert(@table, {id, loan}) do
                :ok -> {:ok, loan}
                {:error, reason} -> {:error, reason}
              end

            [] ->
              {:error, :not_found}

            {:error, reason} ->
              {:error, reason}
          end
        end,
        retry_options(state, opts)
      )

    {:reply, result, state}
  end

  def handle_call({:delete, id, opts}, _from, state) do
    result =
      retry(
        fn ->
          case :dets.lookup(@table, id) do
            [{^id, loan}] ->
              case :dets.delete(@table, id) do
                :ok -> {:ok, loan}
                {:error, reason} -> {:error, reason}
              end

            [] ->
              {:error, :not_found}

            {:error, reason} ->
              {:error, reason}
          end
        end,
        retry_options(state, opts)
      )

    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp stream_next(:start, chunk_size, opts) do
    stream_result(fn -> :dets.select(@table, @match_spec, chunk_size) end, chunk_size, opts)
  end

  defp stream_next({:continuation, continuation}, chunk_size, opts) do
    stream_result(fn -> :dets.select(continuation) end, chunk_size, opts)
  end

  defp stream_result(operation, chunk_size, opts) do
    case retry(operation, opts) do
      {[], continuation} ->
        stream_next({:continuation, continuation}, chunk_size, opts)

      {records, continuation} when is_list(records) ->
        {records, {:continuation, continuation}}

      :"$end_of_table" ->
        {:halt, :done}

      {:error, reason} ->
        raise "Falha ao percorrer empréstimos: #{inspect(reason)}"
    end
  end

  defp retry_options(state, opts) do
    [
      retries: Keyword.get(opts, :retries, state.retries),
      retry_delay: Keyword.get(opts, :retry_delay, state.retry_delay)
    ]
  end

  defp retry(operation, opts) do
    attempts = Keyword.get(opts, :retries, 3)
    delay = Keyword.get(opts, :retry_delay, 50)
    retry(operation, attempts, delay)
  end

  defp retry(operation, attempts, delay) do
    result =
      try do
        operation.()
      catch
        :exit, reason -> {:error, reason}
        :error, reason -> {:error, reason}
        :throw, reason -> {:error, reason}
      end

    case result do
      {:error, reason} = error when attempts > 1 ->
        if transient?(reason) do
          Process.sleep(delay)
          retry(operation, attempts - 1, delay)
        else
          error
        end

      other ->
        other
    end
  end

  defp transient?(reason)
       when reason in [:timeout, :busy, :eagain, :system_limit, :enomem, :eio, :enospc],
       do: true

  defp transient?({_tag, _detail, reason}), do: transient?(reason)
  defp transient?({_tag, reason}), do: transient?(reason)
  defp transient?(_reason), do: false
end
