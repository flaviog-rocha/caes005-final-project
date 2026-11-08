# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciamento de empréstimos de livros em Elixir (biblioteca padrão apenas).
#
# Arquitetura:
#   * Escritas são serializadas por um GenServer (consistência e validação).
#   * Leituras são feitas diretamente em uma tabela ETS :protected com
#     read_concurrency, permitindo leituras simultâneas sem gargalo.
#   * Grandes volumes são consumidos via Stream (lazy, em lotes via
#     continuation do ETS e de arquivos via File.stream!), sem carregar tudo na memória.
#   * Falhas temporárias (timeouts, processo indisponível, {:error, :transient})
#     são re-tentadas com backoff exponencial e jitter.
#
# Uso:
#   {:ok, _} = Library.Loans.start_link()
#   {:ok, loan} = Library.Loans.create(%{book_id: "978-85", borrower: "Ana"})
#   {:ok, loan} = Library.Loans.update(loan.id, %{status: :returned})
#   {:ok, loan} = Library.Loans.get(loan.id)
#   :ok = Library.Loans.delete(loan.id)

defmodule Library.Retry do
  @moduledoc """
  Executa funções com novas tentativas e backoff exponencial com jitter.
  Considera falha temporária: retorno `{:error, :transient}` ou exit
  (ex.: timeout de GenServer.call ou processo indisponível).
  """

  @default_attempts 5
  @default_base_delay 50

  @spec run((-> term()), keyword()) :: term()
  def run(fun, opts \\ []) when is_function(fun, 0) do
    attempts = Keyword.get(opts, :attempts, @default_attempts)
    base = Keyword.get(opts, :base_delay, @default_base_delay)
    do_run(fun, 1, attempts, base)
  end

  defp do_run(fun, attempt, max, base) do
    case safe_call(fun) do
      {:error, :transient} when attempt < max ->
        Process.sleep(backoff(attempt, base))
        do_run(fun, attempt + 1, max, base)

      {:error, :transient} ->
        {:error, :retries_exhausted}

      result ->
        result
    end
  end

  # Exits (timeouts, noproc) são tratados como falhas temporárias.
  defp safe_call(fun) do
    fun.()
  catch
    :exit, _reason -> {:error, :transient}
  end

  # Backoff exponencial: base * 2^(tentativa-1) + jitter aleatório.
  defp backoff(attempt, base) do
    base * Integer.pow(2, attempt - 1) + :rand.uniform(base)
  end
end

defmodule Library.Loans do
  @moduledoc """
  Gerenciamento de empréstimos de livros.
  """

  use GenServer

  alias Library.Retry

  @table __MODULE__
  @loan_days 14
  @chunk_size 500
  @call_timeout 5_000

  defmodule Loan do
    @moduledoc false
    @enforce_keys [:id, :book_id, :borrower, :borrowed_at, :due_at]
    defstruct [
      :id,
      :book_id,
      :borrower,
      :borrowed_at,
      :due_at,
      :returned_at,
      status: :active
    ]
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc "Inicia o processo dono da tabela ETS e do estado de escrita."
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cadastra um novo empréstimo. Requer :book_id e :borrower."
  def create(attrs) when is_map(attrs) do
    Retry.run(fn -> call({:create, attrs}) end)
  end

  @doc "Consulta um empréstimo pelo id (leitura concorrente direto no ETS)."
  def get(id) do
    Retry.run(fn -> read(id) end)
  end

  @doc "Atualiza :borrower, :due_at e/ou :status de um empréstimo."
  def update(id, changes) when is_map(changes) do
    Retry.run(fn -> call({:update, id, changes}) end)
  end

  @doc "Remove um empréstimo."
  def delete(id) do
    Retry.run(fn -> call({:delete, id}) end)
  end

  @doc """
  Retorna um Stream lazy de empréstimos com o status informado.
  Os registros são lidos do ETS em lotes de `@chunk_size`, sem materializar
  a coleção inteira na memória.
  """
  def stream_by_status(status) when is_atom(status) do
    match_spec = [{{:_, :"$1", :"$2"}, [{:==, :"$1", status}], [:"$2"]}]

    Stream.resource(
      fn -> :ets.select(@table, match_spec, @chunk_size) end,
      fn
        :"$end_of_table" -> {:halt, :"$end_of_table"}
        {items, continuation} -> {items, :ets.select(continuation)}
      end,
      fn _ -> :ok end
    )
  end

  @doc "Stream lazy de empréstimos ativos cujo prazo já expirou."
  def overdue_stream(now \\ DateTime.utc_now()) do
    :active
    |> stream_by_status()
    |> Stream.filter(&(DateTime.compare(&1.due_at, now) == :lt))
  end

  @doc """
  Processa um stream com concorrência limitada, sem bufferizar tudo.
  Retorna um stream lazy com os resultados (fora de ordem).
  """
  def process_stream(stream, fun, opts \\ []) when is_function(fun, 1) do
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    Task.async_stream(stream, fun,
      max_concurrency: concurrency,
      ordered: false,
      timeout: :infinity
    )
    |> Stream.map(fn {:ok, result} -> result end)
  end

  @doc """
  Importa empréstimos de um CSV no formato `book_id,borrower` (uma linha por registro),
  processando linha a linha em streaming e com concorrência.
  Retorna um stream lazy de resultados `{:ok, loan} | {:error, reason}`.
  """
  def import_csv(path, opts \\ []) do
    path
    |> File.stream!()
    |> Stream.reject(&(String.trim(&1) == ""))
    |> Stream.map(&parse_line/1)
    |> process_stream(
      fn
        {:ok, attrs} -> create(attrs)
        {:error, _} = error -> error
      end,
      opts
    )
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer (escritas serializadas)
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :set,
      :protected,
      read_concurrency: true
    ])

    {:ok, %{next_id: 1}}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    case build_loan(attrs, state.next_id) do
      {:ok, loan} ->
        :ets.insert(@table, {loan.id, loan.status, loan})
        {:reply, {:ok, loan}, %{state | next_id: state.next_id + 1}}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:update, id, changes}, _from, state) do
    reply =
      case :ets.lookup(@table, id) do
        [{^id, _status, loan}] ->
          case apply_changes(loan, changes) do
            {:ok, updated} ->
              :ets.insert(@table, {id, updated.status, updated})
              {:ok, updated}

            {:error, _} = error ->
              error
          end

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply =
      case :ets.lookup(@table, id) do
        [{^id, _, _}] ->
          :ets.delete(@table, id)
          :ok

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp call(msg), do: GenServer.call(__MODULE__, msg, @call_timeout)

  # Leitura direta no ETS. Se a tabela ainda não existe (ex.: servidor reiniciando),
  # devolve falha temporária para que Retry tente novamente.
  defp read(id) do
    case :ets.lookup(@table, id) do
      [{^id, _status, loan}] -> {:ok, loan}
      [] -> {:error, :not_found}
    end
  rescue
    ArgumentError -> {:error, :transient}
  end

  defp build_loan(attrs, id) do
    book_id = Map.get(attrs, :book_id)
    borrower = Map.get(attrs, :borrower)

    if is_nil(book_id) or is_nil(borrower) do
      {:error, :invalid}
    else
      now = DateTime.utc_now()

      {:ok,
       %Loan{
         id: id,
         book_id: book_id,
         borrower: borrower,
         borrowed_at: now,
         due_at: DateTime.add(now, @loan_days * 86_400, :second)
       }}
    end
  end

  defp apply_changes(loan, changes) do
    updated = struct(loan, Map.take(changes, [:borrower, :due_at, :status]))

    cond do
      updated.status not in [:active, :returned] ->
        {:error, :invalid_status}

      updated.status == :returned and is_nil(loan.returned_at) ->
        {:ok, %{updated | returned_at: DateTime.utc_now()}}

      true ->
        {:ok, updated}
    end
  end

  defp parse_line(line) do
    case line |> String.trim() |> String.split(",", parts: 2) do
      [book, borrower] when book != "" and borrower != "" ->
        {:ok, %{book_id: String.trim(book), borrower: String.trim(borrower)}}

      _ ->
        {:error, :malformed}
    end
  end
end
