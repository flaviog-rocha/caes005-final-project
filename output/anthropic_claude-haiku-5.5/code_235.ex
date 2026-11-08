# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir cadastrar, consultar, atualizar e remover registros.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 4. Permitir a execução de operações simultâneas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# arquivo: biblioteca_emprestimos.exs
# Execução: elixir biblioteca_emprestimos.exs (ou carregar com Code.require_file/1)

defmodule Biblioteca.Retry do
  @moduledoc """
  Novas tentativas com backoff exponencial e jitter para falhas temporárias.

  Considera temporárias:
    * `{:error, :busy}` - sistema sobrecarregado (backpressure);
    * saídas (`exit`) de chamadas a processos - timeout ou processo indisponível.
  """

  @defaults [max_attempts: 5, base_delay: 50, max_delay: 2_000]

  @spec run((-> term()), keyword()) :: term()
  def run(fun, opts \\ []) when is_function(fun, 0) do
    opts = Keyword.merge(@defaults, opts)
    attempt(fun, 1, opts)
  end

  defp attempt(fun, n, opts) do
    result =
      try do
        fun.()
      catch
        # Converte saídas de processo em erro temporário para permitir nova tentativa.
        :exit, reason -> {:error, {:exit, reason}}
      end

    if temporary?(result) and n < opts[:max_attempts] do
      Process.sleep(backoff(n, opts))
      attempt(fun, n + 1, opts)
    else
      result
    end
  end

  defp temporary?({:error, :busy}), do: true
  defp temporary?({:error, {:exit, _}}), do: true
  defp temporary?(_), do: false

  # Backoff exponencial limitado com jitter para evitar tempestades de requisições.
  defp backoff(n, opts) do
    exp = opts[:base_delay] * Integer.pow(2, n - 1)
    capped = min(exp, opts[:max_delay])
    half = div(capped, 2)
    half + :rand.uniform(half + 1) - 1
  end
end

defmodule Biblioteca.Emprestimos do
  @moduledoc """
  Gerenciamento de empréstimos de livros.

  Arquitetura:
    * Um GenServer é o único dono da tabela ETS. Escritas são serializadas
      por ele, garantindo consistência em atualizações e remoções.
    * Leituras acessam a ETS diretamente (`read_concurrency`), sem passar
      pelo processo, permitindo consultas simultâneas.
    * `stream/1` percorre a ETS em blocos via `:ets.select/3` com continuação,
      sem materializar todos os registros na memória.
    * Importação e exportação CSV são feitas com streams preguiçosos,
      processando lotes em paralelo com `Task.async_stream/3`.
    * Falhas temporárias são repetidas por `Biblioteca.Retry`.

  Observação: a criação não é idempotente. Se uma chamada expirar após o
  registro ter sido gravado, a nova tentativa pode criar um duplicado.
  """

  use GenServer

  alias Biblioteca.Retry

  @table __MODULE__
  @max_queue 10_000
  @call_timeout 5_000
  @csv_header "book_id,member_id,loaned_at,due_at"

  # ---------------------------------------------------------------------------
  # Ciclo de vida
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ---------------------------------------------------------------------------
  # CRUD
  # ---------------------------------------------------------------------------

  @doc "Cadastra um empréstimo. Atributos: :book_id, :member_id, :due_at e opcionalmente :loaned_at."
  def create(attrs) when is_map(attrs), do: call({:create, attrs})

  @doc "Atualiza :due_at e/ou :returned_at de um empréstimo existente."
  def update(id, changes) when is_binary(id) and is_map(changes),
    do: call({:update, id, changes})

  @doc "Remove um empréstimo."
  def delete(id) when is_binary(id), do: call({:delete, id})

  @doc "Consulta um empréstimo pelo id. Leitura direta na ETS, sem serialização."
  def get(id) when is_binary(id) do
    case :ets.lookup(@table, id) do
      [{^id, loan}] -> {:ok, loan}
      [] -> {:error, :not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Processamento de grandes volumes
  # ---------------------------------------------------------------------------

  @doc """
  Retorna um stream preguiçoso de empréstimos, lido em blocos de `chunk_size`.
  A leitura é feita por continuação da ETS, então a memória usada é limitada
  ao tamanho do bloco. Modificações concorrentes podem ou não aparecer no
  percurso, mas nunca quebram a iteração.
  """
  def stream(chunk_size \\ 500) when is_integer(chunk_size) and chunk_size > 0 do
    match_spec = [{{:_, :"$1"}, [], [:"$1"]}]

    Stream.resource(
      fn -> :start end,
      fn
        :start -> fetch(:ets.select(@table, match_spec, chunk_size))
        {:cont, cont} -> fetch(:ets.select(cont))
      end,
      fn _ -> :ok end
    )
  end

  defp fetch(:"$end_of_table"), do: {:halt, :done}
  defp fetch({rows, cont}), do: {rows, {:cont, cont}}

  @doc """
  Importa empréstimos de um CSV (`book_id,member_id,loaned_at,due_at`, datas ISO8601).
  Lê o arquivo em linhas, agrupa em lotes e processa os lotes concorrentemente.
  Retorna `%{ok: n, error: m}`.
  """
  def import_csv(path, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 500)
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 in ["", @csv_header]))
    |> Stream.map(&parse_line/1)
    |> Stream.chunk_every(batch_size)
    |> Task.async_stream(&process_batch/1,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(%{ok: 0, error: 0}, fn {:ok, counts}, acc ->
      %{ok: acc.ok + counts.ok, error: acc.error + counts.error}
    end)
  end

  @doc "Exporta todos os empréstimos para CSV, escrevendo linha a linha sem carregar tudo na memória."
  def export_csv(path, chunk_size \\ 1_000) do
    stream(chunk_size)
    |> Stream.map(&to_csv_line/1)
    |> Stream.into(File.stream!(path))
    |> Stream.run()
  end

  @doc """
  Executa operações heterogêneas em paralelo. Cada operação é uma tupla:
  `{:create, attrs}`, `{:update, id, changes}` ou `{:delete, id}`.
  Retorna os resultados na mesma ordem das operações.
  """
  def run_concurrently(ops, max_concurrency \\ System.schedulers_online() * 2) do
    ops
    |> Task.async_stream(&dispatch/1,
      max_concurrency: max_concurrency,
      timeout: :infinity,
      ordered: true
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    :ets.new(@table, [:set, :protected, :named_table, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, attrs}, _from, state) do
    reply =
      with :ok <- check_capacity(),
           {:ok, loan} <- build(attrs) do
        :ets.insert(@table, {loan.id, loan})
        {:ok, loan}
      end

    {:reply, reply, state}
  end

  def handle_call({:update, id, changes}, _from, state) do
    reply =
      with :ok <- check_capacity(),
           {:ok, loan} <- fetch_loan(id),
           {:ok, updated} <- apply_changes(loan, changes) do
        :ets.insert(@table, {id, updated})
        {:ok, updated}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, id}, _from, state) do
    reply =
      with :ok <- check_capacity(),
           {:ok, _loan} <- fetch_loan(id) do
        :ets.delete(@table, id)
        :ok
      end

    {:reply, reply, state}
  end

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  defp call(msg) do
    Retry.run(fn -> GenServer.call(__MODULE__, msg, @call_timeout) end)
  end

  # Backpressure: rejeita com erro temporário quando a fila está muito grande.
  defp check_capacity do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, n} when n > @max_queue -> {:error, :busy}
      _ -> :ok
    end
  end

  defp fetch_loan(id) do
    case :ets.lookup(@table, id) do
      [{^id, loan}] -> {:ok, loan}
      [] -> {:error, :not_found}
    end
  end

  defp build(attrs) do
    loan = %{
      id: new_id(),
      book_id: Map.get(attrs, :book_id),
      member_id: Map.get(attrs, :member_id),
      loaned_at: Map.get(attrs, :loaned_at, Date.utc_today()),
      due_at: Map.get(attrs, :due_at),
      returned_at: nil,
      status: :active
    }

    validate(loan)
  end

  defp apply_changes(loan, changes) do
    merged = Map.merge(loan, Map.take(changes, [:due_at, :returned_at]))
    validate(merged)
  end

  defp validate(%{book_id: b, member_id: m, loaned_at: %Date{} = l, due_at: %Date{} = d} = loan)
       when is_binary(b) and is_binary(m) do
    if Date.compare(d, l) == :lt do
      {:error, :invalid_dates}
    else
      status = if is_nil(loan.returned_at), do: :active, else: :returned
      {:ok, Map.put(loan, :status, status)}
    end
  end

  defp validate(_), do: {:error, :invalid_attrs}

  defp new_id do
    "#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive])}"
  end

  defp dispatch({:create, attrs}), do: create(attrs)
  defp dispatch({:update, id, changes}), do: update(id, changes)
  defp dispatch({:delete, id}), do: delete(id)

  defp process_batch(items) do
    Enum.reduce(items, %{ok: 0, error: 0}, fn
      {:ok, attrs}, acc ->
        case create(attrs) do
          {:ok, _} -> Map.update!(acc, :ok, &(&1 + 1))
          _ -> Map.update!(acc, :error, &(&1 + 1))
        end

      {:error, _line}, acc ->
        Map.update!(acc, :error, &(&1 + 1))
    end)
  end

  defp parse_line(line) do
    case line |> String.split(",") |> Enum.map(&String.trim/1) do
      [book, member, loaned, due] ->
        with {:ok, l} <- Date.from_iso8601(loaned),
             {:ok, d} <- Date.from_iso8601(due) do
          {:ok, %{book_id: book, member_id: member, loaned_at: l, due_at: d}}
        else
          _ -> {:error, line}
        end

      _ ->
        {:error, line}
    end
  end

  defp to_csv_line(loan) do
    [
      loan.id,
      loan.book_id,
      loan.member_id,
      loan.loaned_at,
      loan.due_at,
      loan.returned_at || "",
      loan.status
    ]
    |> Enum.map(&to_string/1)
    |> Enum.join(",")
    |> Kernel.<>("\n")
  end

  # ---------------------------------------------------------------------------
  # Dados de exemplo para testes manuais (não executado automaticamente)
  # ---------------------------------------------------------------------------

  @doc false
  def exemplo do
    {:ok, _} = start_link()

    ops =
      for i <- 1..100 do
        {:create,
         %{
           book_id: "livro-#{i}",
           member_id: "membro-#{rem(i, 10)}",
           due_at: Date.add(Date.utc_today(), 14)
         }}
      end

    resultados = run_concurrently(ops)
    IO.inspect(Enum.count(resultados, &match?({:ok, _}, &1)), label: "criados")

    stream(50)
    |> Stream.take(5)
    |> Enum.each(&IO.inspect(&1.id, label: "amostra"))
  end
end
