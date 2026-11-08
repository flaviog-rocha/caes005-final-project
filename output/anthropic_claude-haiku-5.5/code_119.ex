# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Permitir comunicação entre diferentes processos.
# 3. Permitir limitar a quantidade de operações simultâneas.
# 4. Implementar novas tentativas após falhas temporárias.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Stream
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de empréstimos de livros com processos por entidade,
# comunicação por mensagens, limite de concorrência e novas tentativas.
# Executar: elixir emprestimos.exs

defmodule LoanManager do
  @moduledoc """
  Gerenciamento de empréstimos de livros.

  * Cada livro (entidade) é um processo GenServer independente, localizado via Registry
    e criado sob demanda por um DynamicSupervisor.
  * Operações sobre o mesmo livro são serializadas pelo seu processo; livros diferentes
    são processados em paralelo.
  * A comunicação entre processos ocorre por chamadas GenServer e por notificações
    (mensagens enviadas aos assinantes de cada livro).
  * A concorrência global é limitada pela opção `max_concurrency` do `Task.async_stream/3`.
  * Falhas temporárias são repetidas com backoff exponencial e jitter.
  """

  @supervisor LoanManager.BookSupervisor

  # Sobe a árvore de supervisão: Registry (nomes por entidade) + DynamicSupervisor (livros)
  def start_link(opts \\ []) do
    children = [
      {Registry, keys: :unique, name: LoanManager.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: @supervisor}
    ]

    Supervisor.start_link(
      children,
      Keyword.merge([strategy: :one_for_one, name: LoanManager.Root], opts)
    )
  end

  # Garante que o processo da entidade (livro) exista
  def ensure_book(book_id, copies) do
    case DynamicSupervisor.start_child(@supervisor, {LoanManager.Book, {book_id, copies}}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  # Pipeline lazy de processamento de requisições: Stream + Task.async_stream com limite de concorrência
  def process_loans(requests, opts \\ []) do
    copies = Keyword.get(opts, :copies, 2)
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    failure_rate = Keyword.get(opts, :failure_rate, 0.0)
    retries = Keyword.get(opts, :retries, 3)
    base_delay = Keyword.get(opts, :base_delay, 50)

    requests
    |> Stream.with_index()
    |> Task.async_stream(
      fn {{book_id, member_id}, idx} ->
        :ok = ensure_book(book_id, copies)

        result =
          with_retry(
            fn -> LoanManager.Book.borrow(book_id, member_id, failure_rate) end,
            retries,
            base_delay
          )

        {idx, book_id, member_id, result}
      end,
      max_concurrency: max_concurrency,
      timeout: :infinity,
      ordered: true
    )
    |> Stream.map(fn {:ok, result} -> result end)
  end

  # Repete a função enquanto houver erro temporário, com backoff exponencial e jitter
  def with_retry(fun, max_attempts, base_delay, attempt \\ 1) do
    case fun.() do
      {:error, :temporary} when attempt < max_attempts ->
        Process.sleep(backoff(base_delay, attempt))
        with_retry(fun, max_attempts, base_delay, attempt + 1)

      result ->
        result
    end
  end

  defp backoff(base, attempt) do
    exponential = base * Integer.pow(2, attempt - 1)
    exponential + :rand.uniform(max(base, 1))
  end

  # Demonstração de uso
  def demo do
    {:ok, _} = start_link()

    copies = 5
    books = ["dune", "emma", "ulysses"]

    # Assina notificações de cada livro (comunicação entre processos)
    Enum.each(books, fn book ->
      :ok = ensure_book(book, copies)
      :ok = LoanManager.Book.subscribe(book, self())
    end)

    # Stream lazy de requisições: 20 membros pedindo cada livro
    requests =
      1..20
      |> Stream.flat_map(fn m -> Stream.map(books, &{&1, "membro_#{m}"}) end)

    results =
      requests
      |> process_loans(
        copies: copies,
        max_concurrency: 4,
        failure_rate: 0.3,
        retries: 4,
        base_delay: 20
      )
      |> Enum.to_list()

    summary = Enum.frequencies_by(results, fn {_idx, _book, _member, {status, _}} -> status end)
    events = collect_events([])

    IO.inspect(summary, label: "resumo dos empréstimos")
    IO.inspect(length(events), label: "notificações recebidas")

    summary
  end

  defp collect_events(acc) do
    receive do
      {:loan_event, _book, _event} = msg -> collect_events([msg | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end

defmodule LoanManager.Book do
  @moduledoc """
  Processo de uma entidade (livro). Serializa as operações sobre o livro,
  mantém o estoque, a lista de empréstimos e notifica os assinantes.
  """

  use GenServer

  def start_link({book_id, copies}) do
    GenServer.start_link(__MODULE__, {book_id, copies}, name: via(book_id))
  end

  def via(book_id), do: {:via, Registry, {LoanManager.Registry, book_id}}

  # Erros de processo (ex.: reinício do livro) são tratados como falhas temporárias
  def borrow(book_id, member_id, failure_rate) do
    GenServer.call(via(book_id), {:borrow, member_id, failure_rate})
  catch
    :exit, _reason -> {:error, :temporary}
  end

  def subscribe(book_id, pid) do
    GenServer.call(via(book_id), {:subscribe, pid})
  end

  @impl true
  def init({book_id, copies}) do
    {:ok, %{id: book_id, available: copies, loans: [], subscribers: []}}
  end

  @impl true
  def handle_call({:subscribe, pid}, _from, state) do
    {:reply, :ok, %{state | subscribers: [pid | state.subscribers]}}
  end

  def handle_call({:borrow, member_id, failure_rate}, _from, state) do
    cond do
      # Falha temporária simulada (ex.: indisponibilidade transitória de um recurso externo)
      :rand.uniform() < failure_rate ->
        {:reply, {:error, :temporary}, state}

      state.available > 0 ->
        loan = %{member: member_id, at: DateTime.utc_now()}

        new_state = %{
          state
          | available: state.available - 1,
            loans: [loan | state.loans]
        }

        notify(new_state, {:borrowed, member_id})
        {:reply, {:ok, loan}, new_state}

      true ->
        {:reply, {:error, :no_copies}, state}
    end
  end

  defp notify(%{id: id, subscribers: subscribers}, event) do
    Enum.each(subscribers, &send(&1, {:loan_event, id, event}))
  end
end

LoanManager.demo()
