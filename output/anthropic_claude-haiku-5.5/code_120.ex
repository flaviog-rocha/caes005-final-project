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

# Arquivo único: gerenciamento de empréstimos de livros em Elixir.
# Execute com: elixir emprestimos.exs

# Supervisor principal: registros, supervisor dinâmico, semáforo e eventos.
defmodule LoanSystem do
  use Supervisor

  # Inicia a árvore de supervisão. `max_concurrency` limita operações simultâneas globais.
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registro único: um processo por livro (entidade independente).
      {Registry, keys: :unique, name: LoanSystem.BookRegistry},
      # Registro duplicado: canal de comunicação (pub/sub) entre processos.
      {Registry, keys: :duplicate, name: LoanSystem.EventRegistry},
      # Supervisor dinâmico que cria um processo por livro sob demanda.
      {DynamicSupervisor, strategy: :one_for_one, name: LoanSystem.BookSupervisor},
      # Semáforo global que limita operações concorrentes.
      {LoanSystem.Semaphore, limit: Keyword.get(opts, :max_concurrency, 4)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # Processa requisições `{book_id, member_id}` de forma preguiçosa (Stream).
  # Cada requisição é executada em sua própria Task; livros diferentes são
  # processados em paralelo, enquanto o semáforo limita a concorrência global.
  def process(requests, opts \\ []) do
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    requests
    |> Task.async_stream(&process_one/1,
      max_concurrency: concurrency,
      ordered: false,
      timeout: :infinity
    )
    |> Stream.map(fn {:ok, result} -> result end)
  end

  defp process_one({book_id, member_id} = request) do
    result =
      LoanSystem.Semaphore.run(fn ->
        LoanSystem.Retry.run(
          fn -> LoanSystem.Book.loan(book_id, member_id) end,
          max_attempts: 4,
          base_delay: 20
        )
      end)

    {request, result}
  end
end

# Semáforo com fila de espera: limita quantas operações rodam ao mesmo tempo.
# Monitora os chamadores para liberar a vaga caso morram antes de liberar.
defmodule LoanSystem.Semaphore do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :limit), name: __MODULE__)
  end

  # Executa `fun` ocupando uma vaga do semáforo e a libera ao final.
  def run(fun) do
    {:ok, ref} = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, ref})
    end
  end

  @impl true
  def init(limit) do
    {:ok, %{limit: limit, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, state) do
    if map_size(state.active) < state.limit do
      {ref, state} = grant(pid, state)
      {:reply, {:ok, ref}, state}
    else
      # Sem vaga: o chamador fica na fila até ser atendido.
      {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
    end
  end

  @impl true
  def handle_cast({:release, ref}, state) do
    Process.demonitor(ref, [:flush])
    {:noreply, state |> drop(ref) |> admit()}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, state |> drop(ref) |> admit()}
  end

  defp grant(pid, state) do
    ref = Process.monitor(pid)
    {ref, %{state | active: Map.put(state.active, ref, pid)}}
  end

  defp drop(state, ref), do: %{state | active: Map.delete(state.active, ref)}

  # Concede vagas livres aos próximos chamadores da fila.
  defp admit(state) do
    with true <- map_size(state.active) < state.limit,
         {{:value, {pid, _} = from}, waiting} <- :queue.out(state.waiting) do
      {ref, state} = grant(pid, %{state | waiting: waiting})
      GenServer.reply(from, {:ok, ref})
      admit(state)
    else
      _ -> state
    end
  end
end

# Processo por livro: serializa as operações sobre a mesma entidade
# e publica eventos para os demais processos.
defmodule LoanSystem.Book do
  use GenServer, restart: :transient

  # Solicita o empréstimo de um livro para um membro.
  # Falhas de processo são convertidas em erro temporário para permitir nova tentativa.
  def loan(book_id, member_id) do
    ensure_started(book_id)
    GenServer.call(via(book_id), {:loan, member_id})
  catch
    :exit, _ -> {:error, :temporary_unavailable}
  end

  def start_link(book_id) do
    GenServer.start_link(__MODULE__, book_id, name: via(book_id))
  end

  @impl true
  def init(book_id) do
    {:ok, %{id: book_id, holder: nil}}
  end

  @impl true
  def handle_call({:loan, member_id}, _from, state) do
    cond do
      simulate_failure?() ->
        {:reply, {:error, :temporary_unavailable}, state}

      state.holder != nil ->
        {:reply, {:error, :already_loaned}, state}

      true ->
        LoanSystem.Events.publish({:loaned, state.id, member_id})
        {:reply, {:ok, :loaned}, %{state | holder: member_id}}
    end
  end

  defp ensure_started(book_id) do
    case DynamicSupervisor.start_child(LoanSystem.BookSupervisor, {__MODULE__, book_id}) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp via(book_id), do: {:via, Registry, {LoanSystem.BookRegistry, book_id}}

  # Simula falhas temporárias conforme a configuração da aplicação.
  defp simulate_failure? do
    :rand.uniform() < Application.get_env(:loan_system, :failure_rate, 0.0)
  end
end

# Nova tentativa com backoff exponencial e jitter, apenas para erros temporários.
defmodule LoanSystem.Retry do
  def run(fun, opts \\ []) do
    attempts = Keyword.get(opts, :max_attempts, 3)
    base = Keyword.get(opts, :base_delay, 50)
    do_run(fun, 1, attempts, base)
  end

  defp do_run(fun, attempt, max, base) do
    case fun.() do
      {:error, :temporary_unavailable} when attempt < max ->
        backoff = base * Integer.pow(2, attempt - 1)
        Process.sleep(backoff + :rand.uniform(base))
        do_run(fun, attempt + 1, max, base)

      result ->
        result
    end
  end
end

# Comunicação entre processos por eventos (pub/sub sobre Registry).
defmodule LoanSystem.Events do
  # Registra o processo chamador para receber eventos.
  def subscribe do
    Registry.register(LoanSystem.EventRegistry, :events, nil)
  end

  # Envia o evento para todos os processos inscritos.
  def publish(event) do
    Registry.dispatch(LoanSystem.EventRegistry, :events, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:loan_event, event})
    end)

    :ok
  end
end

# Demonstração
Application.put_env(:loan_system, :failure_rate, 0.3)
{:ok, _} = LoanSystem.start_link(max_concurrency: 3)
LoanSystem.Events.subscribe()

requests = Stream.map(1..20, fn i -> {rem(i, 6) + 1, "membro_#{i}"} end)

requests
|> LoanSystem.process(max_concurrency: 8)
|> Enum.each(&IO.inspect/1)

Stream.repeatedly(fn ->
  receive do
    {:loan_event, event} -> event
  after
    200 -> :fim
  end
end)
|> Stream.take_while(&(&1 != :fim))
|> Enum.each(&IO.inspect(&1, label: "evento"))
