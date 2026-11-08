# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de empréstimos de livros, atendendo somente aos seguintes requisitos:
#
# 1. Gerar notificações quando determinadas condições forem atingidas.
# 2. Impedir que uma mesma operação seja executada mais de uma vez.
# 3. Executar determinadas operações periodicamente.
# 4. Persistir os dados para recuperação após reinicializações.
# 5. Detectar encerramentos inesperados de processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de empréstimos de livros em Elixir.
#
# Uso (referência, em comentário):
#   {:ok, _sup} = Loans.start_link(scheduler: [interval: 5_000])
#   Loans.borrow("op-1", "ana", "livro-42", ~D[2024-01-10])  # => {:ok, 1}
#   Loans.borrow("op-1", "ana", "livro-42", ~D[2024-01-10])  # => {:duplicate, {:ok, 1}}
#   Loans.return_book("op-2", 1)                              # => {:ok, 1}
#   Loans.notifications()                                     # => lista de eventos

defmodule Loans do
  @moduledoc """
  Fachada do sistema de empréstimos. Inicia uma árvore de supervisão
  (`:one_for_one`) com Notifier, Store (persistido) e Scheduler (periódico).
  """

  alias Loans.{Notifier, Scheduler, Store}

  @doc """
  Inicia o supervisor. Opções:
    * `:store` - opções do Store (ex.: `[path: "/caminho/dados.bin"]`)
    * `:scheduler` - opções do Scheduler (ex.: `[interval: 60_000]`)
  """
  def start_link(opts \\ []) do
    children = [
      {Notifier, []},
      {Store, Keyword.get(opts, :store, [])},
      {Scheduler, Keyword.get(opts, :scheduler, [])}
    ]

    # Se um filho cair inesperadamente, o supervisor o reinicia.
    # Limite de 5 reinícios em 10 segundos antes de desistir.
    Supervisor.start_link(children,
      strategy: :one_for_one,
      max_restarts: 5,
      max_seconds: 10,
      name: :loans_supervisor
    )
  end

  defdelegate borrow(op_id, member, book_id, due_date), to: Store
  defdelegate return_book(op_id, loan_id), to: Store
  defdelegate loans(), to: Store
  defdelegate notifications(), to: Notifier, as: :history
end

defmodule Loans.Notifier do
  @moduledoc "Recebe e registra notificações de eventos do domínio."

  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :permanent}
  end

  # Cast não bloqueia o chamador e não falha se o processo estiver fora do ar.
  def notify(event), do: GenServer.cast(__MODULE__, {:notify, event})

  def history, do: GenServer.call(__MODULE__, :history)

  @impl true
  def init(_opts), do: {:ok, []}

  @impl true
  def handle_cast({:notify, event}, history) do
    Logger.warning("[notificacao] #{inspect(event)}")
    {:noreply, [event | history]}
  end

  @impl true
  def handle_call(:history, _from, history) do
    {:reply, Enum.reverse(history), history}
  end
end

defmodule Loans.Store do
  @moduledoc """
  Estado dos empréstimos, persistido em disco a cada escrita.
  Garante idempotência: cada `op_id` é executado no máximo uma vez;
  chamadas repetidas devolvem `{:duplicate, resultado_original}`.
  """

  use GenServer
  alias Loans.Notifier

  @max_loans_per_member 3

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :permanent}
  end

  def borrow(op_id, member, book_id, due_date) do
    GenServer.call(__MODULE__, {:borrow, op_id, member, book_id, due_date})
  end

  def return_book(op_id, loan_id) do
    GenServer.call(__MODULE__, {:return, op_id, loan_id})
  end

  def scan_overdue(today), do: GenServer.call(__MODULE__, {:scan_overdue, today})

  def loans, do: GenServer.call(__MODULE__, :loans)

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, Path.join(System.tmp_dir!(), "loans_store.bin"))
    File.mkdir_p!(Path.dirname(path))
    # Recuperação após reinicialização: carrega o último estado salvo.
    {:ok, %{path: path, data: load(path)}}
  end

  @impl true
  def handle_call({:borrow, op_id, member, book_id, due}, _from, state) do
    {reply, state} = run_once(state, op_id, &do_borrow(&1, member, book_id, due))
    {:reply, reply, state}
  end

  def handle_call({:return, op_id, loan_id}, _from, state) do
    {reply, state} = run_once(state, op_id, &do_return(&1, loan_id))
    {:reply, reply, state}
  end

  # Marca empréstimos vencidos e notifica apenas uma vez por empréstimo.
  def handle_call({:scan_overdue, today}, _from, state) do
    overdue =
      state.data.loans
      |> Map.values()
      |> Enum.filter(&overdue?(&1, today))

    loans =
      Enum.reduce(overdue, state.data.loans, fn loan, acc ->
        Notifier.notify({:overdue, loan.id, loan.member, loan.book_id})
        Map.put(acc, loan.id, %{loan | overdue_notified: true})
      end)

    state =
      if overdue == [] do
        state
      else
        persist(put_in_data(state, :loans, loans))
      end

    {:reply, length(overdue), state}
  end

  def handle_call(:loans, _from, state) do
    {:reply, Map.values(state.data.loans), state}
  end

  # Idempotência: se op_id já foi processado, devolve o resultado anterior.
  defp run_once(state, op_id, fun) do
    case Map.fetch(state.data.ops, op_id) do
      {:ok, previous} ->
        {{:duplicate, previous}, state}

      :error ->
        {reply, state} = fun.(state)
        ops = Map.put(state.data.ops, op_id, reply)
        state = persist(put_in_data(state, :ops, ops))
        {reply, state}
    end
  end

  defp do_borrow(state, member, book_id, due) do
    active = state.data.loans |> Map.values() |> Enum.filter(&(&1.status == :active))

    cond do
      Enum.count(active, &(&1.member == member)) >= @max_loans_per_member ->
        Notifier.notify({:limit_reached, member})
        {{:error, :limit_reached}, state}

      Enum.any?(active, &(&1.book_id == book_id)) ->
        Notifier.notify({:book_unavailable, book_id, member})
        {{:error, :unavailable}, state}

      true ->
        id = state.data.next_id

        loan = %{
          id: id,
          member: member,
          book_id: book_id,
          due: due,
          status: :active,
          overdue_notified: false
        }

        Notifier.notify({:loan_created, id, member, book_id})

        state =
          state
          |> put_in_data(:loans, Map.put(state.data.loans, id, loan))
          |> put_in_data(:next_id, id + 1)

        {{:ok, id}, state}
    end
  end

  defp do_return(state, loan_id) do
    case Map.fetch(state.data.loans, loan_id) do
      {:ok, %{status: :active} = loan} ->
        loans = Map.put(state.data.loans, loan_id, %{loan | status: :returned})
        Notifier.notify({:loan_returned, loan_id, loan.member})
        {{:ok, loan_id}, put_in_data(state, :loans, loans)}

      {:ok, _} ->
        {{:error, :already_returned}, state}

      :error ->
        {{:error, :not_found}, state}
    end
  end

  defp overdue?(%{status: :active, overdue_notified: false, due: due}, today),
    do: Date.compare(due, today) == :lt

  defp overdue?(_loan, _today), do: false

  defp put_in_data(state, key, value), do: %{state | data: Map.put(state.data, key, value)}

  defp default_data, do: %{loans: %{}, ops: %{}, next_id: 1}

  defp load(path) do
    with {:ok, bin} <- File.read(path),
         {:ok, data} <- safe_decode(bin) do
      data
    else
      _ -> default_data()
    end
  end

  defp safe_decode(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> :error
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp persist(%{path: path, data: data} = state) do
    tmp = path <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary(data))
    File.rename!(tmp, path)
    state
  end
end

defmodule Loans.Scheduler do
  @moduledoc """
  Executa tarefas periódicas (verificação de vencimentos) e monitora
  processos críticos, notificando encerramentos inesperados.
  """

  use GenServer
  require Logger
  alias Loans.{Notifier, Store}

  @default_interval 60_000
  @default_watch [Loans.Store, Loans.Notifier]

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :permanent}
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval)
    watched = Keyword.get(opts, :watch, @default_watch)

    schedule_tick(interval)

    state = %{interval: interval, watched: watched, monitors: %{}}
    {:ok, Enum.reduce(watched, state, &watch_name/2)}
  end

  @impl true
  def handle_info(:tick, state) do
    try do
      count = Store.scan_overdue(Date.utc_today())
      if count > 0, do: Logger.info("#{count} empréstimo(s) vencido(s) detectado(s)")
    catch
      :exit, reason -> Logger.error("Falha na verificação periódica: #{inspect(reason)}")
    end

    schedule_tick(state.interval)
    {:noreply, state}
  end

  def handle_info({:rewatch, name}, state) do
    {:noreply, watch_name(name, state)}
  end

  # Detecção de encerramento de processo monitorado.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {name, monitors} ->
        unless expected_exit?(reason) do
          Logger.error("Encerramento inesperado de #{inspect(name)}: #{inspect(reason)}")
          Notifier.notify({:unexpected_exit, name, reason})
        end

        # Supervisor reinicia o processo; aguardamos e o monitoramos novamente.
        Process.send_after(self(), {:rewatch, name}, 100)
        {:noreply, %{state | monitors: monitors}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp watch_name(name, state) do
    case Process.whereis(name) do
      nil ->
        Process.send_after(self(), {:rewatch, name}, 100)
        state

      pid ->
        ref = Process.monitor(pid)
        %{state | monitors: Map.put(state.monitors, ref, name)}
    end
  end

  defp expected_exit?(:normal), do: true
  defp expected_exit?(:shutdown), do: true
  defp expected_exit?({:shutdown, _}), do: true
  defp expected_exit?(_), do: false

  defp schedule_tick(interval), do: Process.send_after(self(), :tick, interval)
end
