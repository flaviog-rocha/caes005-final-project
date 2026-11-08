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

# Gerenciamento de empréstimos de livros com OTP (arquivo único, autocontido).
# Execute com: elixir loans.exs

defmodule Loans.Notifier do
  # Recebe eventos de notificação e os exibe.
  # Usa cast para não bloquear os processos que geram eventos.
  use GenServer

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  def notify(event), do: GenServer.cast(__MODULE__, {:notify, event})

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_cast({:notify, event}, state) do
    IO.puts("[NOTIFICACAO] " <> format(event))
    {:noreply, state}
  end

  defp format({:borrowed, id, book, who}),
    do: "Empréstimo ##{id} de \"#{book}\" para #{who} registrado."

  defp format({:returned, id, book}),
    do: "Empréstimo ##{id} de \"#{book}\" devolvido."

  defp format({:due_soon, id, book, who}),
    do: "Empréstimo ##{id} de \"#{book}\" (#{who}) vence em breve."

  defp format({:overdue, id, book, who}),
    do: "Empréstimo ##{id} de \"#{book}\" (#{who}) está ATRASADO."

  defp format({:process_down, name, reason}),
    do: "Processo #{inspect(name)} caiu com motivo #{inspect(reason)}."
end

defmodule Loans.Store do
  # Mantém o estado dos empréstimos e as operações já aplicadas (idempotência).
  # Persiste o estado em disco a cada alteração (escrita atômica via rename)
  # e o recarrega na inicialização.
  use GenServer

  @default_path "loans_state.bin"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # Empréstimo: a chave de idempotência impede a execução repetida da mesma operação.
  def borrow(key, book, borrower, days),
    do: GenServer.call(__MODULE__, {:borrow, key, book, borrower, days})

  def return_book(key, loan_id),
    do: GenServer.call(__MODULE__, {:return, key, loan_id})

  def list, do: GenServer.call(__MODULE__, :list)

  # Avalia as condições (vencimento próximo / atraso) e gera notificações não enviadas antes.
  def check_conditions(today \\ Date.utc_today()),
    do: GenServer.call(__MODULE__, {:check, today})

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path, @default_path)
    {:ok, %{path: path, data: load(path)}}
  end

  @impl true
  def handle_call({:borrow, key, book, borrower, days}, _from, s) do
    case ensure_new(s, key) do
      {:error, _} = err ->
        {:reply, err, s}

      :ok ->
        id = s.data.next_id

        loan = %{
          id: id,
          book: book,
          borrower: borrower,
          due: Date.add(Date.utc_today(), days),
          status: :active,
          notified: MapSet.new()
        }

        data = %{
          s.data
          | loans: Map.put(s.data.loans, id, loan),
            next_id: id + 1,
            applied: MapSet.put(s.data.applied, key)
        }

        s = commit!(s, data)
        Loans.Notifier.notify({:borrowed, id, book, borrower})
        {:reply, {:ok, id}, s}
    end
  end

  def handle_call({:return, key, id}, _from, s) do
    with :ok <- ensure_new(s, key),
         {:ok, loan} <- fetch_active(s, id) do
      data = %{
        s.data
        | loans: Map.put(s.data.loans, id, %{loan | status: :returned}),
          applied: MapSet.put(s.data.applied, key)
      }

      s = commit!(s, data)
      Loans.Notifier.notify({:returned, id, loan.book})
      {:reply, :ok, s}
    else
      {:error, _} = err -> {:reply, err, s}
    end
  end

  def handle_call(:list, _from, s) do
    {:reply, Map.values(s.data.loans), s}
  end

  def handle_call({:check, today}, _from, s) do
    {events, loans} =
      Enum.reduce(s.data.loans, {[], s.data.loans}, fn {id, loan}, {evs, acc} ->
        with kind when not is_nil(kind) <- condition(loan, today),
             false <- MapSet.member?(loan.notified, kind) do
          updated = %{loan | notified: MapSet.put(loan.notified, kind)}
          {[{kind, id, loan.book, loan.borrower} | evs], Map.put(acc, id, updated)}
        else
          _ -> {evs, acc}
        end
      end)

    if events == [] do
      {:reply, [], s}
    else
      s = commit!(s, %{s.data | loans: loans})
      Enum.each(events, &Loans.Notifier.notify/1)
      {:reply, events, s}
    end
  end

  # Condições de notificação
  defp condition(%{status: :active, due: due}, today) do
    case Date.diff(due, today) do
      d when d < 0 -> :overdue
      d when d <= 1 -> :due_soon
      _ -> nil
    end
  end

  defp condition(_loan, _today), do: nil

  defp ensure_new(%{data: %{applied: applied}}, key) do
    if MapSet.member?(applied, key), do: {:error, :duplicate}, else: :ok
  end

  defp fetch_active(%{data: %{loans: loans}}, id) do
    case Map.fetch(loans, id) do
      {:ok, %{status: :active} = loan} -> {:ok, loan}
      _ -> {:error, :not_found}
    end
  end

  defp commit!(s, data) do
    s = %{s | data: data}
    persist!(s)
    s
  end

  # Escrita atômica: grava em arquivo temporário e renomeia.
  defp persist!(%{path: path, data: data}) do
    tmp = path <> ".tmp"
    File.write!(tmp, :erlang.term_to_binary(data))
    File.rename!(tmp, path)
  end

  defp load(path) do
    with {:ok, bin} <- File.read(path),
         data when is_map(data) <- safe_decode(bin) do
      data
    else
      _ -> %{loans: %{}, next_id: 1, applied: MapSet.new()}
    end
  end

  defp safe_decode(bin) do
    :erlang.binary_to_term(bin, [:safe])
  rescue
    ArgumentError -> nil
  end
end

defmodule Loans.Scheduler do
  # Executa periodicamente a verificação de condições (vencimento e atraso).
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, 60_000)
    schedule(interval)
    {:ok, %{interval: interval}}
  end

  @impl true
  def handle_info(:tick, state) do
    try do
      Loans.Store.check_conditions()
    catch
      :exit, reason -> IO.puts("[SCHEDULER] Store indisponível: #{inspect(reason)}")
    end

    schedule(state.interval)
    {:noreply, state}
  end

  defp schedule(interval), do: Process.send_after(self(), :tick, interval)
end

defmodule Loans.Watchdog do
  # Monitora os processos críticos e detecta encerramentos inesperados.
  # O Supervisor reinicia os processos; o Watchdog os monitora novamente após o restart.
  use GenServer

  @monitored [Loans.Store, Loans.Notifier, Loans.Scheduler]

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    Enum.each(@monitored, &send(self(), {:monitor, &1}))
    {:ok, %{refs: %{}}}
  end

  @impl true
  def handle_info({:monitor, name}, state) do
    case Process.whereis(name) do
      nil ->
        # Processo ainda não reiniciado pelo Supervisor: tenta novamente.
        Process.send_after(self(), {:monitor, name}, 100)
        {:noreply, state}

      pid ->
        ref = Process.monitor(pid)
        {:noreply, %{state | refs: Map.put(state.refs, ref, name)}}
    end
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    {name, refs} = Map.pop(state.refs, ref)

    unless expected?(reason) do
      IO.puts("[WATCHDOG] #{inspect(name)} (#{inspect(pid)}) encerrou inesperadamente: #{inspect(reason)}")
      Loans.Notifier.notify({:process_down, name, reason})
    end

    if name, do: Process.send_after(self(), {:monitor, name}, 100)
    {:noreply, %{state | refs: refs}}
  end

  defp expected?(:normal), do: true
  defp expected?(:shutdown), do: true
  defp expected?({:shutdown, _}), do: true
  defp expected?(_), do: false
end

defmodule Loans.Supervisor do
  # Árvore de supervisão: reinicia processos que falham (one_for_one).
  use Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    children = [
      {Loans.Notifier, []},
      {Loans.Store, Keyword.take(opts, [:path])},
      {Loans.Scheduler, Keyword.take(opts, [:interval])},
      {Loans.Watchdog, []}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Loans.Demo do
  # Demonstração dos requisitos: notificações, idempotência, periodicidade,
  # persistência e recuperação de falhas.
  def run do
    {:ok, _sup} = Loans.Supervisor.start_link(path: "loans_state.bin", interval: 1_000)

    suffix = System.unique_integer([:positive])

    {:ok, id1} = Loans.Store.borrow("borrow-#{suffix}", "Dom Casmurro", "Ana", 14)

    # Mesma chave: deve ser rejeitada (idempotência)
    IO.inspect(Loans.Store.borrow("borrow-#{suffix}", "Dom Casmurro", "Ana", 14), label: "repetida")

    # Empréstimo já vencido: notificação de atraso pelo agendador
    {:ok, _id2} = Loans.Store.borrow("late-#{suffix}", "Iracema", "Bruno", -2)

    Process.sleep(1_500)

    IO.inspect(Loans.Store.return_book("return-#{suffix}", id1), label: "devolucao")
    IO.inspect(Loans.Store.return_book("return-#{suffix}", id1), label: "devolucao repetida")

    # Simula encerramento inesperado do Store: Watchdog notifica e Supervisor reinicia
    Process.exit(Process.whereis(Loans.Store), :kill)
    Process.sleep(500)

    # Estado recuperado do disco após o restart
    IO.inspect(Loans.Store.list(), label: "apos restart")
    Process.sleep(200)
  end
end

Loans.Demo.run()
