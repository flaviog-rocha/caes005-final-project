# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Garantir a consistência dos dados durante operações concorrentes.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DynamicSupervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# notificacoes.exs
#
# Sistema de gerenciamento de notificações em Elixir (biblioteca padrão).
#
# Requisitos atendidos:
#   1. Limite de operações simultâneas: Notifications.Limiter (semáforo com fila de espera).
#   2. Consistência em concorrência: Notifications.Store serializa todas as mutações
#      e usa transições de estado com verificação (claim/finish), evitando envio duplicado.
#   3. Persistência: estado salvo em disco de forma atômica (escrita em arquivo temporário,
#      fsync e rename) e recarregado na inicialização, com recuperação de envios interrompidos.
#
# Recurso exigido: DynamicSupervisor, usado para supervisionar um worker temporário por envio.
#
# Execução: elixir notificacoes.exs

defmodule Notifications.Store do
  @moduledoc """
  Fonte única de verdade do sistema. Todas as mutações passam por este processo,
  o que serializa escritas concorrentes. Cada alteração é persistida antes de
  ser confirmada ao chamador.
  """
  use GenServer

  # --- API pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :path), name: __MODULE__)
  end

  def create(recipient, message) do
    GenServer.call(__MODULE__, {:create, recipient, message})
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def list, do: GenServer.call(__MODULE__, :list)

  # Transição atômica pending -> sending. Garante que apenas um worker envie a notificação.
  def claim(id), do: GenServer.call(__MODULE__, {:claim, id})

  # Transição sending -> sent | failed.
  def finish(id, status) when status in [:sent, :failed] do
    GenServer.call(__MODULE__, {:finish, id, status})
  end

  # --- Callbacks ---

  @impl true
  def init(path) do
    File.mkdir_p!(Path.dirname(path))
    state = load(path)

    # Recuperação: envios que estavam em andamento na queda voltam a ser pendentes.
    items =
      Map.new(state.items, fn
        {id, %{status: :sending} = n} -> {id, %{n | status: :pending}}
        other -> other
      end)

    state = %{state | items: items}
    :ok = persist(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:create, recipient, message}, _from, s) do
    id = s.next_id
    n = %{id: id, recipient: recipient, message: message, status: :pending}
    new = %{s | next_id: id + 1, items: Map.put(s.items, id, n)}
    commit(s, new, {:ok, n})
  end

  def handle_call({:get, id}, _from, s) do
    {:reply, fetch(s, id), s}
  end

  def handle_call(:list, _from, s) do
    list = s.items |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, list, s}
  end

  def handle_call({:claim, id}, _from, s) do
    case fetch(s, id) do
      {:ok, %{status: :pending} = n} ->
        new_n = %{n | status: :sending}
        commit(s, %{s | items: Map.put(s.items, id, new_n)}, {:ok, new_n})

      {:ok, _} ->
        {:reply, {:error, :already_claimed}, s}

      error ->
        {:reply, error, s}
    end
  end

  def handle_call({:finish, id, status}, _from, s) do
    case fetch(s, id) do
      {:ok, %{status: :sending} = n} ->
        new_n = %{n | status: status}
        commit(s, %{s | items: Map.put(s.items, id, new_n)}, {:ok, new_n})

      {:ok, _} ->
        {:reply, {:error, :invalid_transition}, s}

      error ->
        {:reply, error, s}
    end
  end

  # --- Helpers privados ---

  # Confirma a mudança somente se a persistência for bem-sucedida.
  defp commit(old, new, reply) do
    case persist(new) do
      :ok -> {:reply, reply, new}
      {:error, reason} -> {:reply, {:error, {:persistence_failed, reason}}, old}
    end
  end

  defp fetch(s, id) do
    case Map.fetch(s.items, id) do
      {:ok, n} -> {:ok, n}
      :error -> {:error, :not_found}
    end
  end

  defp load(path) do
    case File.read(path) do
      {:ok, bin} ->
        %{next_id: next_id, items: items} = :erlang.binary_to_term(bin, [:safe])
        %{path: path, next_id: next_id, items: items}

      {:error, :enoent} ->
        %{path: path, next_id: 1, items: %{}}
    end
  end

  # Escrita atômica: grava em arquivo temporário, força gravação em disco (fsync)
  # e só então renomeia para o arquivo definitivo. Uma queda nunca deixa arquivo parcial.
  defp persist(%{path: path} = s) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(%{next_id: s.next_id, items: s.items})

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- IO.binwrite(io, bin),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end

defmodule Notifications.Limiter do
  @moduledoc """
  Semáforo que limita o número de operações simultâneas.
  Chamadas excedentes aguardam em uma fila FIFO. Se um processo que possui
  ou aguarda uma vaga morrer, a vaga é liberada automaticamente via monitor.
  """
  use GenServer

  # --- API pública ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max, 4), name: __MODULE__)
  end

  @doc "Executa `fun` ocupando uma vaga do semáforo."
  def run(fun) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      :ok = GenServer.call(__MODULE__, :release)
    end
  end

  def stats, do: GenServer.call(__MODULE__, :stats)

  # --- Callbacks ---

  @impl true
  def init(max) do
    {:ok, %{max: max, monitors: %{}, active: MapSet.new(), waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _} = from, s) do
    ref = Process.monitor(pid)

    s = %{
      s
      | monitors: Map.put(s.monitors, pid, ref),
        waiting: :queue.in(from, s.waiting)
    }

    {:noreply, grant(s)}
  end

  def handle_call(:release, {pid, _}, s) do
    {:reply, :ok, s |> drop(pid) |> grant()}
  end

  def handle_call(:stats, _from, s) do
    stats = %{
      max: s.max,
      active: MapSet.size(s.active),
      waiting: :queue.len(s.waiting)
    }

    {:reply, stats, s}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, s) do
    {:noreply, s |> drop(pid) |> grant()}
  end

  # --- Helpers privados ---

  # Concede vagas enquanto houver capacidade e processos aguardando.
  defp grant(s) do
    if MapSet.size(s.active) < s.max and not :queue.is_empty(s.waiting) do
      {{:value, {pid, _} = from}, rest} = :queue.out(s.waiting)
      GenServer.reply(from, :ok)
      grant(%{s | waiting: rest, active: MapSet.put(s.active, pid)})
    else
      s
    end
  end

  # Remove o processo de todas as estruturas e cancela o monitor.
  defp drop(s, pid) do
    case Map.pop(s.monitors, pid) do
      {nil, _} ->
        s

      {ref, monitors} ->
        Process.demonitor(ref, [:flush])

        %{
          s
          | monitors: monitors,
            active: MapSet.delete(s.active, pid),
            waiting: :queue.filter(fn {p, _} -> p != pid end, s.waiting)
        }
    end
  end
end

defmodule Notifications.Worker do
  @moduledoc """
  Worker temporário que realiza o envio de uma notificação.
  Roda sob o DynamicSupervisor e não é reiniciado automaticamente:
  o estado durável fica no Store, e a recuperação é feita por `Notifications.recover/0`.
  """
  use GenServer, restart: :temporary

  alias Notifications.{Limiter, Store}

  def start_link(id), do: GenServer.start_link(__MODULE__, id)

  @impl true
  def init(id), do: {:ok, id, {:continue, :deliver}}

  @impl true
  def handle_continue(:deliver, id) do
    # Qualquer falha (exceção ou saída de processo) marca a notificação como :failed,
    # evitando que ela fique presa no estado :sending.
    status =
      try do
        Limiter.run(fn -> deliver(id) end)
      catch
        _kind, _reason -> :failed
      end

    Store.finish(id, status)
    {:stop, :normal, id}
  end

  # Simula o envio por um canal externo.
  defp deliver(id) do
    IO.puts("  [worker #{inspect(self())}] enviando notificação #{id}")
    Process.sleep(300)
    if :rand.uniform(10) == 1, do: :failed, else: :sent
  end
end

defmodule Notifications do
  @moduledoc """
  API pública do sistema e árvore de supervisão.

  Árvore:
    Notifications.Supervisor (one_for_one)
    ├── Notifications.Store      (estado persistido)
    ├── Notifications.Limiter    (limite de concorrência)
    └── Notifications.WorkerSup  (DynamicSupervisor de workers de envio)
  """
  alias Notifications.{Store, Worker}

  @worker_sup Notifications.WorkerSup

  @doc """
  Inicia o sistema. Opções:
    * `:path` - caminho do arquivo de persistência (obrigatório)
    * `:max`  - máximo de operações simultâneas (padrão: 4)
  """
  def start_link(opts) do
    children = [
      {Store, opts},
      {Notifications.Limiter, opts},
      {DynamicSupervisor, name: @worker_sup, strategy: :one_for_one}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Notifications.Supervisor)
  end

  @doc "Cria uma notificação no estado :pending (ainda não enviada)."
  def create(recipient, message), do: Store.create(recipient, message)

  def list, do: Store.list()

  @doc """
  Inicia o envio de uma notificação pendente. A transição pending -> sending
  é atômica no Store, então duas chamadas concorrentes não geram envio duplicado.
  """
  def dispatch(id) do
    case Store.claim(id) do
      {:ok, _n} ->
        case DynamicSupervisor.start_child(@worker_sup, {Worker, id}) do
          {:ok, pid} ->
            {:ok, pid}

          {:error, reason} ->
            Store.finish(id, :failed)
            {:error, reason}
        end

      error ->
        error
    end
  end

  @doc "Reenvia as notificações que ficaram pendentes (ex.: após reinicialização)."
  def recover do
    list()
    |> Enum.filter(&(&1.status == :pending))
    |> Enum.map(&dispatch(&1.id))
  end
end

defmodule Notifications.Demo do
  @moduledoc false
  alias Notifications.{Limiter, Store}

  def run do
    path = Path.join(System.tmp_dir!(), "notificacoes_demo.bin")
    File.rm(path)
    opts = [path: path, max: 3]

    IO.puts("== Fase 1: criação e envio concorrente (limite de 3 simultâneos) ==")
    {:ok, sup} = Notifications.start_link(opts)

    ids =
      for i <- 1..10 do
        {:ok, n} = Notifications.create("usuario#{i}@exemplo.com", "Mensagem #{i}")
        n.id
      end

    # Dispara todos ao mesmo tempo; o Limiter controla a execução real.
    ids
    |> Task.async_stream(&Notifications.dispatch/1, max_concurrency: 10, timeout: :infinity)
    |> Stream.run()

    IO.inspect(Limiter.stats(), label: "limiter (durante envio)")
    wait_settled()
    IO.inspect(Enum.frequencies_by(Notifications.list(), & &1.status), label: "status finais")

    IO.puts("\n== Fase 2: notificações pendentes sobrevivem a reinicialização ==")
    {:ok, _} = Notifications.create("admin@exemplo.com", "Pendente antes da queda")
    {:ok, _} = Notifications.create("suporte@exemplo.com", "Também pendente")

    Supervisor.stop(sup)
    IO.puts("Sistema parado.")

    {:ok, sup2} = Notifications.start_link(opts)
    IO.inspect(Enum.frequencies_by(Notifications.list(), & &1.status), label: "após reinício")

    Notifications.recover()
    wait_settled()
    IO.inspect(Enum.frequencies_by(Notifications.list(), & &1.status), label: "após recuperação")

    Supervisor.stop(sup2)
    File.rm(path)
    :ok
  end

  defp wait_settled do
    if Enum.any?(Store.list(), &(&1.status in [:pending, :sending])) do
      Process.sleep(100)
      wait_settled()
    else
      :ok
    end
  end
end

Notifications.Demo.run()
