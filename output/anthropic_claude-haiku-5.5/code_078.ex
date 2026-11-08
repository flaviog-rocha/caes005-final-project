# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Gerenciador de fila de tarefas em Elixir com Agent, persistência em disco,
# comunicação entre processos e recuperação automática após falhas.
#
# Uso (exemplo em comentário, para referência):
#
#   {:ok, _sup} = TaskQueue.start_link(path: "fila.dat", lease_ms: 10_000)
#   TaskQueue.subscribe()                 # recebe {:task_queue, :enqueued, id}
#   {:ok, id} = TaskQueue.enqueue(%{job: :enviar_email, to: "a@b.com"})
#   {:ok, id, task} = TaskQueue.dequeue() # tarefa fica "em voo" até ack/nack/lease
#   :ok = TaskQueue.ack(id)               # confirma processamento
#   TaskQueue.stats()
#
# Garantias:
#   * Persistência: todo estado é gravado de forma atômica (arquivo temporário +
#     rename) a cada mutação; ao reiniciar, o estado é recarregado do disco.
#   * Comunicação: qualquer processo pode chamar a API (o Agent é nomeado) e
#     processos inscritos recebem notificações por mensagens.
#   * Recuperação: o Agent é supervisionado (:one_for_one). Se ele falhar, é
#     reiniciado e recarrega o disco. Tarefas que estavam em voo quando o
#     sistema caiu voltam para a fila. Tarefas cujo lease expirou também são
#     recolocadas na fila pelo processo Reaper.
#   * Entrega: semântica "at-least-once". Um consumidor pode receber a mesma
#     tarefa mais de uma vez; o processamento deve ser idempotente.

defmodule TaskQueue do
  @moduledoc """
  API pública da fila de tarefas. Supervisiona o Store (Agent) e o Reaper.
  """
  use Supervisor

  @doc "Inicia a árvore de supervisão. Opções: :path, :lease_ms, :reap_interval_ms."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {TaskQueue.Store, opts},
      {TaskQueue.Reaper, opts}
    ]

    # Se o Store ou o Reaper falharem, apenas o processo afetado é reiniciado.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10, max_seconds: 60)
  end

  @doc "Adiciona uma tarefa ao final da fila. Retorna {:ok, id}."
  def enqueue(task), do: TaskQueue.Store.enqueue(task)

  @doc "Retira a próxima tarefa. Retorna {:ok, id, task} ou {:error, :empty}."
  def dequeue, do: TaskQueue.Store.dequeue()

  @doc "Confirma o processamento de uma tarefa em voo."
  def ack(id), do: TaskQueue.Store.ack(id)

  @doc "Devolve uma tarefa em voo para o final da fila."
  def nack(id), do: TaskQueue.Store.nack(id)

  @doc "Retorna contadores atuais da fila."
  def stats, do: TaskQueue.Store.stats()

  @doc "Inscreve um processo para receber notificações de novas tarefas."
  def subscribe(pid \\ self()), do: TaskQueue.Store.subscribe(pid)

  @doc "Remove a inscrição de um processo."
  def unsubscribe(pid \\ self()), do: TaskQueue.Store.unsubscribe(pid)
end

defmodule TaskQueue.Store do
  @moduledoc """
  Estado da fila mantido em um Agent nomeado. Cada mutação é executada dentro
  do processo do Agent (serializada) e persistida antes de responder.
  """
  use Agent

  @default_path "task_queue.dat"
  @default_lease_ms 30_000

  @doc false
  def start_link(opts) do
    path = Keyword.get(opts, :path, @default_path)
    lease_ms = Keyword.get(opts, :lease_ms, @default_lease_ms)
    Agent.start_link(fn -> load(path, lease_ms) end, name: __MODULE__)
  end

  # ---------------------------------------------------------------------------
  # Operações públicas (executadas por qualquer processo)
  # ---------------------------------------------------------------------------

  def enqueue(task) do
    Agent.get_and_update(__MODULE__, fn s ->
      id = s.next_id
      new = %{s | next_id: id + 1, pending: :queue.in({id, task}, s.pending)}
      new = new |> persist() |> notify({:task_queue, :enqueued, id})
      {{:ok, id}, new}
    end)
  end

  def dequeue do
    Agent.get_and_update(__MODULE__, fn s ->
      case :queue.out(s.pending) do
        {{:value, {id, task}}, rest} ->
          # O lease registra um prazo (tempo de sistema, pois sobrevive a reinícios).
          entry = {task, now_ms() + s.lease_ms}
          new = %{s | pending: rest, in_flight: Map.put(s.in_flight, id, entry)}
          new = persist(new)
          {{:ok, id, task}, new}

        {:empty, _} ->
          {{:error, :empty}, s}
      end
    end)
  end

  def ack(id) do
    Agent.get_and_update(__MODULE__, fn s ->
      case Map.fetch(s.in_flight, id) do
        {:ok, _} ->
          new = persist(%{s | in_flight: Map.delete(s.in_flight, id)})
          {:ok, new}

        :error ->
          {{:error, :unknown_task}, s}
      end
    end)
  end

  def nack(id) do
    Agent.get_and_update(__MODULE__, fn s ->
      case Map.fetch(s.in_flight, id) do
        {:ok, {task, _deadline}} ->
          new = %{
            s
            | in_flight: Map.delete(s.in_flight, id),
              pending: :queue.in({id, task}, s.pending)
          }

          {:ok, persist(new)}

        :error ->
          {{:error, :unknown_task}, s}
      end
    end)
  end

  @doc false
  # Recoloca na fila as tarefas cujo lease expirou (consumidor possivelmente morreu).
  def reap do
    Agent.get_and_update(__MODULE__, fn s ->
      now = now_ms()
      {expired, alive} = Enum.split_with(s.in_flight, fn {_id, {_t, dl}} -> dl <= now end)

      case expired do
        [] ->
          {0, s}

        _ ->
          requeued =
            expired
            |> Enum.sort_by(fn {id, _} -> id end)
            |> Enum.reduce(s.pending, fn {id, {task, _}}, q -> :queue.in({id, task}, q) end)

          new = persist(%{s | pending: requeued, in_flight: Map.new(alive)})
          {length(expired), new}
      end
    end)
  end

  def stats do
    Agent.get(__MODULE__, fn s ->
      %{
        pending: :queue.len(s.pending),
        in_flight: map_size(s.in_flight),
        next_id: s.next_id,
        subscribers: MapSet.size(s.subscribers)
      }
    end)
  end

  def subscribe(pid) do
    Agent.update(__MODULE__, fn s -> %{s | subscribers: MapSet.put(s.subscribers, pid)} end)
  end

  def unsubscribe(pid) do
    Agent.update(__MODULE__, fn s -> %{s | subscribers: MapSet.delete(s.subscribers, pid)} end)
  end

  # ---------------------------------------------------------------------------
  # Funções internas
  # ---------------------------------------------------------------------------

  # Envia uma mensagem a todos os inscritos (envio assíncrono e não bloqueante).
  defp notify(s, msg) do
    Enum.each(s.subscribers, fn pid -> send(pid, msg) end)
    s
  end

  # Grava o estado de forma atômica: escreve em arquivo temporário, sincroniza
  # em disco e só então renomeia. Uma queda no meio da escrita nunca corrompe
  # o arquivo principal.
  defp persist(s) do
    snapshot = %{
      next_id: s.next_id,
      pending: :queue.to_list(s.pending),
      in_flight: s.in_flight
    }

    bin = :erlang.term_to_binary(snapshot)
    tmp = s.path <> ".tmp"

    File.open!(tmp, [:write, :binary], fn io ->
      :ok = IO.binwrite(io, bin)
      :ok = :file.sync(io)
    end)

    :ok = File.rename!(tmp, s.path)
    s
  end

  # Carrega o estado do disco. Tarefas que estavam em voo na queda são
  # devolvidas para o início da fila, em ordem de id.
  defp load(path, lease_ms) do
    base = %{
      path: path,
      lease_ms: lease_ms,
      next_id: 1,
      pending: :queue.new(),
      in_flight: %{},
      subscribers: MapSet.new()
    }

    state =
      case File.read(path) do
        {:ok, bin} ->
          restore(base, bin, path)

        {:error, :enoent} ->
          base

        {:error, reason} ->
          raise "Falha ao ler #{path}: #{inspect(reason)}"
      end

    persist(state)
  end

  defp restore(base, bin, path) do
    %{next_id: next_id, pending: pending, in_flight: in_flight} =
      :erlang.binary_to_term(bin, [:safe])

    recovered =
      in_flight
      |> Enum.sort_by(fn {id, _} -> id end)
      |> Enum.map(fn {id, {task, _}} -> {id, task} end)

    %{base | next_id: next_id, pending: :queue.from_list(pending ++ recovered)}
  rescue
    ArgumentError ->
      # Arquivo ilegível: preserva-o para análise e inicia uma fila vazia.
      corrupt = path <> ".corrupt-" <> Integer.to_string(now_ms())
      IO.warn("TaskQueue: arquivo corrompido movido para #{corrupt}")
      File.rename(path, corrupt)
      base
  end

  defp now_ms, do: System.system_time(:millisecond)
end

defmodule TaskQueue.Reaper do
  @moduledoc """
  Processo periódico que recoloca na fila tarefas com lease expirado.
  É supervisionado; se falhar, é reiniciado pelo supervisor.
  """

  @default_interval_ms 5_000

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :permanent}
  end

  def start_link(opts) do
    interval = Keyword.get(opts, :reap_interval_ms, @default_interval_ms)
    Task.start_link(fn -> loop(interval) end)
  end

  defp loop(interval) do
    Process.sleep(interval)
    safe_reap()
    loop(interval)
  end

  # Se o Store estiver reiniciando, a chamada falha; ignoramos e tentamos depois.
  defp safe_reap do
    TaskQueue.Store.reap()
  catch
    :exit, _ -> :ok
  end
end
