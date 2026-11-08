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

# notifications.exs
# Sistema de gerenciamento de notificações com:
#   1. Limite de operações simultâneas (Notifications.Limiter)
#   2. Consistência em concorrência (escritas serializadas e transições compare-and-set)
#   3. Persistência em disco com recuperação após reinicialização
# Execução: elixir notifications.exs

defmodule Notifications.Store do
  @moduledoc """
  Fonte única da verdade para as notificações.

  - Todas as mutações passam por este GenServer, o que serializa as escritas.
  - Transições de status são do tipo compare-and-set: só ocorrem se o status
    atual for o esperado, evitando processamento duplicado.
  - Cada mutação é persistida de forma atômica (escrita em arquivo temporário,
    fsync e renomeação).
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Cria uma nova notificação com status :pending."
  def create(payload), do: GenServer.call(__MODULE__, {:create, payload})

  @doc "Retorna a notificação pelo id."
  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  @doc "Lista todas as notificações."
  def list, do: GenServer.call(__MODULE__, :list)

  @doc "Lista as notificações com o status informado."
  def list_by_status(status), do: GenServer.call(__MODULE__, {:list_by_status, status})

  @doc """
  Transição compare-and-set: altera o status de `from` para `to`
  somente se o status atual for exatamente `from`.
  """
  def transition(id, from, to, extra \\ %{}) do
    GenServer.call(__MODULE__, {:transition, id, from, to, extra})
  end

  # ── Callbacks ────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)

    data =
      path
      |> load()
      |> Map.new(fn {id, notif} -> {id, reset_interrupted(notif)} end)

    {:ok, %{path: path, data: data}}
  end

  @impl true
  def handle_call({:create, payload}, _from, state) do
    now = now()

    notif = %{
      id: new_id(),
      payload: payload,
      status: :pending,
      attempts: 0,
      last_error: nil,
      inserted_at: now,
      updated_at: now
    }

    new_data = Map.put(state.data, notif.id, notif)
    commit(state, new_data, {:ok, notif})
  end

  def handle_call({:get, id}, _from, state) do
    {:reply, fetch(state.data, id), state}
  end

  def handle_call(:list, _from, state) do
    {:reply, Map.values(state.data), state}
  end

  def handle_call({:list_by_status, status}, _from, state) do
    list = for {_id, %{status: ^status} = n} <- state.data, do: n
    {:reply, list, state}
  end

  def handle_call({:transition, id, from, to, extra}, _from, state) do
    case Map.fetch(state.data, id) do
      {:ok, %{status: ^from} = notif} ->
        updated =
          notif
          |> Map.merge(extra)
          |> Map.merge(%{status: to, updated_at: now()})

        commit(state, Map.put(state.data, id, updated), {:ok, updated})

      {:ok, %{status: current}} ->
        {:reply, {:error, {:invalid_status, current}}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # ── Auxiliares ───────────────────────────────────────────────────────────

  # Persiste o novo estado antes de responder; em caso de falha, mantém o antigo.
  defp commit(state, new_data, reply) do
    case persist(state.path, new_data) do
      :ok -> {:reply, reply, %{state | data: new_data}}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  defp fetch(data, id) do
    case Map.fetch(data, id) do
      {:ok, notif} -> {:ok, notif}
      :error -> {:error, :not_found}
    end
  end

  # Notificações que estavam :sending no momento da queda voltam a :pending
  # para serem reprocessadas.
  defp reset_interrupted(%{status: :sending} = notif), do: %{notif | status: :pending}
  defp reset_interrupted(notif), do: notif

  defp persist(path, data) do
    tmp = path <> ".tmp"
    bin = :erlang.term_to_binary(data)

    with {:ok, io} <- File.open(tmp, [:write, :binary]),
         :ok <- IO.binwrite(io, bin),
         :ok <- :file.sync(io),
         :ok <- File.close(io),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end

  defp load(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin)
      {:error, :enoent} -> %{}
      {:error, reason} -> raise "falha ao carregar #{path}: #{inspect(reason)}"
    end
  end

  defp new_id do
    "#{System.system_time(:nanosecond)}-#{:erlang.unique_integer([:positive])}"
  end

  defp now, do: System.system_time(:millisecond)
end

defmodule Notifications.Limiter do
  @moduledoc """
  Semáforo que limita o número de operações simultâneas.

  - `with_slot/1` aguarda uma vaga, executa a função e libera a vaga ao final.
  - Os chamadores são monitorados: se um processo morrer, sua vaga (ou sua
    posição na fila) é recuperada automaticamente.
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.get(opts, :max, 4), name: __MODULE__)
  end

  @doc "Executa `fun` ocupando uma vaga do limitador."
  def with_slot(fun) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      :ok = GenServer.call(__MODULE__, :release, :infinity)
    end
  end

  # ── Callbacks ────────────────────────────────────────────────────────────

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, {pid, _tag} = from, state) do
    ref = Process.monitor(pid)

    if map_size(state.active) < state.max do
      {:reply, :ok, %{state | active: Map.put(state.active, ref, pid)}}
    else
      {:noreply, %{state | waiting: :queue.in({from, ref}, state.waiting)}}
    end
  end

  def handle_call(:release, {pid, _tag}, state) do
    case Enum.find(state.active, fn {_ref, p} -> p == pid end) do
      {ref, _pid} ->
        Process.demonitor(ref, [:flush])
        state = %{state | active: Map.delete(state.active, ref)}
        {:reply, :ok, grant(state)}

      nil ->
        {:reply, :error, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    if Map.has_key?(state.active, ref) do
      {:noreply, grant(%{state | active: Map.delete(state.active, ref)})}
    else
      waiting = :queue.filter(fn {_from, r} -> r != ref end, state.waiting)
      {:noreply, %{state | waiting: waiting}}
    end
  end

  # Concede vagas livres aos próximos da fila.
  defp grant(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, {{pid, _tag} = from, ref}}, rest} ->
          GenServer.reply(from, :ok)
          grant(%{state | active: Map.put(state.active, ref, pid), waiting: rest})

        {:empty, _} ->
          state
      end
    else
      state
    end
  end
end

defmodule Notifications.Dispatcher do
  @moduledoc """
  Processo temporário supervisionado pelo DynamicSupervisor que realiza
  o envio de uma notificação.

  Fluxo:
    1. Aguarda vaga no Limiter.
    2. Reivindica a notificação (:pending -> :sending) de forma atômica.
    3. Envia e registra o resultado (:sent ou :failed).
  """
  use GenServer, restart: :temporary

  alias Notifications.{Store, Limiter}

  def start_link(id), do: GenServer.start_link(__MODULE__, id)

  @impl true
  def init(id), do: {:ok, id, {:continue, :dispatch}}

  @impl true
  def handle_continue(:dispatch, id) do
    Limiter.with_slot(fn ->
      case Store.transition(id, :pending, :sending) do
        {:ok, notif} -> deliver(notif)
        # Já reivindicada por outro processo: nada a fazer.
        {:error, _reason} -> :already_handled
      end
    end)

    {:stop, :normal, id}
  end

  defp deliver(notif) do
    attempts = notif.attempts + 1

    case simulate_send(notif.payload) do
      :ok ->
        Store.transition(notif.id, :sending, :sent, %{attempts: attempts})

      {:error, reason} ->
        Store.transition(notif.id, :sending, :failed, %{
          attempts: attempts,
          last_error: reason
        })
    end
  end

  # Simula a chamada a um canal externo (e-mail, SMS, push etc.).
  defp simulate_send(_payload) do
    Process.sleep(:rand.uniform(200) + 50)
    if :rand.uniform(10) <= 9, do: :ok, else: {:error, :timeout}
  end
end

defmodule Notifications do
  @moduledoc """
  API pública do sistema de notificações.

  Árvore de supervisão:
    - Notifications.Store      (estado persistido)
    - Notifications.Limiter    (limite de concorrência)
    - Notifications.DynSup     (DynamicSupervisor dos Dispatchers)
  """

  alias Notifications.{Store, Dispatcher}

  @doc """
  Inicia o sistema.

  Opções:
    - `:path` - arquivo de persistência (padrão "notifications.dat")
    - `:max_concurrency` - máximo de operações simultâneas (padrão 4)
  """
  def start(opts \\ []) do
    children = [
      {Store, path: Keyword.get(opts, :path, "notifications.dat")},
      {Notifications.Limiter, max: Keyword.get(opts, :max_concurrency, 4)},
      {DynamicSupervisor, name: Notifications.DynSup, strategy: :one_for_one}
    ]

    with {:ok, sup} <-
           Supervisor.start_link(children, strategy: :one_for_one, name: :notifications_sup) do
      recover()
      {:ok, sup}
    end
  end

  @doc "Cria uma notificação e agenda seu envio."
  def send_notification(payload) do
    with {:ok, notif} <- Store.create(payload),
         {:ok, _pid} <- DynamicSupervisor.start_child(Notifications.DynSup, {Dispatcher, notif.id}) do
      {:ok, notif}
    end
  end

  @doc "Consulta uma notificação pelo id."
  def get(id), do: Store.get(id)

  @doc "Lista todas as notificações."
  def list, do: Store.list()

  @doc """
  Reagenda o envio de todas as notificações pendentes.
  Executado automaticamente na inicialização, após a recuperação dos dados.
  """
  def recover do
    for %{id: id} <- Store.list_by_status(:pending) do
      DynamicSupervisor.start_child(Notifications.DynSup, {Dispatcher, id})
    end

    :ok
  end
end

# ── Exemplo de uso ──────────────────────────────────────────────────────────

{:ok, _} = Notifications.start(path: "notifications.dat", max_concurrency: 3)

Enum.each(1..10, fn i ->
  {:ok, _notif} = Notifications.send_notification(%{to: "usuario#{i}", body: "Olá!"})
end)

Process.sleep(3_000)

Enum.each(Notifications.list(), &IO.inspect/1)
