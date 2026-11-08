# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de troca de mensagens entre usuários, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Permitir a execução de operações simultâneas.
# 3. Recuperar processos após falhas inesperadas.
# 4. Manter um histórico das operações realizadas.
# 5. Executar determinadas operações periodicamente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - GenServer
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de troca de mensagens entre usuários com GenServer.
#
# Uso básico (exemplo, não executado automaticamente):
#
#   {:ok, _sup} = Chat.Supervisor.start_link(ttl_ms: :timer.hours(1), prune_interval_ms: 5_000)
#   :ok = Chat.Broker.register("ana")
#   :ok = Chat.Broker.register("bruno")
#   :ok = Chat.Broker.register("carla")
#   {:ok, _id} = Chat.Broker.send_message("ana", "bruno", "Olá!")
#   Chat.Broker.broadcast("ana", ["bruno", "carla"], "Reunião às 15h")
#   Chat.Broker.broadcast_async("ana", ["bruno", "carla"], "Lembrete assíncrono")
#   {:ok, mensagens} = Chat.Broker.inbox("bruno")
#   Chat.Broker.history(10)

defmodule Chat.Supervisor do
  @moduledoc """
  Supervisor raiz do sistema.

  Estratégia `:one_for_one`: se o `Chat.Broker` falhar inesperadamente, somente
  ele é reiniciado. Como o broker restaura seu estado a partir do snapshot em
  disco, a reinicialização é transparente para os clientes.

  O `Task.Supervisor` permite executar operações concorrentes supervisionadas
  (entregas assíncronas de broadcast).
  """
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      {Task.Supervisor, name: Chat.TaskSupervisor},
      {Chat.Broker, opts}
    ]

    # Limite de reinicializações para evitar loops infinitos de falha.
    Supervisor.init(children, strategy: :one_for_one, max_restarts: 5, max_seconds: 10)
  end
end

defmodule Chat.Broker do
  @moduledoc """
  GenServer responsável por:

    * Comunicação entre processos: operações são feitas via mensagens
      síncronas (`call`) e assíncronas (`Task`) ao GenServer.
    * Operações simultâneas: `broadcast/3` entrega mensagens em paralelo
      com `Task.async_stream/3`; `broadcast_async/3` as executa em tarefas
      supervisionadas.
    * Recuperação: o estado é persistido em snapshot (escrita atômica via
      arquivo temporário + `File.rename/2`) a cada alteração e restaurado no
      `init/1`, permitindo que o supervisor reinicie o processo sem perda de
      dados já confirmados.
    * Histórico: cada operação é registrada em uma lista limitada.
    * Tarefas periódicas: a cada intervalo configurado, mensagens expiradas
      (não lidas além do TTL) são removidas.
  """
  use GenServer

  @history_limit 500
  @default_ttl_ms :timer.hours(24)
  @default_prune_interval_ms :timer.minutes(1)

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Registra um novo usuário."
  def register(server \\ __MODULE__, user) when is_binary(user) do
    GenServer.call(server, {:register, user})
  end

  @doc "Envia uma mensagem de `from` para `to`. Retorna `{:ok, id}` ou erro."
  def send_message(server \\ __MODULE__, from, to, body) do
    GenServer.call(server, {:send, from, to, body})
  end

  @doc """
  Envia a mesma mensagem para vários destinatários de forma concorrente.
  Retorna uma lista de `{destinatario, resultado}`.
  """
  def broadcast(server \\ __MODULE__, from, recipients, body) do
    recipients
    |> Task.async_stream(
      fn to -> {to, send_message(server, from, to, body)} end,
      max_concurrency: System.schedulers_online(),
      timeout: 5_000,
      ordered: true
    )
    |> Enum.map(fn {:ok, result} -> result end)
  end

  @doc """
  Executa o broadcast em uma tarefa supervisionada, sem bloquear o chamador.
  """
  def broadcast_async(server \\ __MODULE__, from, recipients, body) do
    Task.Supervisor.start_child(Chat.TaskSupervisor, fn ->
      broadcast(server, from, recipients, body)
    end)
  end

  @doc "Retorna e marca como lidas as mensagens não lidas de `user`."
  def inbox(server \\ __MODULE__, user) do
    GenServer.call(server, {:inbox, user})
  end

  @doc "Retorna as últimas `limit` entradas do histórico de operações (mais recentes primeiro)."
  def history(server \\ __MODULE__, limit \\ 20) do
    GenServer.call(server, {:history, limit})
  end

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    snapshot_path =
      Keyword.get(opts, :snapshot_path, Path.join(System.tmp_dir!(), "chat_broker.snapshot"))

    state =
      %{
        path: snapshot_path,
        users: MapSet.new(),
        inboxes: %{},
        history: [],
        seq: 0,
        ttl_ms: Keyword.get(opts, :ttl_ms, @default_ttl_ms),
        prune_interval_ms: Keyword.get(opts, :prune_interval_ms, @default_prune_interval_ms)
      }
      |> restore()

    schedule_prune(state.prune_interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call({:register, user}, _from, state) do
    if MapSet.member?(state.users, user) do
      {:reply, {:error, :already_registered}, commit(state, {:register, user, :rejected})}
    else
      state = %{
        state
        | users: MapSet.put(state.users, user),
          inboxes: Map.put(state.inboxes, user, [])
      }

      {:reply, :ok, commit(state, {:register, user, :ok})}
    end
  end

  def handle_call({:send, from, to, body}, _from, state) do
    if MapSet.member?(state.users, from) and MapSet.member?(state.users, to) do
      msg = %{id: state.seq + 1, from: from, to: to, body: body, at: now()}

      state = %{
        state
        | seq: msg.id,
          inboxes: Map.update!(state.inboxes, to, &[msg | &1])
      }

      {:reply, {:ok, msg.id}, commit(state, {:send, from, to, msg.id})}
    else
      {:reply, {:error, :unknown_user}, commit(state, {:send, from, to, :rejected})}
    end
  end

  def handle_call({:inbox, user}, _from, state) do
    case Map.fetch(state.inboxes, user) do
      {:ok, msgs} ->
        state = %{state | inboxes: Map.put(state.inboxes, user, [])}
        {:reply, {:ok, Enum.reverse(msgs)}, commit(state, {:inbox, user, length(msgs)})}

      :error ->
        {:reply, {:error, :unknown_user}, state}
    end
  end

  def handle_call({:history, limit}, _from, state) do
    {:reply, Enum.take(state.history, limit), state}
  end

  @impl true
  def handle_info(:prune, state) do
    cutoff = now() - state.ttl_ms

    {inboxes, removed} =
      Enum.reduce(state.inboxes, {%{}, 0}, fn {user, msgs}, {acc, count} ->
        {kept, expired} = Enum.split_with(msgs, &(&1.at >= cutoff))
        {Map.put(acc, user, kept), count + length(expired)}
      end)

    schedule_prune(state.prune_interval_ms)
    state = %{state | inboxes: inboxes}

    if removed > 0 do
      {:noreply, commit(state, {:prune, removed})}
    else
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções auxiliares
  # ---------------------------------------------------------------------------

  # Registra a entrada no histórico (limitado) e persiste o estado.
  defp commit(state, operation) do
    entry = %{at: now(), op: operation}
    state = %{state | history: Enum.take([entry | state.history], @history_limit)}
    persist(state)
  end

  defp persist(state) do
    snapshot =
      state
      |> Map.take([:users, :inboxes, :history, :seq])
      |> :erlang.term_to_binary()

    tmp_path = state.path <> ".tmp"
    File.write!(tmp_path, snapshot)
    # Renomeação é atômica: o snapshot nunca fica parcialmente escrito.
    File.rename!(tmp_path, state.path)
    state
  end

  defp restore(state) do
    with {:ok, binary} <- File.read(state.path),
         {:ok, snapshot} <- safe_decode(binary) do
      Map.merge(state, snapshot)
    else
      _ -> state
    end
  end

  defp safe_decode(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    _ -> :error
  end

  defp schedule_prune(interval_ms) do
    Process.send_after(self(), :prune, interval_ms)
  end

  defp now, do: System.system_time(:millisecond)
end
