# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Recuperar processos após falhas inesperadas.
# 2. Gerar notificações quando determinadas condições forem atingidas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Validar os dados recebidos antes de realizar operações.
# 5. Manter um histórico das operações realizadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - ETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: sistema de gerenciamento de notificações com ETS.
# Uso (exemplo): {:ok, _} = NotificationSystem.start_link([])

defmodule NotificationSystem do
  @moduledoc """
  Sistema de gerenciamento de operações e notificações.

  Requisitos atendidos:
    1. Recuperação após falhas: estado persistido em ETS com `heir` (o Keeper
       herda as tabelas se o Storage cair) e workers `:transient` reiniciados
       pelo DynamicSupervisor, retomando do progresso salvo.
    2. Notificações geradas quando o limite (`notify_at`, em %) é atingido.
    3. Consulta do estado atual das operações (`status/1`, `running/0`).
    4. Validação de todos os dados antes de qualquer operação.
    5. Histórico de eventos por operação (`history/1`).
  """

  use Supervisor

  alias NotificationSystem.{Keeper, Storage, Validator, Operation, Worker, Recovery}

  @worker_supervisor NotificationSystem.WorkerSupervisor

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # Mantém as tabelas ETS caso o Storage caia (é o heir das tabelas).
      Keeper,
      # Dono das tabelas ETS; reinicia e reassume as tabelas do Keeper.
      Storage,
      # Supervisiona um worker por operação em andamento.
      {DynamicSupervisor, name: @worker_supervisor, strategy: :one_for_one},
      # Retoma operações que estavam :running antes de uma falha.
      %{
        id: :recovery,
        start: {Recovery, :start_link, []},
        restart: :transient
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Cria uma operação após validar os parâmetros.

  Parâmetros (átomos como chaves):
    * `:name` (obrigatório) - texto não vazio
    * `:total_steps` (obrigatório) - inteiro entre 1 e 100_000
    * `:notify_at` (opcional, padrão 100) - inteiro entre 1 e 100 (percentual)
    * `:step_interval_ms` (opcional, padrão 1000) - inteiro >= 0
  """
  def create_operation(params) do
    with {:ok, attrs} <- Validator.validate(params) do
      op = Operation.new(attrs)
      :ok = Storage.put_operation(op)
      Storage.add_history(op.id, :created, %{total_steps: op.total_steps})
      {:ok, _} = DynamicSupervisor.start_child(@worker_supervisor, {Worker, op.id})
      {:ok, op.id}
    end
  end

  @doc "Retorna o estado atual resumido de uma operação."
  def status(id) when is_binary(id) do
    with {:ok, op} <- Storage.get_operation(id) do
      {:ok, Operation.summary(op)}
    end
  end

  def status(_), do: {:error, :invalid_id}

  @doc "Lista as operações em andamento."
  def running do
    Storage.list_operations(:running)
    |> Enum.map(&Operation.summary/1)
  end

  @doc "Retorna o histórico ordenado de eventos de uma operação."
  def history(id) when is_binary(id) do
    with {:ok, _op} <- Storage.get_operation(id) do
      {:ok, Storage.history(id)}
    end
  end

  def history(_), do: {:error, :invalid_id}

  @doc "Lista as notificações geradas (da mais antiga para a mais recente)."
  def notifications, do: Storage.notifications()
end

# ---------------------------------------------------------------------------
# Keeper: herdeiro das tabelas ETS. Mantém a posse das tabelas enquanto o
# Storage está fora do ar, permitindo recuperação sem perda de dados.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Keeper do
  @moduledoc false
  use GenServer

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  # Entrega a posse das tabelas ao processo que chama (novo Storage).
  def claim(tables), do: GenServer.call(__MODULE__, {:claim, tables})

  @impl true
  def init(_), do: {:ok, nil}

  @impl true
  def handle_call({:claim, tables}, {pid, _tag}, state) do
    Enum.each(tables, fn tab -> true = :ets.give_away(tab, pid, :claimed) end)
    {:reply, :ok, state}
  end

  # Mensagens {:"ETS-TRANSFER", ...} ao receber tabelas herdadas são ignoradas.
  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end

# ---------------------------------------------------------------------------
# Storage: dono das tabelas ETS e camada de acesso aos dados.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Storage do
  @moduledoc false
  use GenServer

  @operations :ns_operations
  @history :ns_history
  @notifications :ns_notifications
  @tables [@operations, @history, @notifications]

  def start_link(_arg), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    if :ets.whereis(@operations) == :undefined do
      # Primeira inicialização: cria as tabelas com o Keeper como herdeiro.
      heir = Process.whereis(NotificationSystem.Keeper)

      :ets.new(@operations, [:set, :named_table, :public, read_concurrency: true, heir: {heir, :ns}])
      :ets.new(@history, [:ordered_set, :named_table, :public, heir: {heir, :ns}])
      :ets.new(@notifications, [:ordered_set, :named_table, :public, heir: {heir, :ns}])
    else
      # Reinicialização: as tabelas estão com o Keeper; reassume a posse.
      :ok = NotificationSystem.Keeper.claim(@tables)
    end

    {:ok, nil}
  end

  # --- Operações -------------------------------------------------------------

  def put_operation(op) do
    true = :ets.insert_new(@operations, {op.id, op})
    :ok
  end

  def get_operation(id) do
    case :ets.lookup(@operations, id) do
      [{^id, op}] -> {:ok, op}
      [] -> {:error, :not_found}
    end
  end

  def update_operation(id, fun) do
    with {:ok, op} <- get_operation(id) do
      new_op = fun.(op)
      :ets.insert(@operations, {id, new_op})
      {:ok, new_op}
    end
  end

  def list_operations(status \\ nil) do
    @operations
    |> :ets.tab2list()
    |> Enum.map(fn {_id, op} -> op end)
    |> Enum.filter(fn op -> is_nil(status) or op.status == status end)
  end

  # --- Histórico -------------------------------------------------------------

  def add_history(op_id, event, details \\ %{}) do
    key = {op_id, :erlang.unique_integer([:monotonic])}
    entry = %{at: DateTime.utc_now(), event: event, details: details}
    :ets.insert(@history, {key, entry})
    :ok
  end

  def history(op_id) do
    for {_key, entry} <- :ets.match_object(@history, {{op_id, :_}, :_}), do: entry
  end

  # --- Notificações ----------------------------------------------------------

  def add_notification(attrs) do
    key = :erlang.unique_integer([:monotonic])
    :ets.insert(@notifications, {key, Map.put(attrs, :at, DateTime.utc_now())})
    :ok
  end

  def notifications do
    for {_key, notification} <- :ets.tab2list(@notifications), do: notification
  end
end

# ---------------------------------------------------------------------------
# Validator: valida todos os dados antes de qualquer operação.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Validator do
  @moduledoc false

  @max_steps 100_000

  @doc """
  Valida e normaliza os parâmetros de uma operação.
  Retorna `{:ok, attrs}` ou `{:error, [{campo, motivo}]}`.
  """
  def validate(params) when is_map(params) do
    attrs = %{
      name: Map.get(params, :name),
      total_steps: Map.get(params, :total_steps),
      notify_at: Map.get(params, :notify_at, 100),
      step_interval_ms: Map.get(params, :step_interval_ms, 1_000)
    }

    case Enum.flat_map(attrs, &check/1) do
      [] -> {:ok, attrs}
      errors -> {:error, errors}
    end
  end

  def validate(_), do: {:error, [{:params, "deve ser um mapa"}]}

  defp check({:name, name}) do
    if is_binary(name) and String.trim(name) != "",
      do: [],
      else: [{:name, "deve ser um texto não vazio"}]
  end

  defp check({:total_steps, n}) when is_integer(n) and n >= 1 and n <= @max_steps, do: []
  defp check({:total_steps, _}), do: [{:total_steps, "deve ser inteiro entre 1 e #{@max_steps}"}]

  defp check({:notify_at, n}) when is_integer(n) and n >= 1 and n <= 100, do: []
  defp check({:notify_at, _}), do: [{:notify_at, "deve ser inteiro entre 1 e 100"}]

  defp check({:step_interval_ms, n}) when is_integer(n) and n >= 0, do: []
  defp check({:step_interval_ms, _}), do: [{:step_interval_ms, "deve ser inteiro >= 0"}]
end

# ---------------------------------------------------------------------------
# Operation: modelo e funções auxiliares de operações.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Operation do
  @moduledoc false

  def new(attrs) do
    Map.merge(attrs, %{
      id: generate_id(),
      status: :running,
      progress: 0,
      notified: false,
      created_at: DateTime.utc_now(),
      finished_at: nil
    })
  end

  def percent(%{progress: progress, total_steps: total}), do: div(progress * 100, total)

  def summary(op) do
    %{
      id: op.id,
      name: op.name,
      status: op.status,
      progress: op.progress,
      total_steps: op.total_steps,
      percent: percent(op),
      notified: op.notified
    }
  end

  defp generate_id do
    time = Integer.to_string(System.system_time(:nanosecond), 36)
    uniq = Integer.to_string(System.unique_integer([:positive]), 36)
    "op_" <> time <> "_" <> uniq
  end
end

# ---------------------------------------------------------------------------
# Worker: executa uma operação passo a passo. Estado vem do ETS, então ao
# ser reiniciado após uma falha ele retoma do progresso salvo.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Worker do
  @moduledoc false
  use GenServer, restart: :transient

  alias NotificationSystem.{Storage, Operation}

  def start_link(op_id), do: GenServer.start_link(__MODULE__, op_id)

  @impl true
  def init(op_id) do
    case Storage.get_operation(op_id) do
      {:ok, %{status: :running} = op} ->
        event = if op.progress > 0, do: :resumed, else: :started
        Storage.add_history(op_id, event, %{progress: op.progress})
        {:ok, %{id: op_id, interval: op.step_interval_ms}, {:continue, :schedule}}

      # Operação inexistente ou já finalizada: não há nada a fazer.
      _ ->
        :ignore
    end
  end

  @impl true
  def handle_continue(:schedule, state) do
    schedule(state.interval)
    {:noreply, state}
  end

  @impl true
  def handle_info(:step, %{id: id} = state) do
    {:ok, op} = Storage.update_operation(id, &%{&1 | progress: &1.progress + 1})
    Storage.add_history(id, :step, %{progress: op.progress, total: op.total_steps})
    op = maybe_notify(op)

    if op.progress >= op.total_steps do
      {:ok, _} =
        Storage.update_operation(id, &%{&1 | status: :completed, finished_at: DateTime.utc_now()})

      Storage.add_history(id, :completed, %{})
      {:stop, :normal, state}
    else
      schedule(state.interval)
      {:noreply, state}
    end
  end

  # Gera a notificação apenas uma vez, quando o limite for atingido.
  defp maybe_notify(op) do
    percent = Operation.percent(op)

    if not op.notified and percent >= op.notify_at do
      Storage.add_notification(%{
        operation_id: op.id,
        percent: percent,
        message: "Operação #{op.name} atingiu #{percent}% (limite: #{op.notify_at}%)"
      })

      {:ok, updated} = Storage.update_operation(op.id, &%{&1 | notified: true})
      updated
    else
      op
    end
  end

  defp schedule(interval), do: Process.send_after(self(), :step, interval)
end

# ---------------------------------------------------------------------------
# Recovery: ao iniciar, reinicia workers de operações que estavam em andamento.
# ---------------------------------------------------------------------------

defmodule NotificationSystem.Recovery do
  @moduledoc false

  alias NotificationSystem.{Storage, Worker}

  def start_link(_arg) do
    Task.start_link(&run/0)
  end

  def run do
    for op <- Storage.list_operations(:running) do
      Storage.add_history(op.id, :recovered, %{progress: op.progress})
      {:ok, _} = DynamicSupervisor.start_child(NotificationSystem.WorkerSupervisor, {Worker, op.id})
    end

    :ok
  end
end
