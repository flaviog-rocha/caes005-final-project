# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 2. Detectar encerramentos inesperados de processos.
# 3. Implementar expiração automática de registros.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Manter um histórico das operações realizadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: gerenciamento de entregas e rastreamento.
# Requisitos atendidos:
#   1. Persistência via behaviour (Deliveries.Store) com adaptador em memória.
#      Um adaptador Ecto poderia ser plugado sem alterar o Manager.
#   2. Detecção de encerramento inesperado via Process.monitor/spawn_monitor.
#   3. Expiração automática via varredura periódica (TTL + retenção).
#   4. Novas tentativas com backoff exponencial para falhas temporárias.
#   5. Histórico por entrega com todas as operações relevantes.
#
# Dependências: somente a biblioteca padrão (OTP/GenServer).
#
# Exemplo de uso:
#   {:ok, _} = Deliveries.Manager.start_link()
#   {:ok, id} = Deliveries.Manager.create(%{to: "Rua A, 10", items: 2})
#   :ok_ = Deliveries.Manager.track(id, %{location: "Centro de distribuição"})
#   {:ok, rec} = Deliveries.Manager.get(id)
#   {:ok, history} = Deliveries.Manager.history(id)

defmodule Deliveries.Store do
  @moduledoc """
  Contrato de persistência fictícia.

  Implementações podem ser trocadas sem alterar o gerenciador. Para uso com
  Ecto, uma implementação poderia delegar para um Repo; a tabela sugerida
  está descrita nos comentários ao final deste arquivo.
  """

  @type state :: term()
  @type id :: String.t()
  @type record :: map()

  @callback init() :: state()
  @callback put(state(), id(), record()) :: state()
  @callback get(state(), id()) :: record() | nil
  @callback delete(state(), id()) :: state()
  @callback all(state()) :: [record()]
end

defmodule Deliveries.Store.InMemory do
  @moduledoc """
  Adaptador de banco fictício em memória, baseado em mapa imutável.
  """
  @behaviour Deliveries.Store

  @impl true
  def init, do: %{}

  @impl true
  def put(db, id, record), do: Map.put(db, id, record)

  @impl true
  def get(db, id), do: Map.get(db, id)

  @impl true
  def delete(db, id), do: Map.delete(db, id)

  @impl true
  def all(db), do: Map.values(db)
end

defmodule Deliveries.Carrier do
  @moduledoc """
  Transportadora fictícia. Simula falhas temporárias, falhas permanentes e
  sucesso. Pode ser substituída por qualquer função `payload -> result`.

  Retornos esperados:
    * `:ok`
    * `{:error, :temporary, reason}`  -> elegível a nova tentativa
    * `{:error, :permanent, reason}`  -> falha definitiva
  """

  def dispatch(_payload) do
    Process.sleep(50)

    case :rand.uniform() do
      n when n < 0.4 -> {:error, :temporary, :carrier_unavailable}
      n when n < 0.45 -> {:error, :permanent, :invalid_address}
      _ -> :ok
    end
  end
end

defmodule Deliveries.Manager do
  @moduledoc """
  GenServer responsável por:

    * criar entregas e despachá-las em processos supervisionados por monitor;
    * detectar término inesperado de workers (`:DOWN` com motivo anormal);
    * reagendar envios com backoff exponencial em falhas temporárias;
    * expirar entregas ativas após `ttl_ms` e purgá-las após `retention_ms`;
    * manter histórico cronológico de todas as operações por entrega.

  Estados possíveis de uma entrega:
    :pending, :dispatching, :retry_scheduled, :in_transit,
    :delivered, :failed, :expired
  """

  use GenServer

  @active [:pending, :dispatching, :retry_scheduled, :in_transit]

  @defaults [
    name: __MODULE__,
    store: Deliveries.Store.InMemory,
    carrier: &Deliveries.Carrier.dispatch/1,
    ttl_ms: 60_000,
    retention_ms: 60_000,
    sweep_interval_ms: 5_000,
    max_attempts: 3,
    base_backoff_ms: 200
  ]

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    opts = Keyword.merge(@defaults, opts)
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def create(server \\ __MODULE__, payload) do
    GenServer.call(server, {:create, payload})
  end

  def track(server \\ __MODULE__, id, update) do
    GenServer.call(server, {:track, id, update})
  end

  def get(server \\ __MODULE__, id) do
    GenServer.call(server, {:get, id})
  end

  def history(server \\ __MODULE__, id) do
    GenServer.call(server, {:history, id})
  end

  # ---------------------------------------------------------------------------
  # Inicialização
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    store = Keyword.fetch!(opts, :store)

    state = %{
      opts: opts,
      store: store,
      db: store.init(),
      # Mapa: monitor_ref -> delivery_id
      monitors: %{}
    }

    schedule_sweep(opts)
    {:ok, state}
  end

  # ---------------------------------------------------------------------------
  # Chamadas síncronas
  # ---------------------------------------------------------------------------

  @impl true
  def handle_call({:create, payload}, _from, state) do
    now = now()
    id = "DLV-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]))

    rec =
      %{
        id: id,
        payload: payload,
        status: :pending,
        attempts: 0,
        created_at: now,
        expires_at: now + state.opts[:ttl_ms],
        worker: nil,
        events: [],
        history: []
      }
      |> log(:created, %{})

    state =
      state
      |> save(rec)
      |> dispatch(id)

    {:reply, {:ok, id}, state}
  end

  def handle_call({:track, id, update}, _from, state) do
    case fetch(state, id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      %{status: :expired} ->
        {:reply, {:error, :expired}, state}

      rec ->
        event = %{at: now(), data: update}

        rec =
          %{rec | events: [event | rec.events]}
          |> log(:tracked, update)

        {:reply, :ok, save(state, rec)}
    end
  end

  def handle_call({:get, id}, _from, state) do
    case fetch(state, id) do
      nil -> {:reply, {:error, :not_found}, state}
      rec -> {:reply, {:ok, rec}, state}
    end
  end

  def handle_call({:history, id}, _from, state) do
    case fetch(state, id) do
      nil -> {:reply, {:error, :not_found}, state}
      rec -> {:reply, {:ok, Enum.reverse(rec.history)}, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Mensagens internas
  # ---------------------------------------------------------------------------

  # Resultado enviado pelo worker antes de encerrar normalmente.
  @impl true
  def handle_info({:worker_result, id, pid, result}, state) do
    case fetch(state, id) do
      %{worker: {^pid, ref}} = rec ->
        Process.demonitor(ref, [:flush])
        state = %{state | monitors: Map.delete(state.monitors, ref)}

        rec =
          %{rec | worker: nil}
          |> log(:worker_result, %{result: result})

        {:noreply, state |> save(rec) |> handle_outcome(id, result)}

      _ ->
        # Resultado obsoleto (ex.: entrega já expirou).
        {:noreply, state}
    end
  end

  # Encerramento de worker monitorado.
  # Se ainda havia worker ativo sem resultado, o término é inesperado.
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, monitors} ->
        state = %{state | monitors: monitors}

        case fetch(state, id) do
          %{worker: {^pid, ^ref}} = rec ->
            rec =
              %{rec | worker: nil}
              |> log(:unexpected_exit, %{reason: reason})

            state = save(state, rec)

            {:noreply,
             handle_outcome(state, id, {:error, :temporary, {:process_down, reason}})}

          _ ->
            {:noreply, state}
        end
    end
  end

  # Disparo de nova tentativa agendada.
  def handle_info({:retry, id}, state) do
    case fetch(state, id) do
      %{status: :retry_scheduled} -> {:noreply, dispatch(state, id)}
      _ -> {:noreply, state}
    end
  end

  # Varredura periódica de expiração e purga.
  def handle_info(:sweep, state) do
    now = now()

    state =
      state.db
      |> then(&state.store.all(&1))
      |> Enum.reduce(state, fn rec, acc -> sweep_record(acc, rec, now) end)

    schedule_sweep(state.opts)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Despacho e tratamento de resultados
  # ---------------------------------------------------------------------------

  defp dispatch(state, id) do
    rec = fetch(state, id)
    rec = %{rec | attempts: rec.attempts + 1, status: :dispatching}

    carrier = state.opts[:carrier]
    parent = self()
    payload = rec.payload

    {pid, ref} =
      spawn_monitor(fn ->
        result = safe_call(carrier, payload)
        send(parent, {:worker_result, id, self(), result})
      end)

    rec =
      %{rec | worker: {pid, ref}}
      |> log(:dispatch_started, %{attempt: rec.attempts, pid: pid})

    state
    |> save(rec)
    |> then(&%{&1 | monitors: Map.put(&1.monitors, ref, id)})
  end

  # Converte exceções e throws/exits do carrier em falha temporária.
  defp safe_call(carrier, payload) do
    carrier.(payload)
  rescue
    e -> {:error, :temporary, {:exception, Exception.message(e)}}
  catch
    kind, reason -> {:error, :temporary, {kind, reason}}
  end

  defp handle_outcome(state, id, :ok) do
    update(state, id, fn rec ->
      rec |> set_status(:delivered) |> log(:delivered, %{})
    end)
  end

  defp handle_outcome(state, id, {:error, :permanent, reason}) do
    update(state, id, fn rec ->
      rec |> set_status(:failed) |> log(:failed, %{reason: reason})
    end)
  end

  defp handle_outcome(state, id, {:error, :temporary, reason}) do
    retry_or_fail(state, id, reason)
  end

  defp handle_outcome(state, id, other) do
    handle_outcome(state, id, {:error, :permanent, {:unexpected_return, other}})
  end

  defp retry_or_fail(state, id, reason) do
    rec = fetch(state, id)

    if rec.attempts < state.opts[:max_attempts] do
      # Backoff exponencial: base * 2^(tentativas - 1)
      delay = state.opts[:base_backoff_ms] * Integer.pow(2, rec.attempts - 1)
      Process.send_after(self(), {:retry, id}, delay)

      update(state, id, fn r ->
        r
        |> set_status(:retry_scheduled)
        |> log(:retry_scheduled, %{reason: reason, delay_ms: delay})
      end)
    else
      update(state, id, fn r ->
        r
        |> set_status(:failed)
        |> log(:failed, %{reason: reason, attempts: rec.attempts})
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Expiração e purga
  # ---------------------------------------------------------------------------

  defp sweep_record(state, rec, now) do
    cond do
      now >= rec.expires_at + state.opts[:retention_ms] ->
        %{state | db: state.store.delete(state.db, rec.id)}

      now >= rec.expires_at and rec.status in @active ->
        expire(state, rec)

      true ->
        state
    end
  end

  defp expire(state, rec) do
    state = cancel_worker(state, rec)

    rec =
      %{rec | worker: nil}
      |> set_status(:expired)
      |> log(:expired, %{})

    save(state, rec)
  end

  defp cancel_worker(state, %{worker: {pid, ref}}) do
    Process.demonitor(ref, [:flush])
    Process.exit(pid, :kill)
    %{state | monitors: Map.delete(state.monitors, ref)}
  end

  defp cancel_worker(state, _rec), do: state

  # ---------------------------------------------------------------------------
  # Auxiliares
  # ---------------------------------------------------------------------------

  defp fetch(state, id), do: state.store.get(state.db, id)

  defp save(state, rec) do
    %{state | db: state.store.put(state.db, rec.id, rec)}
  end

  defp update(state, id, fun) do
    save(state, fun.(fetch(state, id)))
  end

  defp set_status(rec, status), do: %{rec | status: status}

  defp log(rec, event, details) do
    %{rec | history: [{now(), event, details} | rec.history]}
  end

  defp schedule_sweep(opts) do
    Process.send_after(self(), :sweep, opts[:sweep_interval_ms])
  end

  defp now, do: System.system_time(:millisecond)
end

# -----------------------------------------------------------------------------
# Referência opcional para persistência com Ecto (não compilada neste arquivo):
#
# Migration sugerida:
#   create table(:deliveries, primary_key: false) do
#     add :id, :string, primary_key: true
#     add :payload, :map, null: false
#     add :status, :string, null: false
#     add :attempts, :integer, null: false, default: 0
#     add :created_at, :bigint, null: false
#     add :expires_at, :bigint, null: false
#     add :events, {:array, :map}, default: []
#     add :history, {:array, :map}, default: []
#   end
#   create index(:deliveries, [:status])
#   create index(:deliveries, [:expires_at])
#
# Um módulo Deliveries.Store.Ecto implementaria os mesmos callbacks do
# behaviour Deliveries.Store delegando para um Repo.
# -----------------------------------------------------------------------------
