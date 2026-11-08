# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de gerenciamento de notificações, atendendo somente aos seguintes requisitos:
#
# 1. Permitir que diferentes entidades sejam processadas independentemente.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Processar grandes volumes de dados sem carregá-los integralmente na memória.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de gerenciamento de notificações com DETS.
#
# Uso:
#   {:ok, _} = NotificationSystem.start_link(
#     path: "notifications.dets",
#     deliver: fn %{id: id, entity: e, payload: p, attempt: n} -> :ok end,
#     max_attempts: 5,
#     base_backoff: 1_000,
#     max_backoff: 60_000,
#     poll_interval: 1_000,
#     chunk: 500
#   )
#   NotificationSystem.enqueue(:cliente_1, Stream.map(1..1_000_000, &%{msg: &1}))
#   NotificationSystem.stream(:cliente_1, :failed) |> Enum.take(10)

defmodule NotificationSystem do
  alias NotificationSystem.{Store, EntityWorker}

  @registry NotificationSystem.Registry
  @worker_sup NotificationSystem.WorkerSupervisor
  @chunk 500

  # Inicia o supervisor raiz: Registry (um processo por entidade),
  # Store (dono da tabela DETS) e DynamicSupervisor dos workers.
  def start_link(opts \\ []) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {Store, opts},
      {DynamicSupervisor, strategy: :one_for_one, name: @worker_sup}
    ]

    case Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__) do
      {:ok, pid} ->
        # Retoma entidades que ficaram com notificações pendentes antes de uma parada.
        resume_pending()
        {:ok, pid}

      error ->
        error
    end
  end

  # Enfileira notificações para uma entidade. Aceita qualquer enumerável (inclusive
  # streams) e persiste em lotes, sem materializar a coleção inteira em memória.
  def enqueue(entity, payloads) do
    table = Store.table()
    now = now()

    total =
      payloads
      |> Stream.map(fn payload -> {{entity, new_id()}, :pending, now, 0, payload} end)
      |> Stream.chunk_every(@chunk)
      |> Enum.reduce(0, fn batch, acc ->
        true = :dets.insert(table, batch)
        acc + length(batch)
      end)

    ensure_worker(entity)
    EntityWorker.wake(entity)
    {:ok, total}
  end

  # Retorna um stream preguiçoso das notificações de uma entidade com um dado status
  # (:pending, :delivered ou :failed), lendo a tabela DETS em blocos.
  def stream(entity, status) do
    Store.stream(entity, status)
  end

  # Identificador único e ordenável o suficiente para as chaves da tabela.
  defp new_id do
    {System.system_time(:nanosecond), System.unique_integer([:positive])}
  end

  defp now, do: System.system_time(:millisecond)

  # Garante um processo dedicado para a entidade (isolamento entre entidades).
  defp ensure_worker(entity) do
    DynamicSupervisor.start_child(@worker_sup, {EntityWorker, entity})
  end

  # Varre a tabela em blocos coletando as entidades com pendências.
  defp resume_pending do
    ms = [{{{:"$1", :_}, :pending, :_, :_, :_}, [], [:"$1"]}]

    entities =
      Store.fold(
        :dets.select(Store.table(), ms, @chunk),
        MapSet.new(),
        fn rows, acc -> Enum.into(rows, acc) end
      )

    Enum.each(entities, fn entity ->
      ensure_worker(entity)
      EntityWorker.wake(entity)
    end)
  end
end

defmodule NotificationSystem.Store do
  # Processo dono da tabela DETS. Toda a persistência passa por aqui.
  use GenServer

  @table :notification_store
  @chunk 500

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def table, do: @table

  def config, do: GenServer.call(__MODULE__, :config)

  @impl true
  def init(opts) do
    file = opts |> Keyword.get(:path, "notifications.dets") |> String.to_charlist()

    case :dets.open_file(@table, file: file, type: :set) do
      {:ok, _ref} -> {:ok, %{config: build_config(opts)}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:config, _from, state), do: {:reply, state.config, state}

  @impl true
  def terminate(_reason, _state) do
    :dets.close(@table)
  end

  defp build_config(opts) do
    %{
      deliver: Keyword.fetch!(opts, :deliver),
      max_attempts: Keyword.get(opts, :max_attempts, 5),
      base_backoff: Keyword.get(opts, :base_backoff, 1_000),
      max_backoff: Keyword.get(opts, :max_backoff, 60_000),
      poll_interval: Keyword.get(opts, :poll_interval, 1_000),
      chunk: Keyword.get(opts, :chunk, @chunk)
    }
  end

  # Busca notificações pendentes e vencidas de uma entidade, limitadas a `limit` registros.
  # A filtragem é feita dentro do match spec, então só o lote é materializado.
  def due(entity, now, limit) do
    ms = [
      {
        {{:"$1", :_}, :pending, :"$2", :_, :_},
        [{:"=:=", :"$1", {:const, entity}}, {:"=<", :"$2", now}],
        [:"$_"]
      }
    ]

    take_rows(:dets.select(@table, ms, limit))
  end

  # Atualiza o registro (inserção sobrescreve a chave existente).
  def update(key, status, next_at, attempts, payload) do
    true = :dets.insert(@table, {key, status, next_at, attempts, payload})
    :ok
  end

  # Stream preguiçoso sobre o select com continuação do DETS.
  def stream(entity, status, chunk \\ @chunk) do
    ms = [
      {
        {{:"$1", :_}, :"$2", :_, :_, :_},
        [{:"=:=", :"$1", {:const, entity}}, {:"=:=", :"$2", {:const, status}}],
        [:"$_"]
      }
    ]

    Stream.resource(
      fn -> :dets.select(@table, ms, chunk) end,
      fn
        :"$end_of_table" -> {:halt, nil}
        {:error, reason} -> raise "falha na leitura DETS: #{inspect(reason)}"
        {rows, cont} -> {Enum.map(rows, &to_map/1), :dets.select(cont)}
      end,
      fn _ -> :ok end
    )
  end

  # Redução em blocos sobre toda a tabela, sem carregá-la inteira.
  def fold(:"$end_of_table", acc, _fun), do: acc
  def fold({:error, reason}, _acc, _fun), do: raise("falha na leitura DETS: #{inspect(reason)}")
  def fold({rows, cont}, acc, fun), do: fold(:dets.select(cont), fun.(rows, acc), fun)

  defp take_rows(:"$end_of_table"), do: []
  defp take_rows({:error, reason}), do: raise("falha na leitura DETS: #{inspect(reason)}")
  # DETS pode devolver lotes vazios antes do fim; continua até achar algo ou acabar.
  defp take_rows({[], cont}), do: take_rows(:dets.select(cont))
  defp take_rows({rows, _cont}), do: rows

  defp to_map({{entity, id}, status, next_at, attempts, payload}) do
    %{
      entity: entity,
      id: id,
      status: status,
      next_at: next_at,
      attempts: attempts,
      payload: payload
    }
  end
end

defmodule NotificationSystem.EntityWorker do
  # Um processo por entidade: entidades são processadas de forma independente.
  # Falhas de uma entidade não afetam as demais.
  use GenServer, restart: :transient

  alias NotificationSystem.Store

  def start_link(entity) do
    GenServer.start_link(__MODULE__, entity, name: via(entity))
  end

  def wake(entity) do
    GenServer.cast(via(entity), :wake)
  end

  defp via(entity), do: {:via, Registry, {NotificationSystem.Registry, entity}}

  @impl true
  def init(entity) do
    cfg = Store.config()
    send(self(), :drain)
    {:ok, Map.merge(cfg, %{entity: entity, timer: nil})}
  end

  @impl true
  def handle_cast(:wake, state) do
    send(self(), :drain)
    {:noreply, state}
  end

  @impl true
  def handle_info(:drain, state) do
    case Store.due(state.entity, now(), state.chunk) do
      # Nada vencido: agenda nova verificação (cobre retentativas com backoff).
      [] ->
        {:noreply, schedule(state)}

      # Processa um lote e continua drenando sem carregar o restante.
      batch ->
        Enum.each(batch, &attempt(&1, state))
        send(self(), :drain)
        {:noreply, state}
    end
  end

  # Tenta entregar uma notificação pendente e decide o próximo estado.
  defp attempt({{entity, id} = key, :pending, _at, attempts, payload}, state) do
    n = attempts + 1
    notif = %{id: id, entity: entity, payload: payload, attempt: n}

    {status, next_at} =
      case deliver(state.deliver, notif) do
        :ok ->
          {:delivered, 0}

        {:error, :permanent} ->
          {:failed, 0}

        # Falha temporária esgotou as tentativas.
        {:error, :transient} when n >= state.max_attempts ->
          {:failed, 0}

        # Falha temporária: nova tentativa com backoff exponencial e jitter.
        {:error, :transient} ->
          {:pending, now() + backoff(n, state)}
      end

    Store.update(key, status, next_at, n, payload)
  end

  defp attempt(_other, _state), do: :ok

  # Executa o callback isolando exceções: qualquer erro vira falha temporária.
  defp deliver(fun, notif) do
    case fun.(notif) do
      :ok -> :ok
      {:error, :permanent} -> {:error, :permanent}
      _ -> {:error, :transient}
    end
  rescue
    _ -> {:error, :transient}
  catch
    _, _ -> {:error, :transient}
  end

  defp backoff(n, cfg) do
    base = min(cfg.base_backoff * Integer.pow(2, n - 1), cfg.max_backoff)
    base + :rand.uniform(max(div(base, 4), 1))
  end

  defp schedule(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :drain, state.poll_interval)}
  end

  defp now, do: System.system_time(:millisecond)
end
