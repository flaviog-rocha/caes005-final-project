# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Sistema de cache particionado por entidade, usando apenas a biblioteca padrão do Elixir/OTP.
#
# Requisito 1 (grandes volumes sem carga integral em memória):
#   - `load/3` consome um Enumerable em blocos (Stream.chunk_every), sem materializar a coleção.
#   - `stream/2` lê as entradas do ETS em lotes via :ets.select/3 com continuação,
#     devolvendo um Stream lazy.
#
# Requisito 2 (operações periódicas):
#   - Cada entidade executa uma limpeza incremental de entradas expiradas (um lote por tick).
#   - `schedule/3` registra jobs periódicos definidos pelo usuário, executados em tarefas separadas.
#
# Requisito 3 (entidades processadas de forma independente):
#   - Cada entidade é um processo GenServer próprio, com sua própria tabela ETS,
#     supervisionado por um DynamicSupervisor e localizado via Registry.
#   - `process_all/3` processa várias entidades em paralelo, isolando falhas por tarefa.

defmodule CacheSystem do
  # Supervisor raiz: Registry (localização das entidades), Task.Supervisor (tarefas)
  # e DynamicSupervisor (entidades criadas sob demanda).
  use Supervisor

  @registry CacheSystem.Registry
  @entity_sup CacheSystem.EntitySupervisor
  @task_sup CacheSystem.TaskSupervisor

  @default_chunk_size 1_000
  @default_stream_batch 500

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      {Task.Supervisor, name: @task_sup},
      {DynamicSupervisor, strategy: :one_for_one, name: @entity_sup}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  # Inicia explicitamente uma entidade com opções (ex.: sweep_interval).
  # Se a entidade já existir, retorna o pid existente sem alterar suas opções.
  def start_entity(entity, opts \\ []) do
    case DynamicSupervisor.start_child(@entity_sup, {CacheSystem.Entity, {entity, opts}}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_registered, pid}} -> {:ok, pid}
      other -> other
    end
  end

  # Insere uma entrada com TTL opcional (em milissegundos).
  def put(entity, key, value, ttl \\ :infinity) do
    put_many(entity, [{key, value}], ttl)
  end

  # Insere um lote de entradas em uma única chamada ao processo da entidade.
  def put_many(entity, entries, ttl \\ :infinity) do
    with {:ok, pid, _table} <- ensure(entity) do
      GenServer.call(pid, {:put_many, entries, expiry(ttl)}, :infinity)
    end
  end

  # Leitura direta na ETS (sem passar pelo processo), com verificação de expiração.
  def get(entity, key) do
    case lookup(entity) do
      {:ok, _pid, table} ->
        try do
          case :ets.lookup(table, key) do
            [{^key, value, expires_at}] ->
              if alive?(expires_at, now_ms()), do: {:ok, value}, else: :miss

            [] ->
              :miss
          end
        rescue
          # A tabela pode ter sido destruída se a entidade parou entre a busca e a leitura.
          ArgumentError -> {:error, :unknown_entity}
        end

      error ->
        error
    end
  end

  def delete(entity, key) do
    with {:ok, pid, _table} <- ensure(entity) do
      GenServer.call(pid, {:delete, key})
    end
  end

  # Carrega um Enumerable (ex.: File.stream!/1) em blocos de `chunk_size`.
  # Cada bloco é enviado à entidade e descartado da memória antes do próximo,
  # e a chamada síncrona funciona como backpressure natural.
  def load(entity, enumerable, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, :infinity)
    chunk_size = Keyword.get(opts, :chunk_size, @default_chunk_size)

    enumerable
    |> Stream.chunk_every(chunk_size)
    |> Stream.each(fn chunk -> :ok = put_many(entity, chunk, ttl) end)
    |> Stream.run()
  end

  # Retorna um Stream lazy com as entradas vivas {key, value} da entidade.
  # Lê a ETS em lotes via continuação, sem copiar a tabela inteira.
  def stream(entity, opts \\ []) do
    batch = Keyword.get(opts, :batch_size, @default_stream_batch)

    Stream.resource(
      fn ->
        case lookup(entity) do
          {:ok, _pid, table} ->
            {table, :ets.select(table, live_spec(now_ms()), batch)}

          {:error, reason} ->
            raise ArgumentError, "entidade #{inspect(entity)} indisponível: #{inspect(reason)}"
        end
      end,
      fn
        {_table, :"$end_of_table"} ->
          {:halt, nil}

        {table, {items, cont}} ->
          {items, {table, :ets.select(cont)}}
      end,
      fn _ -> :ok end
    )
  end

  # Registra uma operação periódica para a entidade.
  # `fun` recebe o stream das entradas vivas e roda em uma tarefa separada a cada tick.
  # Retorna {:ok, ref}, que pode ser usado em `cancel/2`.
  def schedule(entity, interval_ms, fun)
      when is_integer(interval_ms) and interval_ms > 0 and is_function(fun, 1) do
    with {:ok, pid, _table} <- ensure(entity) do
      GenServer.call(pid, {:schedule, interval_ms, fun})
    end
  end

  def cancel(entity, ref) do
    with {:ok, pid, _table} <- ensure(entity) do
      GenServer.call(pid, {:cancel, ref})
    end
  end

  # Processa várias entidades de forma independente e concorrente.
  # `fun` recebe (entidade, stream). Cada entidade roda em sua própria tarefa;
  # falhas são isoladas e retornadas como {:error, _} sem afetar as demais.
  def process_all(entities, fun, opts \\ []) when is_function(fun, 2) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    @task_sup
    |> Task.Supervisor.async_stream_nolink(
      entities,
      fn entity ->
        try do
          {entity, {:ok, fun.(entity, stream(entity))}}
        catch
          kind, reason -> {entity, {:error, {kind, reason}}}
        end
      end,
      max_concurrency: max,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, reason} -> {:unknown, {:error, reason}}
    end)
  end

  # ---------------------------------------------------------------------------
  # Auxiliares privados
  # ---------------------------------------------------------------------------

  # Garante que a entidade exista, criando-a sob demanda.
  defp ensure(entity) do
    case lookup(entity) do
      {:ok, _pid, _table} = found ->
        found

      {:error, _} ->
        with {:ok, _pid} <- start_entity(entity), do: lookup(entity)
    end
  end

  # Localiza o pid e a tabela ETS da entidade via Registry (o valor registrado é o tid).
  defp lookup(entity) do
    case Registry.lookup(@registry, entity) do
      [{pid, table}] -> {:ok, pid, table}
      [] -> {:error, :unknown_entity}
    end
  end

  # Spec de match: entradas sem expiração (:infinity) ou com expires_at > now.
  # Em match specs, átomos são maiores que números, mas o orelse já trata :infinity.
  defp live_spec(now) do
    [
      {
        {:"$1", :"$2", :"$3"},
        [{:orelse, {:==, :"$3", :infinity}, {:>, :"$3", now}}],
        [{{:"$1", :"$2"}}]
      }
    ]
  end

  defp alive?(:infinity, _now), do: true
  defp alive?(expires_at, now), do: expires_at > now

  defp expiry(:infinity), do: :infinity
  defp expiry(ttl) when is_integer(ttl) and ttl > 0, do: now_ms() + ttl

  defp now_ms, do: System.monotonic_time(:millisecond)
end

defmodule CacheSystem.Entity do
  # Processo que representa uma entidade do cache.
  # Possui sua própria tabela ETS (pública para leituras concorrentes),
  # serializa as escritas e executa limpeza e jobs periódicos.
  use GenServer, restart: :transient

  @default_sweep_interval 30_000
  @sweep_batch 200

  @registry CacheSystem.Registry
  @task_sup CacheSystem.TaskSupervisor

  def start_link({name, opts}) do
    GenServer.start_link(__MODULE__, {name, opts})
  end

  @impl true
  def init({name, opts}) do
    # A tabela pertence a este processo: quando ele termina, a tabela é removida.
    table = :ets.new(:cache_entity, [:set, :public, read_concurrency: true])

    # O tid é guardado como valor no Registry para que leituras não passem pelo processo.
    case Registry.register(@registry, name, table) do
      {:ok, _} ->
        interval = Keyword.get(opts, :sweep_interval, @default_sweep_interval)
        Process.send_after(self(), :sweep, interval)

        {:ok,
         %{
           name: name,
           table: table,
           sweep_interval: interval,
           sweep_cont: nil,
           jobs: %{}
         }}

      {:error, {:already_registered, pid}} ->
        # Evita duplicidade em corridas de criação; a tabela é descartada com o processo.
        {:stop, {:already_registered, pid}}
    end
  end

  @impl true
  def handle_call({:put_many, entries, expires_at}, _from, state) do
    rows = Enum.map(entries, fn {key, value} -> {key, value, expires_at} end)
    true = :ets.insert(state.table, rows)
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, state) do
    true = :ets.delete(state.table, key)
    {:reply, :ok, state}
  end

  def handle_call({:schedule, interval, fun}, _from, state) do
    ref = make_ref()
    Process.send_after(self(), {:job, ref}, interval)
    {:reply, {:ok, ref}, %{state | jobs: Map.put(state.jobs, ref, {interval, fun})}}
  end

  def handle_call({:cancel, ref}, _from, state) do
    # Timers já agendados são ignorados em handle_info quando o job não existe mais.
    {:reply, :ok, %{state | jobs: Map.delete(state.jobs, ref)}}
  end

  @impl true
  def handle_info(:sweep, state) do
    # Limpeza incremental: processa um lote por tick, mantendo a continuação entre ticks.
    cont = sweep_step(state.table, state.sweep_cont, now_ms())
    Process.send_after(self(), :sweep, state.sweep_interval)
    {:noreply, %{state | sweep_cont: cont}}
  end

  def handle_info({:job, ref}, state) do
    case Map.fetch(state.jobs, ref) do
      {:ok, {interval, fun}} ->
        name = state.name
        # O job roda em tarefa separada para não bloquear o processo da entidade.
        # Se o intervalo for menor que a duração do job, pode haver execuções sobrepostas.
        Task.Supervisor.start_child(@task_sup, fn -> fun.(CacheSystem.stream(name)) end)
        Process.send_after(self(), {:job, ref}, interval)
        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Limpeza incremental
  # ---------------------------------------------------------------------------

  # Início de um ciclo: seleciona expiradas em lote.
  defp sweep_step(table, nil, now) do
    table
    |> :ets.select(expired_spec(now), @sweep_batch)
    |> handle_sweep_chunk(table)
  end

  # Continuação de um ciclo em andamento.
  # Só são removidas chaves já visitadas pela continuação, então o percurso não é afetado.
  defp sweep_step(table, cont, _now) do
    handle_sweep_chunk(:ets.select(cont), table)
  end

  defp handle_sweep_chunk(:"$end_of_table", _table), do: nil

  defp handle_sweep_chunk({keys, cont}, table) do
    Enum.each(keys, &:ets.delete(table, &1))
    cont
  end

  # Entradas com expires_at < now. Átomo :infinity é maior que qualquer inteiro,
  # então entradas sem expiração nunca casam com esta condição.
  defp expired_spec(now) do
    [{{:"$1", :"$2", :"$3"}, [{:<, :"$3", now}], [:"$1"]}]
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
