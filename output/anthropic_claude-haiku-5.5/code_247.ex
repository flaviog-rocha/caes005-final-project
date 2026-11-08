# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de um sistema de cache, atendendo somente aos seguintes requisitos:
#
# 1. Processar grandes volumes de dados sem carregá-los integralmente na memória.
# 2. Executar determinadas operações periodicamente.
# 3. Permitir que diferentes entidades sejam processadas independentemente.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# cache_system.ex
# Arquivo único e autocontido. Executar com: elixir cache_system.ex
# ou carregar em um iex: c("cache_system.ex")

defmodule CacheSystem do
  @moduledoc """
  Sistema de cache particionado por entidade.

  * Cada entidade roda em um processo próprio (GenServer) com uma tabela ETS
    própria, o que permite processamento independente entre entidades.
  * Grandes volumes são consumidos e produzidos via `Stream` em lotes
    (`:batch_size`), sem carregar o conjunto inteiro na memória.
  * Operações periódicas (ex.: expurgo de itens expirados) são agendadas pelo
    `CacheSystem.Scheduler` e executadas em tasks isoladas.

  Exemplo:

      {:ok, _} = CacheSystem.start_link(evict_interval: 30_000)

      :ok = CacheSystem.put(:users, "1", %{nome: "Ana"}, ttl: 60_000)
      {:ok, %{nome: "Ana"}} = CacheSystem.get(:users, "1")

      CacheSystem.ingest(:produtos, stream, batch_size: 1_000)
      CacheSystem.ingest_file(:linhas, "dados.txt", batch_size: 1_000)
      CacheSystem.export(:produtos, batch_size: 500) |> Enum.take(10)

      CacheSystem.Scheduler.schedule(:relatorio, 10_000, fn -> IO.puts("tick") end)
  """

  use Supervisor

  @registry CacheSystem.Registry
  @entity_sup CacheSystem.EntitySupervisor
  @task_sup CacheSystem.TaskSupervisor

  @doc "Inicia a árvore de supervisão do sistema de cache."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registro de processos de entidades (nome -> processo + tabela ETS)
      {Registry, keys: :unique, name: @registry},
      # Supervisor dinâmico: uma entidade = um processo, criado sob demanda
      {DynamicSupervisor, strategy: :one_for_one, name: @entity_sup},
      # Tasks isoladas para operações periódicas
      {Task.Supervisor, name: @task_sup},
      # Agendador de operações periódicas
      {CacheSystem.Scheduler, opts}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública - operações por chave
  # ---------------------------------------------------------------------------

  @doc "Insere ou atualiza uma chave. Opções: `:ttl` em milissegundos (padrão `:infinity`)."
  def put(entity, key, value, opts \\ []) do
    CacheSystem.Entity.write(entity, [{key, value}], Keyword.get(opts, :ttl, :infinity))
  end

  @doc "Retorna `{:ok, valor}` ou `:error` (inexistente ou expirado)."
  def get(entity, key), do: CacheSystem.Entity.fetch(entity, key)

  @doc "Remove uma chave da entidade."
  def delete(entity, key), do: CacheSystem.Entity.delete(entity, key)

  @doc "Remove itens expirados da entidade e retorna a quantidade removida."
  def evict_expired(entity), do: CacheSystem.Entity.evict_expired(entity)

  @doc "Lista as entidades ativas."
  def list_entities do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  # ---------------------------------------------------------------------------
  # Processamento de grandes volumes (streaming com backpressure)
  # ---------------------------------------------------------------------------

  @doc """
  Consome um `Enumerable` de pares `{chave, valor}` de forma lazy, gravando em
  lotes de `:batch_size` (padrão 500). Cada lote é enviado de forma síncrona à
  entidade, o que aplica backpressure: a fonte só é lida quando o lote anterior
  foi gravado. Memória utilizada é proporcional ao lote, não ao volume total.
  """
  def ingest(entity, enumerable, opts \\ []) do
    batch = Keyword.get(opts, :batch_size, 500)
    ttl = Keyword.get(opts, :ttl, :infinity)

    enumerable
    |> Stream.chunk_every(batch)
    |> Stream.each(fn chunk -> CacheSystem.Entity.write(entity, chunk, ttl) end)
    |> Stream.run()
  end

  @doc """
  Ingere um arquivo linha a linha no formato `chave<TAB>valor`.
  O arquivo é lido por `File.stream!/1`, sem carregá-lo inteiro na memória.
  Linhas malformadas são descartadas.
  """
  def ingest_file(entity, path, opts \\ []) do
    stream =
      path
      |> File.stream!()
      |> Stream.map(&parse_line/1)
      |> Stream.reject(&is_nil/1)

    ingest(entity, stream, opts)
  end

  @doc """
  Processa várias fontes independentes em paralelo. `sources` é uma lista de
  `{entidade, enumerable}`. Cada entidade é processada em sua própria task,
  com concorrência limitada por `:max_concurrency`.
  """
  def ingest_many(sources, opts \\ []) do
    concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    sources
    |> Task.async_stream(
      fn {entity, enumerable} -> ingest(entity, enumerable, opts) end,
      max_concurrency: concurrency,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()
  end

  @doc """
  Retorna um `Stream` lazy com os pares `{chave, valor}` válidos da entidade.
  Os dados são lidos da ETS em lotes via `:ets.select/3` com continuação.
  A leitura é fracamente consistente: alterações concorrentes podem ou não
  aparecer no resultado, mas nunca há erro nem carga total em memória.
  """
  def export(entity, opts \\ []) do
    CacheSystem.Entity.stream(entity, Keyword.get(opts, :batch_size, 500))
  end

  defp parse_line(line) do
    case String.split(String.trim_trailing(line, "\n"), "\t", parts: 2) do
      [key, value] -> {key, value}
      _ -> nil
    end
  end
end

defmodule CacheSystem.Entity do
  @moduledoc false
  # Um processo por entidade. Escritas passam pelo GenServer (serializadas
  # por entidade); leituras acessam a ETS diretamente (tabela :protected).

  use GenServer

  @registry CacheSystem.Registry
  @entity_sup CacheSystem.EntitySupervisor

  def start_link(entity) do
    GenServer.start_link(__MODULE__, entity, name: {:via, Registry, {@registry, entity}})
  end

  @doc "Garante que o processo da entidade exista e retorna `{:ok, pid}`."
  def ensure(entity) do
    case lookup(entity) do
      {:ok, pid, _tab} ->
        {:ok, pid}

      :error ->
        case DynamicSupervisor.start_child(@entity_sup, {__MODULE__, entity}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          error -> error
        end
    end
  end

  @doc "Grava um lote de pares `{chave, valor}` com o TTL informado."
  def write(entity, pairs, ttl) do
    {:ok, pid} = ensure(entity)
    GenServer.call(pid, {:write, pairs, ttl}, :infinity)
  end

  @doc "Lê uma chave, descartando itens expirados (expiração preguiçosa)."
  def fetch(entity, key) do
    case lookup(entity) do
      {:ok, _pid, tab} ->
        case :ets.lookup(tab, key) do
          [{^key, value, exp}] ->
            if alive?(exp, now()), do: {:ok, value}, else: :error

          [] ->
            :error
        end

      :error ->
        :error
    end
  end

  @doc "Remove uma chave."
  def delete(entity, key) do
    {:ok, pid} = ensure(entity)
    GenServer.call(pid, {:delete, key})
  end

  @doc "Expurga itens expirados desta entidade."
  def evict_expired(entity) do
    case lookup(entity) do
      {:ok, pid, _tab} -> GenServer.call(pid, :evict_expired, :infinity)
      :error -> 0
    end
  end

  @doc "Stream lazy de pares válidos, lidos em lotes da ETS."
  def stream(entity, limit) do
    Stream.resource(
      fn ->
        case lookup(entity) do
          {:ok, _pid, tab} -> {:select, tab, export_spec(now())}
          :error -> :done
        end
      end,
      fn
        :done -> {:halt, :done}
        {:select, tab, ms} -> tab |> :ets.select(ms, limit) |> next_batch()
        {:cont, cont} -> cont |> :ets.select() |> next_batch()
      end,
      fn _ -> :ok end
    )
  end

  def now, do: System.monotonic_time(:millisecond)

  # -- GenServer -------------------------------------------------------------

  @impl true
  def init(entity) do
    tab = :ets.new(:cache_entity, [:set, :protected, read_concurrency: true])
    # Publica a referência da tabela no Registry para leituras diretas
    Registry.update_value(@registry, entity, fn _ -> tab end)
    {:ok, %{tab: tab}}
  end

  @impl true
  def handle_call({:write, pairs, ttl}, _from, %{tab: tab} = state) do
    exp = expiry(ttl)
    :ets.insert(tab, Enum.map(pairs, fn {k, v} -> {k, v, exp} end))
    {:reply, :ok, state}
  end

  def handle_call({:delete, key}, _from, %{tab: tab} = state) do
    :ets.delete(tab, key)
    {:reply, :ok, state}
  end

  def handle_call(:evict_expired, _from, %{tab: tab} = state) do
    t = now()

    removed =
      :ets.select_delete(tab, [
        {{:_, :_, :"$1"}, [{:is_integer, :"$1"}, {:<, :"$1", t}], [true]}
      ])

    {:reply, removed, state}
  end

  # -- Helpers privados ------------------------------------------------------

  defp lookup(entity) do
    case Registry.lookup(@registry, entity) do
      [{pid, tab}] when is_reference(tab) -> {:ok, pid, tab}
      _ -> :error
    end
  end

  defp expiry(:infinity), do: :infinity
  defp expiry(ttl) when is_integer(ttl) and ttl > 0, do: now() + ttl

  defp alive?(:infinity, _t), do: true
  defp alive?(exp, t), do: exp > t

  defp export_spec(t) do
    [
      {{:"$1", :"$2", :"$3"},
       [{:orelse, {:is_atom, :"$3"}, {:>, :"$3", t}}],
       [{{:"$1", :"$2"}}]}
    ]
  end

  defp next_batch(:"$end_of_table"), do: {:halt, :done}
  defp next_batch({rows, cont}), do: {rows, {:cont, cont}}
end

defmodule CacheSystem.Scheduler do
  @moduledoc """
  Agendador de operações periódicas. Cada job é uma função de aridade 0
  executada em uma Task isolada: falhas não derrubam o agendador.
  """

  use GenServer

  @task_sup CacheSystem.TaskSupervisor

  @doc "Agenda (ou reagenda) um job `name` a cada `interval` milissegundos."
  def schedule(name, interval, fun)
      when is_integer(interval) and interval > 0 and is_function(fun, 0) do
    GenServer.call(__MODULE__, {:schedule, name, interval, fun})
  end

  @doc "Cancela um job agendado."
  def cancel(name), do: GenServer.call(__MODULE__, {:cancel, name})

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    evict_interval = Keyword.get(opts, :evict_interval, 60_000)
    state = %{jobs: %{}}
    # Job padrão: expurgo periódico de itens expirados em todas as entidades
    {:ok, put_job(state, :evict_expired, evict_interval, &evict_all/0)}
  end

  @impl true
  def handle_call({:schedule, name, interval, fun}, _from, state) do
    {:reply, :ok, put_job(state, name, interval, fun)}
  end

  def handle_call({:cancel, name}, _from, state) do
    {:reply, :ok, cancel_job(state, name)}
  end

  @impl true
  def handle_info({:tick, name}, state) do
    case Map.get(state.jobs, name) do
      nil ->
        {:noreply, state}

      %{fun: fun, interval: interval} ->
        # Executa de forma assíncrona e isolada, sem bloquear o agendador
        Task.Supervisor.start_child(@task_sup, fun)
        {:noreply, put_job(state, name, interval, fun)}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Helpers privados ------------------------------------------------------

  defp put_job(state, name, interval, fun) do
    state = cancel_job(state, name)
    ref = Process.send_after(self(), {:tick, name}, interval)
    %{state | jobs: Map.put(state.jobs, name, %{interval: interval, fun: fun, ref: ref})}
  end

  defp cancel_job(state, name) do
    case Map.pop(state.jobs, name) do
      {nil, _jobs} ->
        state

      {%{ref: ref}, jobs} ->
        Process.cancel_timer(ref)
        %{state | jobs: jobs}
    end
  end

  defp evict_all do
    Enum.each(CacheSystem.list_entities(), &CacheSystem.evict_expired/1)
  end
end
