# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: delivery_tracker.exs
# Gerenciamento de entregas e rastreamento com macros, histórico,
# consulta de estado, processamento independente por entidade e
# limite de operações simultâneas. Usa apenas a biblioteca padrão e o Logger.

defmodule DeliveryTracker.Steps do
  @moduledoc """
  Macros para declarar operações (passos) rastreáveis de entregas.

  Uso:

      defmodule Entregas do
        use DeliveryTracker.Steps

        defstep coletar(pedido_id) do
          tracked "coleta" do
            Process.sleep(200)
            :coletado
          end
        end
      end

  `defstep` gera uma função que retorna `{nome_da_operacao, fun}`,
  pronta para ser submetida ao `DeliveryTracker.Manager`.
  """

  # Injeta as macros no módulo chamador e registra os passos declarados.
  defmacro __using__(_opts) do
    quote do
      import DeliveryTracker.Steps, only: [defstep: 2, tracked: 2]
      require Logger
      Module.register_attribute(__MODULE__, :delivery_steps, accumulate: true)
      @before_compile DeliveryTracker.Steps
    end
  end

  # Gera a função __steps__/0 com a lista de passos declarados no módulo.
  defmacro __before_compile__(env) do
    steps =
      env.module
      |> Module.get_attribute(:delivery_steps)
      |> Enum.reverse()

    quote do
      @doc false
      def __steps__, do: unquote(Macro.escape(steps))
    end
  end

  # Declara um passo. A função gerada devolve {nome, fun}, onde `fun`
  # encapsula o corpo do passo para ser executado posteriormente.
  defmacro defstep(call, do: body) do
    {name, _meta, args} = call
    arity = length(args || [])

    quote do
      @delivery_steps {unquote(name), unquote(arity)}

      def unquote(call) do
        {unquote(name), fn -> unquote(body) end}
      end
    end
  end

  # Envolve um bloco de código com logs de início, fim e duração.
  defmacro tracked(label, do: block) do
    quote do
      __label__ = unquote(label)
      inicio = System.monotonic_time(:millisecond)
      Logger.debug("[passo] #{__label__} iniciado")

      resultado = unquote(block)

      duracao = System.monotonic_time(:millisecond) - inicio
      Logger.debug("[passo] #{__label__} concluído em #{duracao} ms")
      resultado
    end
  end
end

defmodule DeliveryTracker.Manager do
  @moduledoc """
  GenServer responsável por:

    * enfileirar e executar operações de forma assíncrona;
    * limitar o número de operações simultâneas (`:max_concurrency`);
    * processar entidades de forma independente: operações da mesma
      entidade são executadas em ordem (uma por vez), enquanto entidades
      diferentes podem progredir em paralelo;
    * manter o estado atual de cada operação e um histórico de eventos.
  """

  use GenServer
  require Logger

  @type entity :: term()
  @type op_id :: pos_integer()
  @type status :: :queued | :running | :done | :failed

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o gerenciador.

  Opções:
    * `:name` - nome para registro do processo;
    * `:max_concurrency` - máximo de operações executando ao mesmo tempo
      (padrão: número de schedulers online).
  """
  def start_link(opts \\ []) do
    {gen_opts, init_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, init_opts, gen_opts)
  end

  @doc """
  Submete uma operação (tupla `{nome, fun}`, como retornada pelos passos)
  associada a uma entidade. Retorna `{:ok, id}` imediatamente.
  """
  def submit(server, entity, {op, fun}) when is_function(fun, 0) do
    GenServer.call(server, {:submit, entity, op, fun})
  end

  @doc "Consulta o estado atual de uma operação pelo id."
  def status(server, id), do: GenServer.call(server, {:status, id})

  @doc """
  Lista as operações em andamento (enfileiradas ou executando).
  Pode ser filtrada por entidade passando o segundo argumento.
  """
  def in_progress(server, entity \\ :all) do
    GenServer.call(server, {:in_progress, entity})
  end

  @doc """
  Retorna o histórico de eventos, do mais recente para o mais antigo.

  Opções:
    * `:entity` - filtra eventos de uma entidade;
    * `:limit` - quantidade máxima de eventos retornados.
  """
  def history(server, opts \\ []), do: GenServer.call(server, {:history, opts})

  # ---------------------------------------------------------------------------
  # Callbacks do GenServer
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    max = Keyword.get(opts, :max_concurrency, System.schedulers_online())

    if not (is_integer(max) and max > 0) do
      raise ArgumentError, "max_concurrency deve ser um inteiro positivo"
    end

    state = %{
      max: max,
      seq: 0,
      # id => registro da operação (estado atual)
      ops: %{},
      # id => função a ser executada (removida após conclusão)
      jobs: %{},
      # ids aguardando execução, em ordem FIFO
      pending: [],
      # task_ref => id, para operações em execução
      running: %{},
      # entidades que possuem operação em execução
      busy: MapSet.new(),
      # eventos em ordem decrescente (mais recente primeiro)
      history: []
    }

    Logger.info("DeliveryTracker iniciado com max_concurrency=#{max}")
    {:ok, state}
  end

  @impl true
  def handle_call({:submit, entity, op, fun}, _from, state) do
    id = state.seq + 1
    rec = new_record(id, entity, op)

    state =
      %{state | seq: id, jobs: Map.put(state.jobs, id, fun), pending: state.pending ++ [id]}
      |> put_op(rec)
      |> record(rec, :submitted)

    Logger.info("Operação #{id} (#{op}) enfileirada para entidade #{inspect(entity)}")
    {:reply, {:ok, id}, schedule(state)}
  end

  def handle_call({:status, id}, _from, state) do
    case Map.fetch(state.ops, id) do
      {:ok, rec} -> {:reply, {:ok, rec}, state}
      :error -> {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:in_progress, entity}, _from, state) do
    list =
      state.ops
      |> Map.values()
      |> Enum.filter(&(&1.status in [:queued, :running]))
      |> Enum.filter(&(entity == :all or &1.entity == entity))
      |> Enum.sort_by(& &1.id)

    {:reply, list, state}
  end

  def handle_call({:history, opts}, _from, state) do
    entity = Keyword.get(opts, :entity, :all)
    limit = Keyword.get(opts, :limit, :infinity)

    events =
      state.history
      |> Enum.filter(&(entity == :all or &1.entity == entity))
      |> maybe_take(limit)

    {:reply, events, state}
  end

  @impl true
  # Resultado de uma tarefa concluída (Task.async envia {ref, resultado}).
  def handle_info({ref, outcome}, state) when is_reference(ref) do
    case Map.pop(state.running, ref) do
      {nil, _} ->
        {:noreply, state}

      {id, running} ->
        Process.demonitor(ref, [:flush])

        state =
          %{state | running: running}
          |> finish(id, outcome)
          |> schedule()

        {:noreply, state}
    end
  end

  # Demais mensagens (ex.: :DOWN já tratados) são ignoradas.
  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Funções privadas
  # ---------------------------------------------------------------------------

  # Inicia operações enquanto houver capacidade e itens elegíveis.
  defp schedule(state) do
    if map_size(state.running) < state.max do
      case Enum.find(state.pending, &eligible?(state, &1)) do
        nil -> state
        id -> state |> start(id) |> schedule()
      end
    else
      state
    end
  end

  # Uma operação é elegível se sua entidade não possui outra em execução.
  defp eligible?(state, id) do
    %{entity: entity} = state.ops[id]
    not MapSet.member?(state.busy, entity)
  end

  defp start(state, id) do
    fun = state.jobs[id]
    rec = %{state.ops[id] | status: :running, started_at: DateTime.utc_now()}

    task = Task.async(fn -> execute(fun) end)

    Logger.info("Operação #{id} (#{rec.op}) iniciada para entidade #{inspect(rec.entity)}")

    state
    |> put_op(rec)
    |> Map.update!(:pending, &List.delete(&1, id))
    |> Map.update!(:running, &Map.put(&1, task.ref, id))
    |> Map.update!(:busy, &MapSet.put(&1, rec.entity))
    |> record(rec, :started)
  end

  # Executa a função capturando exceções, para que falhas não derrubem o gerenciador.
  defp execute(fun) do
    try do
      {:ok, fun.()}
    rescue
      e -> {:error, Exception.format_banner(:error, e)}
    catch
      kind, reason -> {:error, Exception.format_banner(kind, reason)}
    end
  end

  defp finish(state, id, outcome) do
    rec = state.ops[id]

    {status, result} =
      case outcome do
        {:ok, value} -> {:done, value}
        {:error, reason} -> {:failed, reason}
      end

    rec = %{rec | status: status, result: result, finished_at: DateTime.utc_now()}
    level = if status == :done, do: :info, else: :error

    Logger.log(level, "Operação #{id} (#{rec.op}) #{status}: #{inspect(result)}")

    state
    |> put_op(rec)
    |> Map.update!(:busy, &MapSet.delete(&1, rec.entity))
    |> Map.update!(:jobs, &Map.delete(&1, id))
    |> record(rec, status)
  end

  defp new_record(id, entity, op) do
    %{
      id: id,
      entity: entity,
      op: op,
      status: :queued,
      submitted_at: DateTime.utc_now(),
      started_at: nil,
      finished_at: nil,
      result: nil
    }
  end

  defp put_op(state, rec), do: %{state | ops: Map.put(state.ops, rec.id, rec)}

  defp record(state, rec, event) do
    entry = %{
      at: DateTime.utc_now(),
      id: rec.id,
      entity: rec.entity,
      op: rec.op,
      event: event
    }

    %{state | history: [entry | state.history]}
  end

  defp maybe_take(list, :infinity), do: list
  defp maybe_take(list, limit) when is_integer(limit), do: Enum.take(list, limit)
end
