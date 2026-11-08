# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir limitar a quantidade de operações simultâneas.
# 2. Persistir os dados para recuperação após reinicializações.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Diferenciar erros de validação de falhas operacionais.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sensor_monitor.ex
#
# Monitoramento de sensores em arquivo único.
#
# Requisitos atendidos:
#  1. Limite de operações simultâneas: SensorMonitor.Limiter (semáforo com fila).
#  2. Persistência: SensorMonitor.Store grava snapshot em disco com escrita atômica
#     (arquivo temporário + rename) e recarrega na inicialização.
#  3. Notificações: eventos de transição de estado (alerta, recuperação, erro)
#     são publicados via Registry (modo :duplicate) para os processos inscritos.
#  4. Novas tentativas: falhas operacionais são repetidas com backoff exponencial.
#  5. Diferenciação de erros:
#       {:error, {:validation, motivo}}   -> dado ou configuração inválidos (não há retry)
#       {:error, {:operational, motivo}}  -> falha de leitura/infraestrutura (há retry)
#
# Exemplo de uso:
#   {:ok, _} = SensorMonitor.start_link(data_path: "sensors.bin", max_concurrency: 3)
#   {:ok, _} = SensorMonitor.subscribe()
#   {:ok, _} = SensorMonitor.register_sensor("temp-1", fn -> {:ok, 42.0} end,
#                min: 10, max: 40, interval: 500, max_attempts: 3, backoff_ms: 100)
#   receive do
#     {:sensor_monitor, event} -> IO.inspect(event)
#   end

defmodule SensorMonitor do
  @moduledoc """
  API pública e supervisor raiz do monitoramento de sensores.
  """

  use Supervisor
  alias SensorMonitor.{Limiter, Sensor, Store}

  @registry SensorMonitor.Registry
  @pubsub SensorMonitor.PubSub
  @sensor_sup SensorMonitor.SensorSup

  # ---------------------------------------------------------------------------
  # Supervisão
  # ---------------------------------------------------------------------------

  @doc """
  Inicia o sistema.

  Opções:
    * `:data_path` - caminho do arquivo de persistência (padrão: "sensor_monitor.bin")
    * `:max_concurrency` - máximo de operações de leitura simultâneas
  """
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registry único: nomes dos sensores (um processo por id).
      {Registry, keys: :unique, name: @registry},
      # Registry duplicado: assinantes de notificações.
      {Registry, keys: :duplicate, name: @pubsub},
      # Persistência carregada antes de qualquer sensor começar.
      {Store, path: Keyword.get(opts, :data_path, "sensor_monitor.bin")},
      # Limite de concorrência para operações de leitura.
      {Limiter, Keyword.get(opts, :max_concurrency, System.schedulers_online())},
      # Supervisor dinâmico dos sensores.
      {DynamicSupervisor, strategy: :one_for_one, name: @sensor_sup}
    ]

    # rest_for_one: se o Store cair, os processos que dependem dele são reiniciados.
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # API pública
  # ---------------------------------------------------------------------------

  @doc """
  Registra um sensor e inicia seu monitoramento periódico.

  `reader` é uma função de aridade 0 que deve retornar:
    * `{:ok, numero}`
    * `{:error, {:validation, motivo}}` - erro de dado, não será repetido
    * `{:error, {:operational, motivo}}` ou `{:error, motivo}` - falha temporária, será repetida

  Opções: `:min`, `:max` (limites; se omitidos, usa os persistidos),
  `:interval` (ms), `:max_attempts`, `:backoff_ms`.
  """
  def register_sensor(id, reader, opts \\ []) do
    with :ok <- validate_id(id),
         :ok <- validate_reader(reader),
         {:ok, cfg} <- build_config(id, opts) do
      args = Map.merge(cfg, %{id: id, reader: reader})

      case DynamicSupervisor.start_child(@sensor_sup, {Sensor, args}) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, _}} -> {:error, {:validation, :already_registered}}
        {:error, reason} -> {:error, {:operational, reason}}
      end
    end
  end

  @doc "Para o monitoramento de um sensor. O estado persistido é mantido."
  def unregister_sensor(id) do
    case Registry.lookup(@registry, {:sensor, id}) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(@sensor_sup, pid)
      [] -> {:error, {:validation, :not_found}}
    end
  end

  @doc "Inscreve o processo atual para receber mensagens `{:sensor_monitor, evento}`."
  def subscribe, do: Registry.register(@pubsub, :events, nil)

  @doc "Retorna o registro persistido de um sensor (último valor, alerta, erro, limites)."
  def status(id), do: Store.get(id)

  @doc false
  def publish(event) do
    Registry.dispatch(@pubsub, :events, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:sensor_monitor, event})
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Validações (erros de validação, nunca repetidos)
  # ---------------------------------------------------------------------------

  defp validate_id(id) when is_binary(id) or is_atom(id), do: :ok
  defp validate_id(_), do: {:error, {:validation, :invalid_id}}

  defp validate_reader(reader) when is_function(reader, 0), do: :ok
  defp validate_reader(_), do: {:error, {:validation, :invalid_reader}}

  defp build_config(id, opts) do
    persisted = Store.get(id) || %{}

    min = Keyword.get(opts, :min, Map.get(persisted, :min))
    max = Keyword.get(opts, :max, Map.get(persisted, :max))
    interval = Keyword.get(opts, :interval, 1_000)
    attempts = Keyword.get(opts, :max_attempts, 3)
    backoff = Keyword.get(opts, :backoff_ms, 100)

    cond do
      not (is_number(min) and is_number(max)) ->
        {:error, {:validation, :thresholds_required}}

      min > max ->
        {:error, {:validation, :min_greater_than_max}}

      not (is_integer(interval) and interval > 0) ->
        {:error, {:validation, :invalid_interval}}

      not (is_integer(attempts) and attempts > 0) ->
        {:error, {:validation, :invalid_max_attempts}}

      not (is_integer(backoff) and backoff >= 0) ->
        {:error, {:validation, :invalid_backoff}}

      true ->
        {:ok,
         %{
           min: min,
           max: max,
           interval: interval,
           max_attempts: attempts,
           backoff_ms: backoff
         }}
    end
  end
end

# ---------------------------------------------------------------------------
# Limitador de concorrência: semáforo com fila de espera.
# Monitora os processos detentores de permissão para liberar em caso de crash.
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Limiter do
  @moduledoc false
  use GenServer

  def start_link(max) when is_integer(max) and max > 0 do
    GenServer.start_link(__MODULE__, max, name: __MODULE__)
  end

  @doc """
  Executa `fun` ocupando uma permissão do semáforo.
  Bloqueia (sem timeout) enquanto o limite estiver atingido.
  A permissão é liberada mesmo se `fun` lançar exceção.
  """
  def run(fun) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      GenServer.cast(__MODULE__, {:release, self()})
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, from, state) do
    state = %{state | waiting: :queue.in(from, state.waiting)}
    {:noreply, grant(state)}
  end

  @impl true
  def handle_cast({:release, pid}, state) do
    {:noreply, grant(release(pid, state))}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, grant(release(pid, state))}
  end

  # Remove o detentor da permissão e desfaz o monitor.
  defp release(pid, state) do
    case Map.pop(state.active, pid) do
      {nil, _} ->
        state

      {ref, active} ->
        Process.demonitor(ref, [:flush])
        %{state | active: active}
    end
  end

  # Concede permissões aos próximos da fila enquanto houver vagas.
  defp grant(%{active: active, max: max, waiting: waiting} = state) do
    with true <- map_size(active) < max,
         {{:value, {pid, _} = from}, rest} <- :queue.out(waiting) do
      ref = Process.monitor(pid)
      GenServer.reply(from, :ok)
      grant(%{state | active: Map.put(active, pid, ref), waiting: rest})
    else
      _ -> state
    end
  end
end

# ---------------------------------------------------------------------------
# Persistência: snapshot do estado dos sensores em arquivo binário.
# A escrita é atômica (arquivo temporário + rename).
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Store do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def put(id, record) when is_map(record) do
    GenServer.call(__MODULE__, {:put, id, record})
  end

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :path)
    {:ok, %{path: path, data: load(path)}}
  end

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.data, id), state}
  end

  def handle_call({:put, id, record}, _from, state) do
    data = Map.put(state.data, id, record)

    case persist(state.path, data) do
      :ok ->
        {:reply, :ok, %{state | data: data}}

      {:error, reason} ->
        Logger.error("falha ao persistir dados: #{inspect(reason)}")
        {:reply, {:error, {:operational, reason}}, state}
    end
  end

  # Ausência do arquivo significa estado inicial vazio.
  # Arquivo ilegível é erro fatal de inicialização.
  defp load(path) do
    case File.read(path) do
      {:ok, bin} -> :erlang.binary_to_term(bin)
      {:error, :enoent} -> %{}
      {:error, reason} -> raise File.Error, reason: reason, action: "read", path: path
    end
  end

  defp persist(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end

# ---------------------------------------------------------------------------
# Worker de sensor: leitura periódica, retry com backoff, detecção de limites
# e publicação de notificações apenas nas transições de estado.
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Sensor do
  @moduledoc false
  use GenServer
  require Logger
  alias SensorMonitor.{Limiter, Store}

  def start_link(%{id: id} = args) do
    GenServer.start_link(__MODULE__, args,
      name: {:via, Registry, {SensorMonitor.Registry, {:sensor, id}}}
    )
  end

  @impl true
  def init(args) do
    persisted = Store.get(args.id) || %{}
    state = Map.put(args, :alert, Map.get(persisted, :alert))

    save(state, %{})
    send(self(), :poll)

    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    # A leitura (com retries) ocupa uma permissão do limitador.
    result =
      Limiter.run(fn ->
        with_retries(fn -> read(state.reader) end, 1, state.max_attempts, state.backoff_ms)
      end)

    state = handle_result(result, state)
    Process.send_after(self(), :poll, state.interval)
    {:noreply, state}
  end

  # Repete apenas falhas operacionais, com backoff exponencial.
  # Erros de validação são retornados imediatamente.
  defp with_retries(fun, attempt, max_attempts, backoff) do
    case fun.() do
      {:error, {:operational, _}} when attempt < max_attempts ->
        Process.sleep(backoff * Integer.pow(2, attempt - 1))
        with_retries(fun, attempt + 1, max_attempts, backoff)

      result ->
        result
    end
  end

  # Normaliza o retorno do leitor para {:ok, n} ou {:error, {kind, motivo}}.
  defp read(reader) do
    case invoke(reader) do
      {:ok, value} when is_number(value) -> {:ok, value}
      {:ok, value} -> {:error, {:validation, {:not_numeric, value}}}
      {:error, {:validation, _} = error} -> {:error, error}
      {:error, {:operational, _} = error} -> {:error, error}
      {:error, reason} -> {:error, {:operational, reason}}
      other -> {:error, {:operational, {:unexpected_return, other}}}
    end
  end

  # Exceções e saídas do leitor são falhas operacionais.
  defp invoke(reader) do
    reader.()
  rescue
    e -> {:error, {:operational, {:exception, Exception.message(e)}}}
  catch
    :exit, reason -> {:error, {:operational, {:exit, reason}}}
  end

  defp handle_result({:ok, value}, state) do
    alert = classify(value, state.min, state.max)
    notify_transition(state, alert, value)
    save(state, %{last_value: value, alert: alert, last_error: nil, updated_at: now()})
    %{state | alert: alert}
  end

  defp handle_result({:error, {kind, reason} = error}, state) do
    SensorMonitor.publish({:sensor_error, state.id, kind, reason})
    save(state, %{last_error: error, updated_at: now()})
    state
  end

  # Notifica somente quando o estado muda (evita spam a cada leitura).
  defp notify_transition(state, alert, value) do
    cond do
      alert == state.alert -> :ok
      alert == nil -> SensorMonitor.publish({:sensor_recovered, state.id, value})
      true -> SensorMonitor.publish({:sensor_alert, state.id, alert, value})
    end
  end

  defp classify(value, min, _max) when value < min, do: :low
  defp classify(value, _min, max) when value > max, do: :high
  defp classify(_value, _min, _max), do: nil

  defp save(state, changes) do
    current = Store.get(state.id) || %{}

    record =
      current
      |> Map.merge(%{min: state.min, max: state.max})
      |> Map.merge(changes)

    case Store.put(state.id, record) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("sensor #{inspect(state.id)}: falha ao salvar #{inspect(reason)}")
        :error
    end
  end

  defp now, do: System.system_time(:millisecond)
end
