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

# Monitoramento de sensores em Elixir (arquivo único, autocontido).
#
# Requisitos atendidos:
#   1. Limite de operações simultâneas: SensorMonitor.Limiter (semáforo com fila).
#   2. Persistência: SensorMonitor.Store grava o estado em disco (escrita atômica).
#   3. Notificações: eventos publicados via Registry (modo :duplicate) para assinantes.
#   4. Novas tentativas: backoff exponencial somente para falhas transitórias.
#   5. Diferenciação: erros de validação (dados inválidos) são separados de falhas
#      operacionais (leitura/infraestrutura), com tratamentos distintos.
#
# Uso (exemplo):
#   {:ok, _} = SensorMonitor.start_link(max_concurrency: 4, state_file: "estado.bin")
#   SensorMonitor.subscribe()
#   SensorMonitor.start_sensor("temp-1", %{
#     read: fn -> {:ok, 25.3} end,
#     interval: 1_000,
#     min: -10, max: 60,
#     alert_above: 30
#   })
#   # Mensagens chegam como {:sensor_monitor, evento} no processo assinante.

defmodule SensorMonitor do
  @moduledoc """
  Supervisor raiz e API pública do monitor de sensores.
  """
  use Supervisor

  @registry SensorMonitor.Registry
  @pubsub SensorMonitor.PubSub
  @task_sup SensorMonitor.TaskSupervisor
  @sensor_sup SensorMonitor.SensorSupervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    children = [
      # Registry único para localizar sensores pelo id (chaves únicas).
      {Registry, keys: :unique, name: @registry},
      # Registry de publicação/assinatura para notificações (chaves duplicadas).
      {Registry, keys: :duplicate, name: @pubsub},
      # Persistência: a ordem importa, pois os sensores leem o Store ao iniciar.
      {SensorMonitor.Store, Keyword.get(opts, :state_file, "sensor_monitor_state.bin")},
      # Limite global de operações simultâneas.
      {SensorMonitor.Limiter, Keyword.get(opts, :max_concurrency, System.schedulers_online())},
      # Supervisor das tarefas de leitura assíncronas.
      {Task.Supervisor, name: @task_sup},
      # Supervisor dinâmico dos processos de sensores.
      {DynamicSupervisor, name: @sensor_sup, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Inicia o monitoramento de um sensor."
  def start_sensor(id, config) do
    DynamicSupervisor.start_child(@sensor_sup, {SensorMonitor.Sensor, {id, config}})
  end

  @doc "Retorna o snapshot atual do sensor."
  def read(id), do: GenServer.call(via(id), :snapshot)

  @doc "Lista os ids dos sensores registrados."
  def sensors do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc "Nome de registro (via Registry) de um sensor."
  def via(id), do: {:via, Registry, {@registry, id}}

  @doc "Inscreve o processo chamador para receber notificações."
  def subscribe, do: Registry.register(@pubsub, :alerts, nil)

  @doc "Publica um evento para todos os assinantes."
  def notify(event) do
    Registry.dispatch(@pubsub, :alerts, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:sensor_monitor, event})
    end)
  end
end

defmodule SensorMonitor.Store do
  @moduledoc """
  Persistência do estado dos sensores em arquivo binário.
  A gravação é atômica (arquivo temporário + rename), evitando corrupção
  em caso de queda durante a escrita.
  """
  use GenServer

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  def get(id), do: GenServer.call(__MODULE__, {:get, id})

  def put(id, data), do: GenServer.call(__MODULE__, {:put, id, data})

  @impl true
  def init(path), do: {:ok, %{path: path, data: load(path)}}

  @impl true
  def handle_call({:get, id}, _from, state) do
    {:reply, Map.get(state.data, id), state}
  end

  def handle_call({:put, id, data}, _from, state) do
    new_data = Map.put(state.data, id, data)

    case persist(state.path, new_data) do
      :ok -> {:reply, :ok, %{state | data: new_data}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Carrega o estado salvo; se ausente ou corrompido, inicia vazio.
  defp load(path) do
    case File.read(path) do
      {:ok, bin} ->
        try do
          :erlang.binary_to_term(bin, [:safe])
        rescue
          ArgumentError -> %{}
        end

      {:error, _} ->
        %{}
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

defmodule SensorMonitor.Limiter do
  @moduledoc """
  Semáforo que limita a quantidade de operações simultâneas.
  Chamadas excedentes ficam em fila (FIFO). Se o processo detentor do
  slot morrer, o slot é liberado automaticamente via monitor.

  Não é reentrante: um mesmo processo não deve aninhar chamadas a `run/1`.
  """
  use GenServer

  def start_link(max), do: GenServer.start_link(__MODULE__, max, name: __MODULE__)

  @doc "Executa `fun` ocupando um slot de concorrência."
  def run(fun) when is_function(fun, 0) do
    :ok = GenServer.call(__MODULE__, :acquire, :infinity)

    try do
      fun.()
    after
      :ok = GenServer.call(__MODULE__, :release, :infinity)
    end
  end

  @impl true
  def init(max) do
    {:ok, %{max: max, active: %{}, waiting: :queue.new()}}
  end

  @impl true
  def handle_call(:acquire, from, state) do
    if map_size(state.active) < state.max do
      {:noreply, grant(from, state)}
    else
      {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
    end
  end

  def handle_call(:release, {pid, _}, state) do
    case Map.pop(state.active, pid) do
      {nil, _active} ->
        {:reply, :ok, state}

      {ref, active} ->
        Process.demonitor(ref, [:flush])
        {:reply, :ok, next(%{state | active: active})}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.fetch(state.active, pid) do
      {:ok, ^ref} -> {:noreply, next(%{state | active: Map.delete(state.active, pid)})}
      _ -> {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp grant({pid, _} = from, state) do
    ref = Process.monitor(pid)
    GenServer.reply(from, :ok)
    %{state | active: Map.put(state.active, pid, ref)}
  end

  # Concede slots livres a quem está aguardando na fila.
  defp next(state) do
    if map_size(state.active) < state.max do
      case :queue.out(state.waiting) do
        {{:value, from}, rest} -> next(grant(from, %{state | waiting: rest}))
        {:empty, _} -> state
      end
    else
      state
    end
  end
end

defmodule SensorMonitor.Sensor do
  @moduledoc """
  Processo de um sensor. Periodicamente executa uma leitura em uma Task,
  aplica tentativas com backoff para falhas transitórias, valida o valor,
  verifica limiares, persiste o estado e emite notificações.

  Configuração (mapa):
    * `:read`          - função `0-aridade` retornando `{:ok, valor}` ou
                         `{:error, {:transient | :operational | :validation, motivo}}`
    * `:interval`      - intervalo entre leituras (ms)
    * `:min` / `:max`  - faixa válida (validação)
    * `:alert_above` / `:alert_below` - limiares de notificação
    * `:max_failures`  - falhas operacionais consecutivas para alerta de saúde
    * `:max_attempts`  - tentativas totais para falhas transitórias
    * `:base_backoff`  - atraso inicial do backoff exponencial (ms)
  """
  use GenServer

  @defaults %{
    interval: 1_000,
    min: nil,
    max: nil,
    alert_above: nil,
    alert_below: nil,
    max_failures: 3,
    max_attempts: 3,
    base_backoff: 100
  }

  @persisted_keys [:last_value, :last_error, :failures, :breached]

  def start_link({id, config}) do
    GenServer.start_link(__MODULE__, {id, config}, name: SensorMonitor.via(id))
  end

  @impl true
  def init({id, config}) do
    config = Map.merge(@defaults, config)

    # Recuperação após reinicialização: restaura o estado persistido.
    stored = SensorMonitor.Store.get(id) || %{}

    state =
      %{id: id, config: config, task: nil, last_value: nil, last_error: nil,
        failures: 0, breached: nil}
      |> Map.merge(Map.take(stored, @persisted_keys))

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, Map.take(state, [:id | @persisted_keys]), state}
  end

  @impl true
  def handle_info(:poll, %{task: nil} = state) do
    config = state.config

    task =
      Task.Supervisor.async_nolink(SensorMonitor.TaskSupervisor, fn ->
        read_with_retries(config)
      end)

    {:noreply, %{state | task: task}}
  end

  # Resultado da Task.
  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = handle_result(result, %{state | task: nil})
    {:noreply, schedule(state)}
  end

  # Task terminou com falha inesperada (crash).
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    state = handle_result({:error, {:operational, {:crashed, reason}}}, %{state | task: nil})
    {:noreply, schedule(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Leitura com tentativas (executada dentro da Task)
  # ---------------------------------------------------------------------------

  defp read_with_retries(config), do: attempt(config, 1)

  defp attempt(config, n) do
    case SensorMonitor.Limiter.run(fn -> safe_read(config.read) end) do
      # Somente falhas transitórias são reexecutadas, com backoff exponencial.
      {:error, {:transient, _}} when n < config.max_attempts ->
        Process.sleep(config.base_backoff * Integer.pow(2, n - 1))
        attempt(config, n + 1)

      result ->
        result
    end
  end

  # Garante que o retorno da leitura respeite o contrato e converte exceções
  # em falhas operacionais.
  defp safe_read(fun) do
    case fun.() do
      {:ok, _} = ok ->
        ok

      {:error, {kind, _}} = err when kind in [:transient, :operational, :validation] ->
        err

      other ->
        {:error, {:operational, {:invalid_reader_return, other}}}
    end
  rescue
    e -> {:error, {:operational, {:exception, Exception.message(e)}}}
  end

  # ---------------------------------------------------------------------------
  # Tratamento do resultado
  # ---------------------------------------------------------------------------

  # Leitura bem-sucedida: valida o valor antes de aceitá-lo.
  defp handle_result({:ok, raw}, state) do
    case validate(raw, state.config) do
      {:ok, value} ->
        %{state | last_value: value, last_error: nil, failures: 0}
        |> check_thresholds()
        |> persist()

      {:error, reason} ->
        validation_failed(state, reason)
    end
  end

  # Erro de validação reportado pelo leitor: não conta como falha operacional.
  defp handle_result({:error, {:validation, reason}}, state) do
    validation_failed(state, reason)
  end

  # Falhas operacionais (inclui transitórias já esgotadas nas tentativas).
  defp handle_result({:error, {_kind, _} = reason}, state) do
    failures = state.failures + 1
    state = %{state | last_error: {:operational, reason}, failures: failures}

    SensorMonitor.notify({:operational_failure, state.id, reason})

    if failures == state.config.max_failures do
      SensorMonitor.notify({:sensor_unhealthy, state.id, failures})
    end

    persist(state)
  end

  defp validation_failed(state, reason) do
    SensorMonitor.notify({:validation_error, state.id, reason})
    persist(%{state | last_error: {:validation, reason}})
  end

  defp validate(value, config) do
    cond do
      not is_number(value) -> {:error, {:not_a_number, value}}
      is_number(config.min) and value < config.min -> {:error, {:below_min, value}}
      is_number(config.max) and value > config.max -> {:error, {:above_max, value}}
      true -> {:ok, value}
    end
  end

  # ---------------------------------------------------------------------------
  # Limiares e notificações
  # ---------------------------------------------------------------------------

  defp check_thresholds(state) do
    breached = breach(state.last_value, state.config)

    cond do
      breached == state.breached ->
        :noop

      breached ->
        SensorMonitor.notify({:threshold_breached, state.id, breached, state.last_value})

      true ->
        SensorMonitor.notify({:threshold_recovered, state.id, state.last_value})
    end

    %{state | breached: breached}
  end

  defp breach(value, %{alert_above: hi}) when is_number(hi) and value > hi, do: :above
  defp breach(value, %{alert_below: lo}) when is_number(lo) and value < lo, do: :below
  defp breach(_value, _config), do: nil

  # ---------------------------------------------------------------------------
  # Persistência e agendamento
  # ---------------------------------------------------------------------------

  defp persist(state) do
    case SensorMonitor.Store.put(state.id, Map.take(state, @persisted_keys)) do
      :ok -> state
      {:error, reason} ->
        SensorMonitor.notify({:persistence_error, state.id, reason})
        state
    end
  end

  defp schedule(state) do
    Process.send_after(self(), :poll, state.config.interval)
    state
  end
end
