# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Impedir atualizações parcialmente concluídas.
# 3. Diferenciar erros de validação de falhas operacionais.
# 4. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
# 5. Fazer uso de macros.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Registry
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Arquivo único: monitoramento de sensores com Registry, supervisão, detecção de
# encerramentos inesperados, transações all-or-nothing, erros tipados e macros.
#
# Execução: elixir sensor_monitor.exs

# ---------------------------------------------------------------------------
# Macros utilitárias
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Rules do
  @moduledoc """
  Macros usadas para declarar regras de validação e para classificar falhas
  operacionais (exceções e saídas de processos) de forma uniforme.
  """

  # Declara uma regra de validação como cláusula privada `check_rule/2`.
  # O predicado recebe a variável `value` e, se for falso, gera
  # `{:error, {campo, mensagem}}`. Trata-se de erro de VALIDAÇÃO (entrada inválida).
  defmacro defrule(field, message, do: predicate) do
    # Macro.var/2 com contexto nil faz a variável `value` casar com a do chamador
    value = Macro.var(:value, nil)

    quote do
      defp check_rule(unquote(field), unquote(value)) do
        if unquote(predicate),
          do: :ok,
          else: {:error, {unquote(field), unquote(message)}}
      end
    end
  end

  # Executa um bloco convertendo exceções e saídas (exit) em erro OPERACIONAL.
  # Falhas operacionais não são culpa da entrada do usuário e podem ser reexecutadas.
  defmacro safely(do: block) do
    quote do
      try do
        unquote(block)
      rescue
        exception ->
          {:error, {:operational, {:exception, Exception.message(exception)}}}
      catch
        :exit, reason ->
          {:error, {:operational, {:exit, reason}}}
      end
    end
  end
end

# ---------------------------------------------------------------------------
# Validação (erros de validação, separados de falhas operacionais)
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Validator do
  @moduledoc """
  Valida atributos de sensores sem efeitos colaterais. Retorna
  `{:ok, attrs}` ou `{:error, {:validation, erros}}`.
  """
  import SensorMonitor.Rules, only: [defrule: 3]

  defrule :value, "must be a number between -100 and 1000",
    do: is_number(value) and value >= -100 and value <= 1000

  defrule :unit, "must be celsius or percent",
    do: value in ["celsius", "percent"]

  # Catch-all: campos desconhecidos também são erros de validação
  defp check_rule(field, _value), do: {:error, {field, "unknown field"}}

  @doc "Valida um mapa de atributos e acumula todos os erros encontrados."
  def validate(attrs) when is_map(attrs) do
    results = Enum.map(attrs, fn {field, value} -> check_rule(field, value) end)

    case for({:error, _} = error <- results, do: error) do
      [] -> {:ok, attrs}
      errors -> {:error, {:validation, errors}}
    end
  end

  def validate(_other), do: {:error, {:validation, [{:attrs, "must be a map"}]}}
end

# ---------------------------------------------------------------------------
# Banco de dados fictício (GenServer com transações atômicas)
# ---------------------------------------------------------------------------
defmodule SensorMonitor.FakeDB do
  @moduledoc """
  Banco fictício em memória. Cada transação recebe uma cópia dos dados,
  produz um novo estado e só o confirma se tudo der certo. Se a função
  falhar (erro, exceção ou saída), o estado anterior é preservado, impedindo
  atualizações parcialmente concluídas.

  Não depende de Ecto. Um adaptador Ecto.Repo poderia substituí-lo, usando
  `Ecto.Multi` para a mesma garantia all-or-nothing.
  """
  use GenServer
  import SensorMonitor.Rules, only: [safely: 1]

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @doc """
  Executa `fun.(dados)`, que deve retornar `{:ok, novos_dados, resultado}`
  ou `{:error, motivo}`.
  """
  def transaction(fun) when is_function(fun, 1),
    do: GenServer.call(__MODULE__, {:transaction, fun})

  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})

  @doc "Simula uma falha operacional na próxima transação (uso em testes)."
  def inject_failure(reason), do: GenServer.call(__MODULE__, {:inject_failure, reason})

  @impl true
  def init(_arg) do
    {:ok, %{data: %{sensors: %{}, history: []}, fail_next: nil}}
  end

  @impl true
  def handle_call({:transaction, _fun}, _from, %{fail_next: reason} = state)
      when not is_nil(reason) do
    {:reply, {:error, {:operational, reason}}, %{state | fail_next: nil}}
  end

  def handle_call({:transaction, fun}, _from, state) do
    case stage(fun, state.data) do
      {:ok, new_data, result} ->
        # Commit somente em caso de sucesso
        {:reply, {:ok, result}, %{state | data: new_data}}

      {:error, _reason} = error ->
        # Rollback implícito: o estado anterior permanece intacto
        {:reply, error, state}
    end
  end

  def handle_call({:fetch, id}, _from, state) do
    reply =
      case Map.fetch(state.data.sensors, id) do
        {:ok, record} -> {:ok, record}
        :error -> {:error, {:operational, :not_found}}
      end

    {:reply, reply, state}
  end

  def handle_call({:inject_failure, reason}, _from, state) do
    {:reply, :ok, %{state | fail_next: reason}}
  end

  # Executa a função de staging de forma isolada
  defp stage(fun, data) do
    safely do
      case fun.(data) do
        {:ok, _new_data, _result} = ok -> ok
        {:error, _reason} = error -> error
        other -> {:error, {:operational, {:invalid_transaction_result, other}}}
      end
    end
  end
end

# ---------------------------------------------------------------------------
# Processo de sensor (registrado no Registry)
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Sensor do
  @moduledoc """
  Processo que representa um sensor. É registrado no `Registry` pelo id,
  e o `restart: :temporary` deixa a reinicialização a cargo do `Watchdog`,
  que consegue distinguir saídas esperadas de inesperadas.
  """
  use GenServer, restart: :temporary
  import SensorMonitor.Rules, only: [safely: 1]
  alias SensorMonitor.{FakeDB, Validator}

  @registry :sensor_monitor_registry

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))

  defp via(id), do: {:via, Registry, {@registry, id}}

  @doc "Atualiza o sensor. `expected_version` habilita controle de concorrência otimista."
  def update(id, attrs, expected_version \\ :any) do
    with_pid(id, fn pid ->
      safely(do: GenServer.call(pid, {:update, attrs, expected_version}))
    end)
  end

  @doc "Provoca uma falha não tratada no processo (uso em testes de monitoramento)."
  def crash(id), do: with_pid(id, fn pid -> GenServer.cast(pid, :crash) end)

  defp with_pid(id, fun) do
    case Registry.lookup(@registry, id) do
      [{pid, _value}] -> fun.(pid)
      [] -> {:error, {:operational, :sensor_not_found}}
    end
  end

  @impl true
  def init(id), do: {:ok, %{id: id}}

  @impl true
  def handle_call({:update, attrs, expected}, _from, %{id: id} = state) do
    # 1) Validação (sem efeitos colaterais)
    # 2) Persistência atômica (tudo ou nada)
    reply =
      with {:ok, changes} <- Validator.validate(attrs),
           {:ok, record} <- persist(id, changes, expected) do
        {:ok, record}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_cast(:crash, _state), do: raise("falha simulada no sensor")

  defp persist(id, changes, expected) do
    safely do
      FakeDB.transaction(fn data -> stage_update(data, id, changes, expected) end)
    end
  end

  # Monta o novo estado do sensor e o histórico em uma única transação:
  # ou ambos são gravados, ou nenhum.
  defp stage_update(data, id, changes, expected) do
    current = Map.get(data.sensors, id, %{value: nil, unit: nil, version: 0})

    if expected != :any and expected != current.version do
      {:error, {:operational, {:conflict, current.version}}}
    else
      record =
        current
        |> Map.merge(changes)
        |> Map.put(:version, current.version + 1)

      new_data = %{
        data
        | sensors: Map.put(data.sensors, id, record),
          history: [{id, record.version, changes} | data.history]
      }

      {:ok, new_data, record}
    end
  end
end

# ---------------------------------------------------------------------------
# Watchdog: monitora os sensores e detecta encerramentos inesperados
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Watchdog do
  @moduledoc """
  Monitora os processos de sensor com `Process.monitor/1`. Ao receber `:DOWN`
  com motivo inesperado, registra o evento e reinicia o sensor. Saídas
  esperadas (`:normal`, `:shutdown`, `{:shutdown, _}`) não são tratadas como falha.
  """
  use GenServer
  alias SensorMonitor.Sensor

  @sup SensorMonitor.SensorSup

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  def watch(id), do: GenServer.call(__MODULE__, {:watch, id})
  def unwatch(id), do: GenServer.call(__MODULE__, {:unwatch, id})
  def unexpected_exits, do: GenServer.call(__MODULE__, :unexpected_exits)

  @impl true
  def init(_arg), do: {:ok, %{refs: %{}, exits: []}}

  @impl true
  def handle_call({:watch, id}, _from, state) do
    case spawn_monitored(id, state) do
      {:ok, new_state} -> {:reply, :ok, new_state}
      {:error, reason} -> {:reply, {:error, {:operational, reason}}, state}
    end
  end

  def handle_call({:unwatch, id}, _from, state) do
    case Enum.find(state.refs, fn {_ref, {sid, _pid}} -> sid == id end) do
      nil ->
        {:reply, {:error, {:operational, :not_watched}}, state}

      {ref, {_sid, pid}} ->
        # Remove o monitor antes de encerrar, para que a saída seja esperada
        Process.demonitor(ref, [:flush])
        DynamicSupervisor.terminate_child(@sup, pid)
        {:reply, :ok, %{state | refs: Map.delete(state.refs, ref)}}
    end
  end

  def handle_call(:unexpected_exits, _from, state) do
    {:reply, Enum.reverse(state.exits), state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.refs, ref) do
      {nil, _refs} ->
        {:noreply, state}

      {{id, ^pid}, refs} ->
        state = %{state | refs: refs}

        if expected_exit?(reason) do
          {:noreply, state}
        else
          event = %{id: id, pid: pid, reason: reason, at: System.system_time(:millisecond)}
          state = %{state | exits: [event | state.exits]}

          case spawn_monitored(id, state) do
            {:ok, new_state} -> {:noreply, new_state}
            {:error, _reason} -> {:noreply, state}
          end
        end
    end
  end

  defp spawn_monitored(id, state) do
    case DynamicSupervisor.start_child(@sup, {Sensor, id}) do
      {:ok, pid} ->
        ref = Process.monitor(pid)
        {:ok, %{state | refs: Map.put(state.refs, ref, {id, pid})}}

      {:error, reason} ->
        {:error, reason}

      :ignore ->
        {:error, :ignored}
    end
  end

  defp expected_exit?(:normal), do: true
  defp expected_exit?(:shutdown), do: true
  defp expected_exit?({:shutdown, _}), do: true
  defp expected_exit?(_reason), do: false
end

# ---------------------------------------------------------------------------
# API pública e supervisão
# ---------------------------------------------------------------------------
defmodule SensorMonitor do
  @moduledoc """
  Ponto de entrada do sistema. Inicia o Registry, o banco fictício, o
  DynamicSupervisor de sensores e o Watchdog.
  """
  use Supervisor

  alias SensorMonitor.{Sensor, Watchdog, FakeDB}

  @registry :sensor_monitor_registry

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  def add_sensor(id), do: Watchdog.watch(id)
  def remove_sensor(id), do: Watchdog.unwatch(id)

  def update(id, attrs, expected_version \\ :any),
    do: Sensor.update(id, attrs, expected_version)

  def read(id), do: FakeDB.fetch(id)
  def crash(id), do: Sensor.crash(id)
  def unexpected_exits, do: Watchdog.unexpected_exits()

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry},
      FakeDB,
      {DynamicSupervisor, name: SensorMonitor.SensorSup, strategy: :one_for_one},
      Watchdog
    ]

    # rest_for_one: se o DynamicSupervisor cair, o Watchdog também é reiniciado,
    # pois perdeu as referências de monitoramento
    Supervisor.init(children, strategy: :rest_for_one)
  end
end

# ---------------------------------------------------------------------------
# Demonstração dos requisitos
# ---------------------------------------------------------------------------
defmodule SensorMonitor.Demo do
  @moduledoc "Executa um cenário demonstrando cada requisito."

  alias SensorMonitor.FakeDB

  def run do
    {:ok, _pid} = SensorMonitor.start_link()
    :ok = SensorMonitor.add_sensor("temp-1")

    # Atualização válida
    IO.inspect(SensorMonitor.update("temp-1", %{value: 25.5, unit: "celsius"}), label: "ok")

    # Erro de validação (entrada inválida)
    IO.inspect(SensorMonitor.update("temp-1", %{value: "abc", foo: 1}), label: "validação")

    # Falha operacional com garantia de atomicidade
    FakeDB.inject_failure(:disk_full)
    IO.inspect(SensorMonitor.update("temp-1", %{value: 30.0, unit: "celsius"}), label: "operacional")
    IO.inspect(SensorMonitor.read("temp-1"), label: "estado após falha")

    # Encerramento inesperado detectado e sensor reiniciado pelo Watchdog
    SensorMonitor.crash("temp-1")
    Process.sleep(50)
    IO.inspect(SensorMonitor.unexpected_exits(), label: "saídas inesperadas")
    IO.inspect(SensorMonitor.update("temp-1", %{value: 27.0, unit: "celsius"}), label: "após reinício")
  end
end
