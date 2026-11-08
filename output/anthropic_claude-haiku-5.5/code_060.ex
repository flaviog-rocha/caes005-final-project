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

# Monitoramento de sensores com Registry, macros, transações atômicas em um
# banco fictício e detecção de encerramentos inesperados de processos.
#
# Convenções de erro:
#   {:error, {:validation, [{campo, motivo}]}}  -> entrada inválida (erro do cliente, não retentar)
#   {:error, {:operational, motivo}}            -> falha de infraestrutura/processo (pode retentar)

defmodule SensorMonitor.Macros do
  @moduledoc """
  Macros do domínio:

    * `rules/2`  - gera, em tempo de compilação, cláusulas `validate/3` a partir de
                   regras declarativas (tipo, mínimo, máximo).
    * `atomic/2` - envolve um bloco em uma transação do banco fictício. Exceções são
                   convertidas em `{:error, {:operational, _}}`, então nada é gravado
                   parcialmente.
  """

  @doc false
  # Verifica tipo e faixa de um valor de acordo com as opções da regra.
  def check(value, opts) do
    with :ok <- check_type(value, Keyword.get(opts, :type)),
         :ok <- check_range(value, Keyword.get(opts, :min), Keyword.get(opts, :max)) do
      :ok
    end
  end

  defp check_type(_value, nil), do: :ok
  defp check_type(value, :float) when is_number(value), do: :ok
  defp check_type(value, :integer) when is_integer(value), do: :ok
  defp check_type(_value, type), do: {:error, {:wrong_type, type}}

  defp check_range(_value, nil, nil), do: :ok

  defp check_range(value, min, max) when is_number(value) do
    cond do
      not is_nil(min) and value < min -> {:error, {:below_min, min}}
      not is_nil(max) and value > max -> {:error, {:above_max, max}}
      true -> :ok
    end
  end

  defp check_range(_value, _min, _max), do: {:error, {:wrong_type, :number}}

  @doc """
  Gera cláusulas `validate/3` para um tipo de sensor.

      rules :temperature, value: [type: :float, min: -50, max: 150]
  """
  defmacro rules(sensor_type, specs) do
    clauses =
      for {field, opts} <- specs do
        quote do
          def validate(unquote(sensor_type), unquote(field), value) do
            SensorMonitor.Macros.check(value, unquote(opts))
          end
        end
      end

    {:__block__, [], clauses}
  end

  @doc """
  Executa o bloco dentro de uma transação atômica do `SensorMonitor.Store`.

  O bloco recebe o estado atual do banco na variável informada e deve retornar
  `{:ok, novo_estado, resultado}` ou `{:error, motivo}`. Exceções viram falha operacional.

  Atenção: o bloco roda dentro do processo do Store; não deve chamar o próprio Store.
  """
  defmacro atomic(var, do: block) do
    quote do
      SensorMonitor.Store.transaction(fn unquote(var) ->
        try do
          unquote(block)
        rescue
          exception ->
            {:error, {:operational, {:exception, Exception.message(exception)}}}
        end
      end)
    end
  end
end

defmodule SensorMonitor.Rules do
  @moduledoc """
  Regras de validação por tipo de sensor. Cada linha gera uma cláusula de `validate/3`
  em tempo de compilação através da macro `rules/2`.
  """
  import SensorMonitor.Macros

  rules :temperature, value: [type: :float, min: -50, max: 150]
  rules :humidity, value: [type: :float, min: 0, max: 100]
  rules :pressure, value: [type: :float, min: 300, max: 1100]

  # Fallback: campo ou tipo desconhecido é erro de validação.
  def validate(type, field, _value), do: {:error, {:unknown_field, type, field}}
end

defmodule SensorMonitor.Store do
  @moduledoc """
  Banco de dados fictício em memória.

  Ponto de extensão: a transação poderia ser substituída por `Ecto.Multi` /
  `Repo.transaction/1` sem alterar os chamadores, e o esquema poderia ser criado
  com `Ecto.Migration`. Nada disso é obrigatório aqui.
  """
  use GenServer

  @timeout 2_000

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{sensors: %{}, events: []}, name: __MODULE__)
  end

  @doc "Lê o registro persistido de um sensor."
  def get(id) do
    GenServer.call(__MODULE__, {:get, id}, @timeout)
  catch
    :exit, reason -> {:error, {:operational, {:store_unavailable, reason}}}
  end

  @doc "Executa uma função de transação. Use através da macro `atomic/2`."
  def transaction(fun) do
    GenServer.call(__MODULE__, {:tx, fun}, @timeout)
  catch
    :exit, reason -> {:error, {:operational, {:store_unavailable, reason}}}
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:get, id}, _from, state) do
    reply =
      case Map.fetch(state.sensors, id) do
        {:ok, record} -> {:ok, record}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:tx, fun}, _from, state) do
    # O estado só é substituído se a transação retornar {:ok, ...}.
    # Em erro, o estado anterior permanece, impedindo escrita parcial.
    case fun.(state) do
      {:ok, new_state, result} -> {:reply, {:ok, result}, new_state}
      {:error, _reason} = error -> {:reply, error, state}
      other -> {:reply, {:error, {:operational, {:invalid_tx_result, other}}}, state}
    end
  end
end

defmodule SensorMonitor.Watchdog do
  @moduledoc """
  Monitora os processos de sensores com `Process.monitor/1` e registra encerramentos
  inesperados (qualquer motivo diferente de `:normal`, `:shutdown` ou `{:shutdown, _}`).
  """
  use GenServer
  require Logger
  import SensorMonitor.Macros

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc "Registra o processo chamador para monitoramento."
  def watch(id), do: GenServer.cast(__MODULE__, {:watch, id, self()})

  @impl true
  def init(:ok), do: {:ok, %{refs: %{}}}

  @impl true
  def handle_cast({:watch, id, pid}, state) do
    ref = Process.monitor(pid)
    {:noreply, %{state | refs: Map.put(state.refs, ref, id)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {id, refs} = Map.pop(state.refs, ref)

    if not is_nil(id) and not expected_exit?(reason) do
      Logger.error("sensor #{inspect(id)} encerrou inesperadamente: #{inspect(reason)}")
      record_incident(id, reason)
    end

    {:noreply, %{state | refs: refs}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp expected_exit?(:normal), do: true
  defp expected_exit?(:shutdown), do: true
  defp expected_exit?({:shutdown, _}), do: true
  defp expected_exit?(_reason), do: false

  defp record_incident(id, reason) do
    atomic db do
      event = {:unexpected_exit, id, reason, System.system_time(:millisecond)}
      {:ok, %{db | events: [event | db.events]}, :recorded}
    end
  end
end

defmodule SensorMonitor.Sensor do
  @moduledoc """
  Processo de um sensor, registrado no `Registry` pelo id.

  O estado em memória só é alterado após a confirmação da transação no banco,
  então um crash entre validação e gravação não deixa estado inconsistente.
  """
  use GenServer, restart: :transient
  import SensorMonitor.Macros

  alias SensorMonitor.{Rules, Store, Watchdog}

  @call_timeout 2_000

  def start_link({id, type}) do
    GenServer.start_link(__MODULE__, {id, type}, name: via(id))
  end

  def via(id), do: {:via, Registry, {SensorMonitor.Registry, id}}

  def update(id, readings) when is_map(readings) do
    call(id, {:update, readings})
  end

  def update(_id, _readings), do: {:error, {:validation, [:readings_must_be_a_map]}}

  def read(id), do: call(id, :read)

  defp call(id, msg) do
    GenServer.call(via(id), msg, @call_timeout)
  catch
    :exit, reason -> {:error, {:operational, {:sensor_unavailable, reason}}}
  end

  @impl true
  def init({id, type}) do
    Watchdog.watch(id)

    case Store.get(id) do
      {:ok, record} -> {:ok, Map.put(record, :id, id)}
      :not_found -> {:ok, %{id: id, type: type, value: %{}, version: 0}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:read, _from, state) do
    {:reply, {:ok, state.value}, state}
  end

  def handle_call({:update, readings}, _from, state) do
    with {:ok, clean} <- validate_readings(state.type, readings),
         {:ok, record} <- persist(state, clean) do
      new_state = %{state | value: record.value, version: record.version}
      {:reply, {:ok, new_state.version}, new_state}
    else
      error -> {:reply, error, state}
    end
  end

  # Valida todos os campos antes de qualquer escrita (tudo ou nada).
  defp validate_readings(_type, readings) when map_size(readings) == 0 do
    {:error, {:validation, [:empty_update]}}
  end

  defp validate_readings(type, readings) do
    errors =
      Enum.flat_map(readings, fn {field, value} ->
        case Rules.validate(type, field, value) do
          :ok -> []
          {:error, reason} -> [{field, reason}]
        end
      end)

    case errors do
      [] -> {:ok, readings}
      _ -> {:error, {:validation, errors}}
    end
  end

  # Grava valor, versão e evento em uma única transação; detecta conflito de versão.
  defp persist(state, readings) do
    id = state.id
    expected = state.version
    type = state.type
    current_value = state.value

    atomic db do
      case Map.get(db.sensors, id) do
        %{version: v} when v != expected ->
          {:error, {:operational, :version_conflict}}

        _ ->
          record = %{
            type: type,
            value: Map.merge(current_value, readings),
            version: expected + 1
          }

          event = {:sensor_updated, id, record.version}

          new_db = %{
            db
            | sensors: Map.put(db.sensors, id, record),
              events: [event | db.events]
          }

          {:ok, new_db, record}
      end
    end
  end
end

defmodule SensorMonitor do
  @moduledoc """
  API pública do monitoramento de sensores.

      {:ok, _} = SensorMonitor.start_link()
      {:ok, _} = SensorMonitor.add_sensor("t1", :temperature)
      {:ok, 1} = SensorMonitor.update("t1", %{value: 23.5})
      {:error, {:validation, _}} = SensorMonitor.update("t1", %{value: 999.0})
  """

  alias SensorMonitor.{Sensor, Store, Watchdog}

  def start_link(_opts \\ []) do
    children = [
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      Store,
      Watchdog,
      {DynamicSupervisor, strategy: :one_for_one, name: SensorMonitor.SensorSup}
    ]

    # rest_for_one: se o Store cair, Watchdog e sensores são reiniciados em ordem,
    # e os sensores recuperam seu estado do Store no init.
    Supervisor.start_link(children, strategy: :rest_for_one, name: SensorMonitor.Root)
  end

  def add_sensor(id, type) do
    DynamicSupervisor.start_child(SensorMonitor.SensorSup, {Sensor, {id, type}})
  end

  def update(id, readings), do: Sensor.update(id, readings)

  def read(id), do: Sensor.read(id)
end
