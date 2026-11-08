# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Diferenciar erros de validação de falhas operacionais.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Supervisor
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# sensor_monitor.exs
# Execução: elixir sensor_monitor.exs

defmodule SensorMonitor do
  @moduledoc """
  Monitoramento de sensores com:
  - comunicação entre processos via Registry (pub/sub) e chamadas GenServer;
  - separação entre erros de validação ({:error, {:validation, _}}) e falhas
    operacionais ({:error, {:operational, _}});
  - persistência em DETS para recuperação após reinicializações.
  """

  use Supervisor

  @sensor_sup SensorMonitor.SensorSup

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    data_file = Keyword.get(opts, :data_file, "sensor_monitor.dets")

    children = [
      # Registro de nomes únicos (um processo por sensor)
      {Registry, keys: :unique, name: SensorMonitor.Names},
      # Registro de assinantes (pub/sub, aceita múltiplas entradas por tópico)
      {Registry, keys: :duplicate, name: SensorMonitor.Bus},
      # Dono da persistência
      {SensorMonitor.Store, data_file},
      # Supervisor dinâmico dos sensores
      {DynamicSupervisor, strategy: :one_for_one, name: @sensor_sup}
    ]

    # rest_for_one: se o Store cair, os sensores (que dependem dele) são reiniciados
    # e recarregam seus dados persistidos
    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "Cria um sensor. Retorna {:ok, pid} ou {:error, {:validation | :operational, reason}}."
  def add_sensor(id) do
    with :ok <- SensorMonitor.Validator.validate_id(id) do
      safe(fn ->
        case DynamicSupervisor.start_child(@sensor_sup, {SensorMonitor.Sensor, id}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, _pid}} -> {:error, {:validation, :already_registered}}
          {:error, reason} -> {:error, {:operational, reason}}
        end
      end)
    end
  end

  @doc "Registra uma leitura. Retorna {:ok, reading} ou {:error, {:validation | :operational, reason}}."
  def record(id, type, value) do
    with :ok <- SensorMonitor.Validator.validate_id(id) do
      safe(fn -> GenServer.call(SensorMonitor.Sensor.via(id), {:record, type, value}) end)
    end
  end

  @doc "Retorna as leituras recentes do sensor (mais recente primeiro)."
  def readings(id) do
    with :ok <- SensorMonitor.Validator.validate_id(id) do
      safe(fn -> GenServer.call(SensorMonitor.Sensor.via(id), :readings) end)
    end
  end

  # Converte falhas de processo (sensor inexistente, processo caído, timeout) em erro operacional.
  defp safe(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:operational, {:unavailable, reason}}}
  end
end

defmodule SensorMonitor.Validator do
  @moduledoc """
  Validação pura de entradas. Só produz erros de validação, nunca
  falhas operacionais.
  """

  @ranges %{
    temperature: {-50.0, 150.0},
    humidity: {0.0, 100.0},
    pressure: {300.0, 1100.0}
  }

  def validate_id(id) when is_binary(id) and byte_size(id) in 1..64, do: :ok
  def validate_id(_id), do: {:error, {:validation, :invalid_sensor_id}}

  def validate_reading(type, value) do
    with {:ok, {min, max}} <- fetch_range(type),
         :ok <- validate_number(value),
         :ok <- validate_range(value, min, max) do
      {:ok, %{type: type, value: value, ts: System.os_time(:nanosecond)}}
    end
  end

  defp fetch_range(type) do
    case Map.fetch(@ranges, type) do
      {:ok, range} -> {:ok, range}
      :error -> {:error, {:validation, {:unknown_type, type}}}
    end
  end

  defp validate_number(value) when is_number(value), do: :ok
  defp validate_number(_value), do: {:error, {:validation, :not_a_number}}

  defp validate_range(value, min, max) when value >= min and value <= max, do: :ok
  defp validate_range(_value, min, max), do: {:error, {:validation, {:out_of_range, {min, max}}}}
end

defmodule SensorMonitor.Store do
  @moduledoc """
  Persistência em DETS. Um único processo é dono da tabela, o que serializa
  o acesso e centraliza a recuperação de falhas.
  """

  use GenServer

  @table :sensor_monitor_readings
  @max_stored 1_000
  @recent 20

  def start_link(file), do: GenServer.start_link(__MODULE__, file, name: __MODULE__)

  def put(id, reading), do: GenServer.call(__MODULE__, {:put, id, reading})
  def fetch(id), do: GenServer.call(__MODULE__, {:fetch, id})

  @impl true
  def init(file) do
    # Permite que terminate/2 feche a tabela em um shutdown ordenado
    Process.flag(:trap_exit, true)

    case :dets.open_file(@table, file: String.to_charlist(file), type: :set) do
      {:ok, table} -> {:ok, table}
      {:error, reason} -> {:stop, {:operational, {:open_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:put, id, reading}, _from, table) do
    reply =
      with :ok <- :dets.insert(table, {{id, reading.ts}, reading}),
           :ok <- prune(table, id) do
        :dets.sync(table)
      end

    {:reply, reply, table}
  end

  def handle_call({:fetch, id}, _from, table) do
    reply =
      case :dets.match_object(table, {{id, :_}, :_}) do
        {:error, reason} ->
          {:error, reason}

        entries ->
          readings =
            entries
            |> Enum.sort_by(fn {{_id, ts}, _} -> ts end, :desc)
            |> Enum.take(@recent)
            |> Enum.map(fn {_key, reading} -> reading end)

          {:ok, readings}
      end

    {:reply, reply, table}
  end

  @impl true
  def terminate(_reason, table) do
    :dets.close(table)
  end

  # Remove as leituras mais antigas quando o limite por sensor é excedido.
  defp prune(table, id) do
    case :dets.match_object(table, {{id, :_}, :_}) do
      {:error, reason} ->
        {:error, reason}

      entries ->
        entries
        |> Enum.sort_by(fn {{_id, ts}, _} -> ts end)
        |> Enum.take(max(length(entries) - @max_stored, 0))
        |> Enum.reduce_while(:ok, fn {key, _}, :ok ->
          case :dets.delete(table, key) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
    end
  end
end

defmodule SensorMonitor.Sensor do
  @moduledoc """
  Processo de um sensor. Ao iniciar, recarrega suas leituras persistidas.
  Erros de validação não alteram o estado; falhas de persistência são
  devolvidas como erro operacional sem derrubar o processo.
  """

  use GenServer

  alias SensorMonitor.{Bus, Store, Validator}

  @recent 20

  def start_link(id), do: GenServer.start_link(__MODULE__, id, name: via(id))

  def via(id), do: {:via, Registry, {SensorMonitor.Names, {:sensor, id}}}

  @impl true
  def init(id) do
    case Store.fetch(id) do
      {:ok, readings} -> {:ok, %{id: id, readings: readings}}
      {:error, reason} -> {:stop, {:operational, {:recovery_failed, reason}}}
    end
  end

  @impl true
  def handle_call({:record, type, value}, _from, state) do
    case Validator.validate_reading(type, value) do
      {:error, _} = error -> {:reply, error, state}
      {:ok, reading} -> persist(reading, state)
    end
  end

  def handle_call(:readings, _from, state) do
    {:reply, {:ok, state.readings}, state}
  end

  defp persist(reading, state) do
    case Store.put(state.id, reading) do
      :ok ->
        # Comunicação com outros processos: notifica assinantes do tópico
        Bus.publish(:readings, {:reading, state.id, reading})
        readings = Enum.take([reading | state.readings], @recent)
        {:reply, {:ok, reading}, %{state | readings: readings}}

      {:error, reason} ->
        {:reply, {:error, {:operational, {:persist_failed, reason}}}, state}
    end
  end
end

defmodule SensorMonitor.Bus do
  @moduledoc "Pub/sub simples sobre Registry com chaves duplicadas."

  @registry SensorMonitor.Bus

  def subscribe(topic) do
    case Registry.register(@registry, topic, nil) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  def unsubscribe(topic), do: Registry.unregister(@registry, topic)

  def publish(topic, message) do
    Registry.dispatch(@registry, topic, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:sensor_monitor, topic, message})
    end)
  end
end

defmodule SensorMonitor.Demo do
  @moduledoc "Demonstração: validação, comunicação e recuperação após reinício."

  alias SensorMonitor.Bus

  def run do
    {:ok, _} = SensorMonitor.start_link(data_file: "sensor_monitor.dets")
    :ok = Bus.subscribe(:readings)
    {:ok, _} = SensorMonitor.add_sensor("sala-1")

    IO.inspect(SensorMonitor.record("sala-1", :temperature, 22.5), label: "gravação válida")
    IO.inspect(SensorMonitor.record("sala-1", :temperature, 500), label: "erro de validação")
    IO.inspect(SensorMonitor.record("sala-1", :umidade, 40), label: "tipo desconhecido")
    IO.inspect(SensorMonitor.record("inexistente", :humidity, 50), label: "falha operacional")

    receive do
      {:sensor_monitor, :readings, message} -> IO.inspect(message, label: "mensagem recebida")
    after
      1_000 -> IO.puts("nenhuma mensagem recebida")
    end

    # Simula reinicialização completa do sistema
    Supervisor.stop(SensorMonitor)

    {:ok, _} = SensorMonitor.start_link(data_file: "sensor_monitor.dets")
    {:ok, _} = SensorMonitor.add_sensor("sala-1")
    IO.inspect(SensorMonitor.readings("sala-1"), label: "recuperado após reinício")

    Supervisor.stop(SensorMonitor)
  end
end

SensorMonitor.Demo.run()
