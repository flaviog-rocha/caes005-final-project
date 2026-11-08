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
#
# Monitoramento de sensores com:
#   1. Comunicação entre processos (Registry como barramento pub/sub e lookup de processos,
#      mensagens assíncronas enviadas aos inscritos).
#   2. Distinção entre erros de validação ({:error, {:validation, _}}) e falhas operacionais
#      ({:error, {:operational, _}}).
#   3. Persistência em disco (escrita atômica) com recuperação após reinicialização.
#
# Árvore de supervisão:
#   SensorMonitor.Root (Supervisor, :one_for_one)
#     ├── Registry :unique   (SensorMonitor.Registry)  - lookup de sensores por id
#     ├── Registry :duplicate (SensorMonitor.PubSub)   - inscrições de assinantes
#     ├── SensorMonitor.Store                          - estado e persistência
#     ├── DynamicSupervisor (SensorMonitor.SensorSup)  - um processo por sensor
#     └── Task (restore_sensors, :transient)           - reinicia sensores persistidos
#
# Execução: elixir sensor_monitor.exs

defmodule SensorMonitor do
  @moduledoc """
  API pública do monitor de sensores.

  Convenção de retorno:
    * `{:ok, valor}` / `:ok`                         - sucesso
    * `{:error, {:validation, motivo}}`              - entrada inválida (culpa do chamador)
    * `{:error, {:operational, motivo}}`             - falha do sistema (persistência, processo indisponível etc.)
  """

  alias SensorMonitor.{Sensor, Store, Validation}

  @doc "Inicia a árvore de supervisão do monitor."
  def start_link(opts \\ []), do: SensorMonitor.Root.start_link(opts)

  @doc """
  Registra um sensor. Valida a configuração, persiste antes de iniciar o processo
  e só então sobe o processo supervisionado.
  """
  def register_sensor(id, type, opts \\ []) do
    with {:ok, config} <- Validation.sensor_config(id, type, opts),
         :ok <- Store.put_sensor(config),
         {:ok, _pid} <- start_sensor(config) do
      {:ok, config.id}
    end
  end

  @doc "Envia uma leitura para o sensor identificado por `id`."
  def record(id, value) do
    # Sensor inexistente é erro de validação do chamador; processo indisponível é operacional.
    case Registry.lookup(SensorMonitor.Registry, id) do
      [] -> {:error, {:validation, :unknown_sensor}}
      [_] -> Sensor.record(id, value)
    end
  end

  @doc "Retorna até `limit` leituras mais recentes do sensor."
  def history(id, limit \\ 20), do: Store.readings(id, limit)

  @doc """
  Inscreve o processo atual em um tópico. Use o id do sensor ou `:all` para receber
  todas as leituras. Mensagens recebidas: `{:sensor_reading, id, reading}`.
  """
  def subscribe(topic) do
    case Registry.register(SensorMonitor.PubSub, topic, nil) do
      {:ok, _owner} -> :ok
      {:error, reason} -> {:error, {:operational, reason}}
    end
  end

  @doc "Lista as configurações de todos os sensores persistidos."
  def sensors, do: Store.list_sensors()

  defp start_sensor(config) do
    case DynamicSupervisor.start_child(SensorMonitor.SensorSup, {Sensor, config}) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, {:operational, {:start_failed, reason}}}
    end
  end
end

defmodule SensorMonitor.Root do
  @moduledoc "Supervisor raiz da aplicação."
  use Supervisor

  @default_path "sensor_monitor_data.bin"

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :data_path, @default_path)

    children = [
      # Lookup de processos de sensores por id.
      {Registry, keys: :unique, name: SensorMonitor.Registry},
      # Barramento de mensagens (pub/sub) entre processos.
      {Registry, keys: :duplicate, name: SensorMonitor.PubSub},
      # Estado persistente. Carrega o arquivo ao iniciar.
      {SensorMonitor.Store, path},
      # Um processo supervisionado por sensor.
      {DynamicSupervisor, name: SensorMonitor.SensorSup, strategy: :one_for_one},
      # Após a inicialização, reativa os sensores persistidos.
      Supervisor.child_spec({Task, fn -> restore_sensors() end}, restart: :transient)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp restore_sensors do
    Enum.each(Store.list_sensors(), fn config ->
      DynamicSupervisor.start_child(SensorMonitor.SensorSup, {SensorMonitor.Sensor, config})
    end)

    :ok
  end

  alias SensorMonitor.Store
end

defmodule SensorMonitor.Validation do
  @moduledoc "Regras de validação de configurações e leituras."

  # Faixas padrão por tipo de sensor.
  @ranges %{
    temperature: {-50.0, 150.0},
    humidity: {0.0, 100.0},
    pressure: {300.0, 1100.0}
  }

  @doc "Valida e normaliza a configuração de um sensor."
  def sensor_config(id, type, opts) do
    case Map.fetch(@ranges, type) do
      :error ->
        {:error, {:validation, {:unknown_type, type}}}

      {:ok, {default_min, default_max}} ->
        min = Keyword.get(opts, :min, default_min)
        max = Keyword.get(opts, :max, default_max)

        cond do
          not (is_binary(id) and id != "") ->
            {:error, {:validation, {:invalid_id, id}}}

          not (is_number(min) and is_number(max) and min < max) ->
            {:error, {:validation, {:invalid_range, min, max}}}

          true ->
            {:ok, %{id: id, type: type, min: min * 1.0, max: max * 1.0}}
        end
    end
  end

  @doc "Valida uma leitura contra a configuração do sensor."
  def reading(%{min: min, max: max}, value) do
    cond do
      not is_number(value) ->
        {:error, {:validation, {:not_a_number, value}}}

      value < min or value > max ->
        {:error, {:validation, {:out_of_range, value, min, max}}}

      true ->
        :ok
    end
  end
end

defmodule SensorMonitor.Store do
  @moduledoc """
  Estado persistente do sistema. Todas as alterações são gravadas em disco
  antes de serem aplicadas em memória, usando escrita em arquivo temporário
  seguida de renomeação atômica.
  """
  use GenServer

  @max_readings 100

  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  def put_sensor(config), do: GenServer.call(__MODULE__, {:put_sensor, config})
  def list_sensors, do: GenServer.call(__MODULE__, :list_sensors)
  def append_reading(id, reading), do: GenServer.call(__MODULE__, {:append_reading, id, reading})
  def readings(id, limit), do: GenServer.call(__MODULE__, {:readings, id, limit})

  @impl true
  def init(path) do
    case load(path) do
      {:ok, data} -> {:ok, %{path: path, data: data}}
      {:error, reason} -> {:stop, {:corrupted_store, reason}}
    end
  end

  @impl true
  def handle_call({:put_sensor, config}, _from, state) do
    if Map.has_key?(state.data, config.id) do
      {:reply, {:error, {:validation, :already_registered}}, state}
    else
      data = Map.put(state.data, config.id, %{config: config, readings: []})
      commit(state, data, :ok)
    end
  end

  def handle_call(:list_sensors, _from, state) do
    {:reply, Enum.map(Map.values(state.data), & &1.config), state}
  end

  def handle_call({:append_reading, id, reading}, _from, state) do
    case Map.fetch(state.data, id) do
      {:ok, entry} ->
        readings = Enum.take([reading | entry.readings], @max_readings)
        data = Map.put(state.data, id, %{entry | readings: readings})
        commit(state, data, :ok)

      :error ->
        {:reply, {:error, {:validation, :unknown_sensor}}, state}
    end
  end

  def handle_call({:readings, id, limit}, _from, state) do
    case Map.fetch(state.data, id) do
      {:ok, entry} -> {:reply, {:ok, Enum.take(entry.readings, limit)}, state}
      :error -> {:reply, {:error, {:validation, :unknown_sensor}}, state}
    end
  end

  # Persiste primeiro; só atualiza o estado em memória se a gravação tiver sucesso.
  defp commit(state, new_data, reply) do
    case persist(state.path, new_data) do
      :ok -> {:reply, reply, %{state | data: new_data}}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  defp persist(path, data) do
    tmp = path <> ".tmp"

    with :ok <- File.write(tmp, :erlang.term_to_binary(data)),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} -> {:error, {:operational, {:persist_failed, reason}}}
    end
  end

  defp load(path) do
    case File.read(path) do
      {:ok, binary} ->
        try do
          {:ok, :erlang.binary_to_term(binary, [:safe])}
        rescue
          ArgumentError -> {:error, :invalid_format}
        end

      # Primeira execução: não há dados ainda.
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end
end

defmodule SensorMonitor.Sensor do
  @moduledoc """
  Processo de um sensor. Valida leituras, delega a persistência ao Store
  e publica as leituras aceitas no barramento pub/sub.

  Falhas inesperadas derrubam o processo; o supervisor (DynamicSupervisor)
  o reinicia e a configuração é recuperada do Store.
  """
  use GenServer
  alias SensorMonitor.{Store, Validation}

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: via(config.id))
  end

  def record(id, value) do
    GenServer.call(via(id), {:record, value}, 5_000)
  catch
    # Processo indisponível, reiniciando ou timeout: falha operacional.
    :exit, reason -> {:error, {:operational, {:sensor_unavailable, reason}}}
  end

  defp via(id), do: {:via, Registry, {SensorMonitor.Registry, id}}

  @impl true
  def init(config), do: {:ok, config}

  @impl true
  def handle_call({:record, value}, _from, config) do
    with :ok <- Validation.reading(config, value),
         reading = %{value: value, at: System.system_time(:millisecond)},
         :ok <- Store.append_reading(config.id, reading) do
      publish(config.id, reading)
      {:reply, {:ok, reading}, config}
    else
      error -> {:reply, error, config}
    end
  end

  # Envia a leitura a todos os assinantes do id do sensor e de :all.
  defp publish(id, reading) do
    message = {:sensor_reading, id, reading}

    for topic <- [id, :all] do
      Registry.dispatch(SensorMonitor.PubSub, topic, fn entries ->
        for {pid, _} <- entries, do: send(pid, message)
      end)
    end

    :ok
  end
end

defmodule SensorMonitor.Demo do
  @moduledoc "Demonstração de uso. Execute o script mais de uma vez para ver a recuperação dos dados."

  def run do
    {:ok, _} = SensorMonitor.start_link(data_path: "sensor_monitor_data.bin")
    :ok = SensorMonitor.subscribe(:all)

    IO.inspect(SensorMonitor.register_sensor("temp-01", :temperature), label: "registro")
    IO.inspect(SensorMonitor.register_sensor("temp-01", :temperature), label: "registro duplicado")
    IO.inspect(SensorMonitor.register_sensor("", :humidity), label: "id inválido")
    IO.inspect(SensorMonitor.record("temp-01", 23.5), label: "leitura válida")
    IO.inspect(SensorMonitor.record("temp-01", 500), label: "erro de validação")
    IO.inspect(SensorMonitor.record("inexistente", 1), label: "sensor desconhecido")

    receive do
      msg -> IO.inspect(msg, label: "broadcast recebido")
    after
      500 -> IO.puts("sem broadcast")
    end

    IO.inspect(SensorMonitor.history("temp-01", 5), label: "histórico")
    IO.inspect(SensorMonitor.sensors(), label: "sensores persistidos")
  end
end

SensorMonitor.Demo.run()
