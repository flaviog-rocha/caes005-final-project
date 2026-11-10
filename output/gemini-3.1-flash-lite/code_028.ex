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

defmodule SensorSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {SensorSystem.Storage, "sensor_data.dets"},
      {SensorSystem.Monitor, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end

defmodule SensorSystem.Storage do
  use GenServer

  def start_link(file), do: GenServer.start_link(__MODULE__, file, name: __MODULE__)

  @impl true
  def init(file) do
    {:ok, db} = :dets.open_file(:sensor_db, file: String.to_charlist(file))
    {:ok, db}
  end

  def persist(key, value), do: GenServer.cast(__MODULE__, {:save, key, value})

  @impl true
  def handle_cast({:save, key, value}, db) do
    :dets.insert(db, {key, value})
    {:noreply, db}
  end
end

defmodule SensorSystem.Monitor do
  use GenServer

  # Erros de validação (esperados, não crasham)
  defmodule ValidationError, do: defexception [:message]
  # Falhas operacionais (inesperadas, causam restart)
  defmodule OperationalFailure, do: defexception [:message]

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def process_reading(sensor_id, value) when value < 0 do
    raise ValidationError, message: "Valor negativo inválido: #{value}"
  end

  def process_reading(sensor_id, value) do
    GenServer.call(__MODULE__, {:record, sensor_id, value})
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:record, id, val}, _from, state) do
    if val > 1000 do
      # Simulação de falha operacional
      {:stop, %OperationalFailure{message: "Hardware em sobrecarga"}, state}
    else
      SensorSystem.Storage.persist(id, val)
      {:reply, :ok, Map.put(state, id, val)}
    end
  end
end
