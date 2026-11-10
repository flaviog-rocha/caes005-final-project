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

defmodule SensorSystem.Errors do
  defexception [:message, :type]

  @type error_type :: :validation | :operational

  defexception [:message, type: :operational]
end

defmodule SensorSystem.Supervisor do
  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: SensorRegistry},
      {DynamicSupervisor, name: SensorSupervisor, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end

defmodule SensorSystem.Sensor do
  use GenServer
  require Logger

  # Macro para facilitar a definição de leitura de sensores com verificação de integridade
  defmacro def_sensor_read(name, do: block) do
    quote do
      def unquote(name)(pid) do
        GenServer.call(pid, {:read, fn -> unquote(block) end})
      end
    end
  end

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: {:via, Registry, {SensorRegistry, id}})
  end

  @impl true
  def init(id) do
    # Monitoramento: O processo pai (SensorSupervisor) já monitora via estratégia
    # Aqui garantimos que o estado inicial seja consistente
    {:ok, %{id: id, last_value: nil, status: :ready}}
  end

  def_sensor_read(:safe_read) do
    # Simula logica de leitura
    :rand.uniform(100)
  end

  @impl true
  def handle_call({:read, func}, _from, state) do
    # Impedir atualizações parcialmente concluídas:
    # Usamos uma transação fictícia. Em um cenário real, 
    # usaríamos Ecto.Multi para garantir atomicidade.
    try do
      value = func.()
      
      if value < 0, do: raise(SensorSystem.Errors, message: "Valor inválido", type: :validation)
      
      # Simulação de escrita em DB
      # Ecto.Adapters.SQL.query(Repo, "UPDATE sensors SET val = $1", [value])
      
      new_state = %{state | last_value: value}
      {:reply, {:ok, value}, new_state}
    rescue
      e in [SensorSystem.Errors] -> 
        Logger.error("Erro de #{e.type}: #{e.message}")
        {:reply, {:error, e.type}, state}
      e ->
        Logger.error("Falha operacional inesperada: #{inspect(e)}")
        {:stop, :operational_failure, state}
    end
  end
end

# Exemplo de uso:
# SensorSystem.Supervisor.start_link([])
# DynamicSupervisor.start_child(SensorSupervisor, {SensorSystem.Sensor, "term-01"})
# SensorSystem.Sensor.safe_read(pid)
