# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o monitoramento de sensores, atendendo somente aos seguintes requisitos:
#
# 1. Detectar encerramentos inesperados de processos.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir comunicação entre diferentes processos.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule SensorMonitor do
  @moduledoc """
  Monitor de sensores utilizando Task para processamento concorrente,
  Registry para comunicação entre processos e Supervisor para resiliência.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    children = [
      {Registry, keys: :unique, name: SensorRegistry},
      {Task.Supervisor, name: SensorTaskSupervisor, max_children: 10}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Inicia o monitoramento de um sensor.
  A Task é monitorada pelo Task.Supervisor. Se falhar, o supervisor pode reiniciar.
  """
  def monitor_sensor(sensor_id, data_source) do
    Task.Supervisor.start_child(SensorTaskSupervisor, fn ->
      # Registra o processo para permitir comunicação via Registry
      {:ok, _} = Registry.register(SensorRegistry, sensor_id, self())

      loop_sensor(sensor_id, data_source)
    end)
  end

  defp loop_sensor(id, source) do
    # Simulação de leitura
    Process.sleep(1000)
    data = "Dados de #{id}: #{:rand.uniform(100)}"
    
    IO.puts("Processo #{inspect(self())} lendo: #{data}")
    
    # Exemplo de comunicação: Envia mensagem para qualquer processo interessado
    # através do Registry se necessário, ou processa localmente.
    
    loop_sensor(id, source)
  end

  @doc """
  Comunicação: Busca o PID de um sensor registrado para enviar uma mensagem direta.
  """
  def send_command(sensor_id, message) do
    case Registry.lookup(SensorRegistry, sensor_id) do
      [{pid, _}] -> send(pid, message)
      [] -> {:error, :sensor_not_found}
    end
  end
end

# Exemplo de uso:
# SensorMonitor.start_link([])
# SensorMonitor.monitor_sensor("sensor_01", :temp)
# SensorMonitor.send_command("sensor_01", :stop)
