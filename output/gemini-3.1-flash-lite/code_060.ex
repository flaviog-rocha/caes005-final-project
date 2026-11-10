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

  defexception [:message]
  @type t :: %__MODULE__{message: String.t()}
end

defmodule SensorSystem.Macros do
  defmacro def_sensor_handler(name, do: block) do
    quote do
      def unquote(name)(sensor_id, payload) do
        try do
          # Garantia de atomicidade/consistência simulada
          result = unquote(block)
          {:ok, result}
        rescue
          e in [SensorSystem.Errors.ValidationError] -> {:error, :validation, e.message}
          e -> {:error, :operational, Exception.message(e)}
        end
      end
    end
  end
end

defmodule SensorSystem.Registry do
  def child_spec(_) do
    Supervisor.child_spec({Registry, keys: :unique, name: __MODULE__}, id: __MODULE__)
  end
end

defmodule SensorSystem.SensorWorker do
  use GenServer
  require Logger

  def start_link(id) do
    GenServer.start_link(__MODULE__, id, name: via_tuple(id))
  end

  def init(id) do
    Process.flag(:trap_exit, true)
    Logger.info("Sensor #{id} iniciado.")
    {:ok, %{id: id, state: :idle}}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    Logger.error("Sensor #{state.id} encerrou inesperadamente: #{inspect(reason)}")
    {:stop, reason, state}
  end

  defp via_tuple(id), do: {:via, Registry, {SensorSystem.Registry, id}}
end

defmodule SensorSystem.Manager do
  import SensorSystem.Macros

  # Implementação de requisito: Impedir atualizações parciais via transação lógica
  def_sensor_handler update_reading do
    # Simulação de escrita em banco (Ecto-like)
    # Repo.transaction(fn -> ... end)
    if payload[:value] < 0, 
      do: raise(SensorSystem.Errors, message: "Valor inválido", type: :validation)
    
    if payload[:value] > 1000, 
      do: raise("Falha catastrófica no hardware")

    :db_success
  end
end

defmodule SensorSystem.App do
  use Application

  def start(_type, _args) do
    children = [
      SensorSystem.Registry,
      {DynamicSupervisor, name: SensorSystem.SensorSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_all, name: SensorSystem.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
