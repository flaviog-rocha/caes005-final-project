# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de entregas e rastreamento, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Manter um histórico das operações realizadas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Permitir que diferentes entidades sejam processadas independentemente.
# 5. Permitir limitar a quantidade de operações simultâneas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Logger
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule DeliverySystem.DSL do
  @moduledoc """
  DSL para definir operações de entrega com rastreamento automático.
  """
  defmacro __using__(_opts) do
    quote do
      import unquote(__MODULE__)
      require Logger
    end
  end

  defmacro def_delivery_op(name, do: block) do
    quote do
      def unquote(name)(entity_id, data) do
        Logger.info("Iniciando operação #{unquote(name)} para #{entity_id}")
        
        # Registra o histórico
        DeliverySystem.Tracker.record(entity_id, unquote(name), :started)
        
        try do
          result = unquote(block)
          DeliverySystem.Tracker.record(entity_id, unquote(name), :completed)
          {:ok, result}
        rescue
          e ->
            DeliverySystem.Tracker.record(entity_id, unquote(name), {:failed, e})
            {:error, e}
        end
      end
    end
  end
end

defmodule DeliverySystem.Tracker do
  use GenServer

  # API
  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def record(entity_id, operation, status) do
    GenServer.cast(__MODULE__, {:record, entity_id, operation, status})
  end

  def get_state(entity_id) do
    GenServer.call(__MODULE__, {:get, entity_id})
  end

  # Callbacks
  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_cast({:record, entity_id, op, status}, state) do
    entry = %{operation: op, status: status, timestamp: System.system_time(:second)}
    new_state = Map.update(state, entity_id, [entry], &[entry | &1])
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:get, entity_id}, _from, state) do
    {:reply, Map.get(state, entity_id, []), state}
  end
end

defmodule DeliverySystem.WorkerPool do
  @doc """
  Limita a quantidade de operações simultâneas usando Task.async_stream.
  """
  def process_batch(entities, func, max_concurrency \\ 2) do
    entities
    |> Task.async_stream(fn entity -> func.(entity) end, max_concurrency: max_concurrency)
    |> Enum.to_list()
  end
end

defmodule DeliveryApp do
  use DeliverySystem.DSL

  def_delivery_op :ship_item do
    # Simula trabalho
    Process.sleep(100)
    "Item enviado com sucesso"
  end

  def run_demo do
    # Inicializa o rastreador
    DeliverySystem.Tracker.start_link([])

    # Entidades processadas independentemente
    entities = [:package_1, :package_2, :package_3, :package_4]

    # Processamento com limite de concorrência (ex: 2 por vez)
    DeliverySystem.WorkerPool.process_batch(entities, fn id ->
      ship_item(id, %{})
    end, 2)

    # Consulta de estado
    IO.inspect(DeliverySystem.Tracker.get_state(:package_1), label: "Histórico package_1")
  end
end

# Para rodar: DeliveryApp.run_demo()
