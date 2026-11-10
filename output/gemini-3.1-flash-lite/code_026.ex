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

defmodule DeliverySystem do
  require Logger

  defmodule Tracker do
    use GenServer

    # Macro para definir operações rastreáveis com log automático
    defmacro def_operation(name, args, do: block) do
      quote do
        def unquote(name)(unquote_splicing(args)) do
          Logger.info("Iniciando operação: #{unquote(name)}")
          result = unquote(block)
          Logger.info("Operação finalizada: #{unquote(name)}")
          result
        end
      end
    end

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_) do
      {:ok, %{history: [], active: %{}}}
    end

    def process_delivery(id, task_fn) do
      GenServer.call(__MODULE__, {:run, id, task_fn})
    end

    def get_state do
      GenServer.call(__MODULE__, :get_state)
    end

    @impl true
    def handle_call({:run, id, task_fn}, _from, state) do
      # Limite de concorrência: 5 operações simultâneas
      if map_size(state.active) >= 5 do
        {:reply, {:error, :busy}, state}
      else
        new_active = Map.put(state.active, id, "Em andamento")
        
        # Executa a tarefa de forma assíncrona
        Task.start(fn ->
          task_fn.()
          GenServer.cast(__MODULE__, {:finished, id})
        end)

        new_history = [%{id: id, status: "Iniciado", ts: System.system_time(:second)} | state.history]
        {:reply, :ok, %{state | active: new_active, history: new_history}}
      end
    end

    @impl true
    def handle_call(:get_state, _from, state) do
      {:reply, state, state}
    end

    @impl true
    def handle_cast({:finished, id}, state) do
      new_active = Map.delete(state.active, id)
      new_history = [%{id: id, status: "Concluído", ts: System.system_time(:second)} | state.history]
      {:noreply, %{state | active: new_active, history: new_history}}
    end
  end

  # Exemplo de uso da macro
  defmodule DeliveryOperations do
    import DeliverySystem.Tracker
    
    def_operation(deliver_package, [id], do: :timer.sleep(1000))
  end
end

# Demonstração simples
{:ok, _pid} = DeliverySystem.Tracker.start_link([])

DeliverySystem.Tracker.process_delivery("PKG-001", fn -> 
  DeliverySystem.DeliveryOperations.deliver_package("PKG-001") 
end)

IO.inspect(DeliverySystem.Tracker.get_state())
