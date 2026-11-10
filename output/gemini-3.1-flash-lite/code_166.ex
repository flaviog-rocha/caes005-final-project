# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Permitir consultar o estado atual das operações em andamento.
# 2. Fazer uso de macros.
# 3. Executar determinadas operações periodicamente.
# 4. Permitir comunicação entre diferentes processos.
# 5. Quando fizer sentido para o domínio, considerar a comunicação com um banco de dados fictício e, opcionalmente, o uso de Ecto.Migration, sem tornar isso obrigatório.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue.Store do
  @moduledoc "Simulação de camada de persistência"
  def save(task), do: IO.inspect(task, label: "DB Persist")
end

defmodule TaskQueue.Worker do
  use GenServer

  # Macros para definir operações periódicas de forma declarativa
  defmacro def_periodic_task(name, interval, do: block) do
    quote do
      def handle_info({:periodic, unquote(name)}, state) do
        unquote(block)
        Process.send_after(self(), {:periodic, unquote(name)}, unquote(interval))
        {:noreply, state}
      end
    end
  end

  def start_link(_), do: GenServer.start_link(__MODULE__, %{running: []}, name: __MODULE__)

  @impl true
  def init(state) do
    # Agendamento inicial das operações periódicas
    Process.send_after(self(), {:periodic, :cleanup}, 5000)
    {:ok, state}
  end

  # Requisito 1 & 4: Consultar estado e comunicação entre processos
  @impl true
  def handle_call(:get_status, _from, state), do: {:reply, state, state}

  @impl true
  def handle_cast({:add_task, task}, state) do
    TaskQueue.Store.save(task)
    {:noreply, %{state | running: [task | state.running]}}
  end

  # Requisito 3: Operação periódica via Macro
  def_periodic_task :cleanup, 5000 do
    IO.puts("Executando limpeza periódica da fila...")
  end
end

defmodule TaskQueue do
  @doc "Interface pública para o sistema"
  def add(task), do: GenServer.cast(TaskQueue.Worker, {:add_task, task})
  def status, do: GenServer.call(TaskQueue.Worker, :get_status)

  def run do
    {:ok, _} = TaskQueue.Worker.start_link(nil)
    
    add("Processamento de Imagem")
    add("Envio de E-mail")
    
    IO.inspect(status(), label: "Estado Atual")
  end
end

# Exemplo de uso
TaskQueue.run()
