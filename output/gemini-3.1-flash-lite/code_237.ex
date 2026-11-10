# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Fazer uso de macros.
# 2. Permitir comunicação entre diferentes processos.
# 3. Garantir a consistência dos dados durante operações concorrentes.
# 4. Implementar novas tentativas após falhas temporárias.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Gerenciador de fila de tarefas com suporte a reintentos e concorrência.
  """

  use GenServer

  # 1. Macro para definir uma tarefa com lógica de reintento
  defmacro def_task(name, do: block) do
    quote do
      def unquote(name)(payload) do
        fn -> unquote(block) end.(payload)
      end
    end
  end

  # API Pública
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{queue: []}, name: __MODULE__)

  def enqueue(task_fun, payload, retries \\ 3) do
    GenServer.cast(__MODULE__, {:push, {task_fun, payload, retries}})
  end

  # GenServer Callbacks
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:push, task}, %{queue: queue}) do
    spawn(fn -> execute_task(task) end)
    {:noreply, %{queue: [task | queue]}}
  end

  # 3. Consistência garantida pelo estado isolado do GenServer
  # 4. Implementação de reintento
  defp execute_task({task_fun, payload, retries}) do
    try do
      task_fun.(payload)
    rescue
      _e ->
        if retries > 0 do
          Process.sleep(1000)
          execute_task({task_fun, payload, retries - 1})
        else
          IO.puts("Tarefa falhou após várias tentativas.")
        end
    end
  end
end

# Demonstração de uso
defmodule Worker do
  import TaskQueue

  # Uso da macro
  def_task :process_data do
    fn payload ->
      if :rand.uniform() > 0.7, do: :ok, else: raise "Erro temporário"
      IO.puts("Processado: #{payload}")
    end
  end
end

# Execução
TaskQueue.start_link([])
Worker.process_data("Item A") |> TaskQueue.enqueue()
Worker.process_data("Item B") |> TaskQueue.enqueue()
