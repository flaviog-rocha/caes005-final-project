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

defmodule TaskQueue do
  @moduledoc """
  Um gerenciador de fila de tarefas com suporte a execução periódica e estado compartilhado.
  """

  use GenServer

  # --- Macro para simplificar a definição de tarefas agendadas ---
  defmacro def_task(name, interval, do: block) do
    quote do
      def handle_info({:run_task, unquote(name)}, state) do
        IO.puts("[TaskQueue] Executando tarefa: #{unquote(name)}")
        unquote(block)
        Process.send_after(self(), {:run_task, unquote(name)}, unquote(interval))
        {:noreply, state}
      end
    end
  end

  # --- API Pública ---

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{active_tasks: [], db_mock: []}, name: __MODULE__)
  end

  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  def enqueue_task(task_name) do
    GenServer.cast(__MODULE__, {:enqueue, task_name})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(state) do
    # Inicia a tarefa periódica
    Process.send_after(self(), {:run_task, :cleanup}, 5000)
    {:ok, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast({:enqueue, task_name}, state) do
    new_state = Map.update!(state, :active_tasks, fn tasks -> [task_name | tasks] end)
    # Simulação de escrita em banco de dados
    IO.puts("[DB] Persistindo tarefa: #{task_name}")
    {:noreply, new_state}
  end

  # Uso da macro definida acima
  def_task(:cleanup, 10000) do
    # Lógica de limpeza fictícia
    IO.puts("[System] Limpeza periódica concluída.")
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end

# --- Exemplo de uso ---
# TaskQueue.start_link([])
# TaskQueue.enqueue_task("Processamento de Imagem")
# IO.inspect(TaskQueue.get_status())
