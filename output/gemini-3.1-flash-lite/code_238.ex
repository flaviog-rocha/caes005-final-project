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
  Gerenciador de fila de tarefas com suporte a reintentos, concorrência e macros.
  """

  use GenServer

  # Macro para facilitar a criação de tarefas que utilizam a fila
  defmacro def_task(name, do: block) do
    quote do
      def unquote(name)(args) do
        TaskQueue.enqueue(fn -> unquote(block) end, args)
      end
    end
  end

  # API Pública
  def start_link(_), do: GenServer.start_link(__MODULE__, %{queue: :queue.new()}, name: __MODULE__)

  def enqueue(task_fun, args, retries \\ 3) do
    GenServer.cast(__MODULE__, {:enqueue, {task_fun, args, retries}})
  end

  # Callbacks GenServer
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_cast({:enqueue, task_data}, %{queue: q} = state) do
    new_q = :queue.in(task_data, q)
    send(self(), :process_next)
    {:noreply, %{state | queue: new_q}}
  end

  @impl true
  def handle_info(:process_next, %{queue: q} = state) do
    case :queue.out(q) do
      {{:value, {task_fun, args, retries}}, rest} ->
        # Execução assíncrona em processo isolado para manter consistência e evitar bloqueio
        Task.start(fn ->
          try do
            task_fun.(args)
          rescue
            _ ->
              if retries > 0 do
                enqueue(task_fun, args, retries - 1)
              else
                IO.puts("Tarefa falhou após múltiplas tentativas: #{inspect(args)}")
              end
          end
        end)
        {:noreply, %{state | queue: rest}}

      {:empty, _} ->
        {:noreply, state}
    end
  end
end

# Demonstração de uso
defmodule Worker do
  require TaskQueue
  
  # Uso da Macro
  TaskQueue.def_task(:enviar_email) do
    IO.puts("Processando tarefa: #{inspect(args)}")
    # Simula falha aleatória
    if :rand.uniform(2) == 1, do: raise "Erro temporário"
    IO.puts("Sucesso!")
  end
end

# Inicialização
{:ok, _pid} = TaskQueue.start_link([])

# Disparo concorrente
Worker.enviar_email("usuario@exemplo.com")
Worker.enviar_email("admin@exemplo.com")
