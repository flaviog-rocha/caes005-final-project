# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o gerenciamento de uma fila de tarefas, atendendo somente aos seguintes requisitos:
#
# 1. Persistir os dados para recuperação após reinicializações.
# 2. Permitir comunicação entre diferentes processos.
# 3. Recuperar processos após falhas inesperadas.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Agent
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule TaskQueue do
  @moduledoc """
  Gerenciador de fila de tarefas utilizando Agent com persistência em disco.
  """

  use Agent

  @storage_file "task_queue.dat"

  @doc "Inicia o Agent e carrega o estado persistido se existir."
  def start_link(_opts) do
    initial_state = load_from_disk()
    Agent.start_link(fn -> initial_state end, name: __MODULE__)
  end

  @doc "Adiciona uma tarefa à fila e persiste no disco."
  def enqueue(task) do
    Agent.update(__MODULE__, fn state ->
      new_state = state ++ [task]
      save_to_disk(new_state)
      new_state
    end)
  end

  @doc "Remove a primeira tarefa da fila e persiste no disco."
  def dequeue do
    Agent.get_and_update(__MODULE__, fn
      [] -> {nil, []}
      [head | tail] ->
        save_to_disk(tail)
        {head, tail}
    end)
  end

  @doc "Retorna o estado atual da fila."
  def list do
    Agent.get(__MODULE__, & &1)
  end

  # --- Helpers de Persistência ---

  defp save_to_disk(data) do
    :erlang.term_to_binary(data) |> then(&File.write!(@storage_file, &1))
  end

  defp load_from_disk do
    if File.exists?(@storage_file) do
      File.read!(@storage_file) |> :erlang.binary_to_term()
    else
      []
    end
  end
end

# Para garantir a recuperação após falhas (Supervisor), 
# incluímos uma estratégia simples de reinicialização.

defmodule TaskQueue.Supervisor do
  use Supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      TaskQueue
    ]

    # O strategy :one_for_one garante que se o Agent (TaskQueue) falhar, 
    # o supervisor o reiniciará automaticamente.
    Supervisor.init(children, strategy: :one_for_one)
  end
end
