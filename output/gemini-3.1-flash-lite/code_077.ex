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
  O supervisor garante a recuperação após falhas.
  """

  use Agent
  use Supervisor

  @storage_file "task_queue.bin"

  # --- Interface Pública ---

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_task(task) do
    Agent.update(__MODULE__, fn queue ->
      new_queue = queue ++ [task]
      persist(new_queue)
      new_queue
    end)
  end

  def pop_task do
    Agent.get_and_update(__MODULE__, fn
      [] -> {nil, []}
      [task | rest] ->
        persist(rest)
        {task, rest}
    end)
  end

  # --- Callback do Supervisor ---

  def init(_) do
    initial_state = load_persisted()
    
    children = [
      {Agent, fn -> initial_state end, name: __MODULE__}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  # --- Funções Internas de Persistência ---

  defp persist(queue) do
    :erlang.term_to_binary(queue) |> then(&File.write!(@storage_file, &1))
  end

  defp load_persisted do
    if File.exists?(@storage_file) do
      @storage_file |> File.read!() |> :erlang.binary_to_term()
    else
      []
    end
  rescue
    _ -> []
  end
end
