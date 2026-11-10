# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça o processamento de arquivos CSV, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Permitir limitar a quantidade de operações simultâneas.
# 3. Permitir consultar o estado atual das operações em andamento.
# 4. Recuperar processos após falhas inesperadas.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule CSVProcessor do
  @moduledoc """
  Processador de CSV resiliente utilizando GenStage-like pattern com Task.Supervisor
  e um agente para controle de estado.
  """
  use Supervisor

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      {Agent, fn -> %{running: 0, completed: 0, failed: 0} end, name: :proc_state},
      {Task.Supervisor, name: :csv_task_supervisor, max_children: 10}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc "Processa um arquivo CSV com limite de concorrência e retry"
  def process_file(file_path, concurrency_limit \\ 5) do
    file_path
    |> File.stream!()
    |> Stream.chunk_every(concurrency_limit)
    |> Enum.each(fn chunk ->
      chunk
      |> Enum.map(&Task.Supervisor.async_nolink(:csv_task_supervisor, fn -> execute_with_retry(&1) end))
      |> Enum.map(&Task.await/1)
    end)
  end

  defp execute_with_retry(row, retries \\ 3) do
    update_state(:running, 1)

    try do
      # Simulação de processamento de linha
      :timer.sleep(100)
      if :rand.uniform() > 0.8, do: raise("Transient error")
      
      update_state(:completed, 1)
    rescue
      e ->
        if retries > 0 do
          execute_with_retry(row, retries - 1)
        else
          update_state(:failed, 1)
          {:error, e}
        end
    after
      update_state(:running, -1)
    end
  end

  defp update_state(key, delta) do
    Agent.update(:proc_state, fn state ->
      Map.update!(state, key, &(&1 + delta))
    end)
  end

  @doc "Consulta o estado atual das operações"
  def get_status do
    Agent.get(:proc_state, & &1)
  end
end
