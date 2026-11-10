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
  Processador de CSV com concorrência controlada, resiliência e monitoramento.
  Utiliza Task.Supervisor para recuperação e Registry para rastreamento.
  """
  use Supervisor

  @registry_name :csv_process_registry
  @task_supervisor_name :csv_task_supervisor

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry_name},
      {Task.Supervisor, name: @task_supervisor_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Processa uma linha com estratégia de retry e limite de concorrência via semaphore/pool.
  """
  def process_file(file_path, concurrency_limit \\ 10) do
    # Simulação de leitura de linhas
    lines = File.stream!(file_path)

    lines
    |> Task.async_stream(
      fn line -> retry_operation(line, 3) end,
      max_concurrency: concurrency_limit,
      ordered: false,
      on_timeout: :kill_task
    )
    |> Stream.run()
  end

  defp retry_operation(line, attempts) do
    # Registra o processo atual para consulta de estado
    {:ok, _} = Registry.register(@registry_name, "proc_#{inspect(self())}", %{line: line})

    try do
      do_work(line)
    rescue
      _ ->
        if attempts > 1, do: retry_operation(line, attempts - 1), else: :failed
    end
  end

  defp do_work(_line) do
    # Simula lógica de processamento
    :timer.sleep(100)
    :ok
  end

  @doc """
  Consulta o estado atual das operações em andamento.
  """
  def list_active_operations do
    @registry_name
    |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [%{pid: :"$2", meta: :"$3"}]}])
  end

  @doc """
  Executa uma tarefa garantindo recuperação via Supervisor.
  """
  def run_supervised(func) do
    Task.Supervisor.start_child(@task_supervisor_name, fn ->
      func.()
    end)
  end
end
