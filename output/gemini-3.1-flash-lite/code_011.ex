# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Implementar novas tentativas após falhas temporárias.
# 2. Executar determinadas operações periodicamente.
# 3. Persistir os dados para recuperação após reinicializações.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - Task
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

defmodule Scheduler do
  @moduledoc """
  Um sistema de agendamento que utiliza Task para tarefas periódicas,
  implementa retentativas exponenciais e persiste em um arquivo ETS.
  """
  use GenServer

  @file_path "appointments.ets"

  def start_link(_), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(_) do
    :ets.file2tab(@file_path) |> case do
      {:ok, _} -> {:ok, %{}}
      _ -> :ets.new(:appointments, [:named_table, :set, :public])
           {:ok, %{}}
    end
  end

  def schedule_task(id, action) do
    # Task encapsula a execução assíncrona
    Task.start(fn -> execute_with_retry(id, action, 0) end)
  end

  defp execute_with_retry(id, action, attempt) do
    try do
      action.()
      persist_status(id, :completed)
    rescue
      _ ->
        if attempt < 3 do
          Process.sleep(:math.pow(2, attempt) |> round() * 1000)
          execute_with_retry(id, action, attempt + 1)
        else
          persist_status(id, :failed)
        end
    end
  end

  defp persist_status(id, status) do
    :ets.insert(:appointments, {id, status})
    :ets.tab2file(:appointments, @file_path)
  end

  # Simulação de loop periódico usando recursão com Task
  def start_periodic_task(interval, task_fun) do
    Task.start(fn -> periodic_loop(interval, task_fun) end)
  end

  defp periodic_loop(interval, task_fun) do
    task_fun.()
    Process.sleep(interval)
    periodic_loop(interval, task_fun)
  end
end

# Exemplo de uso:
# Scheduler.start_link([])
# Scheduler.start_periodic_task(60_000, fn -> IO.puts("Executando verificação periódica...") end)
# Scheduler.schedule_task("job_1", fn -> raise "falha temporária" end)
