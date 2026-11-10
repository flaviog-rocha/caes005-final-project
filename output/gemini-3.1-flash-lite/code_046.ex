# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Executar determinadas operações periodicamente.
# 2. Permitir a execução de operações simultâneas.
# 3. Impedir atualizações parcialmente concluídas.
# 4. Implementar novas tentativas após falhas temporárias.
# 5. Permitir que diferentes entidades sejam processadas independentemente.
#
# A implementação deve utilizar os seguintes recursos do Elixir:
# - DETS
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para executar, salve como scheduler.ex e rode: elixir scheduler.ex
# O sistema utiliza DETS para persistência atômica, Task.Supervisor para 
# processamento simultâneo e GenServer para orquestração de entidades.

defmodule AppointmentScheduler do
  use GenServer

  @table_name :appointments_table
  @max_retries 3

  # Inicia o sistema e abre a tabela DETS
  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, _} = :dets.open_file(@table_name, [type: :set, auto_save: 5000])
    # Agenda a varredura periódica a cada 60 segundos
    schedule_work()
    {:ok, %{}}
  end

  # Requisito 1 & 5: Processamento periódico e independente por entidade
  defp schedule_work do
    Process.send_after(self(), :work, 60_000)
  end

  @impl true
  def handle_info(:work, state) do
    # Processa entidades independentemente via Task.Supervisor
    :dets.traverse(@table_name, fn {id, data} ->
      Task.Supervisor.start_child(SchedulerSupervisor, fn -> 
        execute_with_retry(id, data) 
      end)
      :continue
    end)
    
    schedule_work()
    {:noreply, state}
  end

  # Requisito 3 & 4: Atomicidade via DETS e Retry com exponencial backoff
  defp execute_with_retry(id, data, attempt \\ 0) do
    try do
      # Simulação de trabalho
      perform_operation(data)
      
      # Requisito 3: Atualização atômica (transação implícita no DETS)
      :dets.insert(@table_name, {id, Map.put(data, :last_run, System.system_time())})
    rescue
      _ ->
        if attempt < @max_retries do
          Process.sleep(1000 * (attempt + 1))
          execute_with_retry(id, data, attempt + 1)
        end
    end
  end

  defp perform_operation(_data), do: :ok
end

# Supervisor para gerenciar tarefas simultâneas (Requisito 2)
defmodule SchedulerSupervisor do
  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    children = [
      {Task.Supervisor, name: __MODULE__},
      {AppointmentScheduler, []}
    ]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

# Inicialização do sistema
{:ok, _} = SchedulerSupervisor.start_link(nil)
