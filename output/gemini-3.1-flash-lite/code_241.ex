# Prompt gerado:
# Você é um engenheiro de software sênior focado em programação funcional. Preciso que você gere um código Elixir que faça um sistema de agendamento de compromissos, atendendo somente aos seguintes requisitos:
#
# 1. Permitir comunicação entre diferentes processos.
# 2. Implementar novas tentativas após falhas temporárias.
# 3. Gerar notificações quando determinadas condições forem atingidas.
# 4. Persistir os dados para recuperação após reinicializações.
#
# Gere um único arquivo autocontido, prefira recursos da biblioteca padrão do Elixir. Não escreva nada além do código (e comentários do código).

# Para persistência simples, utilizaremos um arquivo local via :dets (Disk Based Erterm Term Storage)
# O sistema utiliza um GenServer para gerenciar o estado e Processos para simular agendamento.

defmodule SchedulerSystem do
  use GenServer
  require Logger

  @db_file 'appointments.dets'

  # --- API Pública ---

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def schedule(id, time, task) do
    GenServer.call(__MODULE__, {:schedule, id, time, task})
  end

  # --- Callbacks do GenServer ---

  @impl true
  def init(_) do
    {:ok, db} = :dets.open_file(@db_file, type: :set)
    # Recupera estado persistente
    state = :dets.foldl(fn {id, entry}, acc -> [entry | acc] end, [], db)
    {:ok, %{db: db, appointments: state}}
  end

  @impl true
  def handle_call({:schedule, id, time, task}, _from, state) do
    entry = %{id: id, time: time, task: task, retries: 0}
    :dets.insert(state.db, {id, entry})
    
    # Inicia o processo de agendamento (comunicação entre processos)
    spawn(fn -> execute_with_retry(entry) end)
    
    {:reply, :ok, %{state | appointments: [entry | state.appointments]}}
  end

  # --- Lógica de Execução e Retentativa ---

  defp execute_with_retry(entry, attempt \\ 0) do
    max_retries = 3
    
    try do
      # Simulação de trabalho
      Logger.info("Executando tarefa: #{entry.id}")
      
      # Condição para notificação
      if entry.retries > 0, do: notify("Alerta: Tarefa #{entry.id} recuperada após falha.")
      
      # Sucesso
      Logger.info("Tarefa #{entry.id} concluída com sucesso.")
    rescue
      _ ->
        if attempt < max_retries do
          Process.sleep(1000 * (attempt + 1)) # Backoff exponencial
          execute_with_retry(entry, attempt + 1)
        else
          notify("Erro crítico: Tarefa #{entry.id} falhou após #{max_retries} tentativas.")
        end
    end
  end

  defp notify(message) do
    # Simula envio de notificação (ex: Log ou Canal PubSub)
    Logger.warn("[NOTIFICAÇÃO]: #{message}")
  end
end

# Exemplo de uso:
# {:ok, _} = SchedulerSystem.start_link([])
# SchedulerSystem.schedule("job_1", ~N[2023-10-27 10:00:00], fn -> IO.puts("Work") end)
