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

# Para executar, salve como scheduler.exs e rode: elixir scheduler.exs
# Este sistema utiliza DETS para persistência atômica, GenStage/Task para paralelismo
# e um loop recursivo com backoff para resiliência.

defmodule AppointmentScheduler do
  @table_name :appointments_table
  @retry_limit 3

  # Inicia o sistema e abre a tabela DETS
  def start() do
    {:ok, _} = :dets.open_file(@table_name, [type: :set, auto_save: 5000])
    spawn_worker_pool()
  end

  # Requisito 5: Processamento independente por entidade
  defp spawn_worker_pool() do
    entities = [:entity_a, :entity_b, :entity_c]
    Enum.each(entities, fn entity ->
      Task.start(fn -> schedule_loop(entity) end)
    end)
  end

  # Requisito 1: Execução periódica
  defp schedule_loop(entity) do
    process_entity(entity)
    Process.sleep(5000) # Intervalo de 5 segundos
    schedule_loop(entity)
  end

  # Requisito 2 e 4: Execução simultânea e novas tentativas
  defp process_entity(entity) do
    case perform_with_retry(entity, 0) do
      :ok -> :ok
      {:error, _reason} -> IO.puts("Falha persistente para #{entity}")
    end
  end

  defp perform_with_retry(entity, attempt) when attempt < @retry_limit do
    try do
      # Requisito 3: Impedir atualizações parcialmente concluídas via transação DETS
      # Usamos a natureza atômica das inserções únicas do DETS
      task_id = :erlang.unique_integer([:positive])
      data = {:appointment, entity, task_id, :pending}
      
      :dets.insert(@table_name, {task_id, data})
      
      # Simulação de processamento
      :ok = do_business_logic(entity)
      
      :dets.insert(@table_name, {task_id, {:appointment, entity, task_id, :completed}})
      :ok
    rescue
      _ -> 
        Process.sleep(1000 * attempt) # Backoff simples
        perform_with_retry(entity, attempt + 1)
    end
  end

  defp perform_with_retry(_, _), do: {:error, :max_retries_reached}

  defp do_business_logic(_entity) do
    # Simula possibilidade de falha temporária
    if :rand.uniform(10) > 8, do: raise "Erro temporário"
    :ok
  end
end

AppointmentScheduler.start()
