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
  Sistema de agendamento que utiliza Task para tarefas periódicas,
  implementa retentativas com estratégia de backoff e persiste o estado em arquivo.
  """

  @storage_file "appointments.dets"
  @interval 5_000 # 5 segundos

  def start_link do
    spawn(fn ->
      :dets.open_file(:appointments, [file: @storage_file, type: :set])
      loop()
    end)
  end

  defp loop do
    Task.start(fn -> process_pending_appointments() end)
    Process.sleep(@interval)
    loop()
  end

  defp process_pending_appointments do
    :dets.traverse(:appointments, fn {id, data} ->
      execute_with_retry(id, data, 3)
      :continue
    end)
  end

  defp execute_with_retry(id, data, attempts) do
    try do
      # Simulação de operação instável
      if :rand.uniform(10) > 7, do: raise "Erro de rede temporário"
      IO.puts("Compromisso #{id} processado com sucesso.")
      :dets.delete(:appointments, id)
    rescue
      e ->
        if attempts > 1 do
          IO.warn("Falha no compromisso #{id}. Tentando novamente... Restam: #{attempts - 1}")
          Process.sleep(1000)
          execute_with_retry(id, data, attempts - 1)
        else
          IO.puts("Compromisso #{id} falhou após várias tentativas. Mantendo para persistência.")
        end
    end
  end

  def schedule(id, data) do
    :dets.insert(:appointments, {id, data})
  end
end

# Exemplo de uso:
# Scheduler.start_link()
# Scheduler.schedule(1, %{task: "Reunião", time: ~N[2023-10-27 10:00:00]})
